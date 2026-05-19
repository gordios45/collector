// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Wikipedia EventStreams collector — SSE feed of every edit across all
// Wikimedia wikis. We filter for en.wikipedia main-namespace non-bot
// edits, count per-article edits in a rolling 10-minute window, and
// when an article crosses the edit-rate threshold we geocode it via
// the MediaWiki coordinates API and emit one "edit surge" event.
//
// The signal this catches: an article becomes the focus of rapid
// crowdsourced fact-updating, which is a near-real-time tip that
// "something is happening here". Named-entity / location articles
// with coords are automatically plottable.
package wikipedia

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/sources"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	streamURL = "https://stream.wikimedia.org/v2/stream/recentchange"
	threshold = 3 // edits within window → surge
	window    = 10 * time.Minute
	cacheTTL  = 1 * time.Hour // geocode cache TTL
)

type Collector struct {
	pool *pgxpool.Pool
	sink *sources.EventSink

	mu         sync.Mutex
	edits      map[string][]time.Time    // title → recent edit timestamps
	geocodeHit map[string]*geoCacheEntry // title → coords
	emitted    map[string]time.Time      // title → last emit time (dedupe)
}

type geoCacheEntry struct {
	lat, lon float64
	found    bool
	at       time.Time
}

func New(pool *pgxpool.Pool) *Collector {
	return &Collector{
		pool:       pool,
		sink:       sources.NewEventSink(pool, "wikipedia_surge", 50, 2*time.Second),
		edits:      make(map[string][]time.Time, 2048),
		geocodeHit: make(map[string]*geoCacheEntry, 512),
		emitted:    make(map[string]time.Time, 512),
	}
}

func (c *Collector) ID() string { return "wikipedia_surge" }

func (c *Collector) Run(ctx context.Context) error {
	go c.sink.Run(ctx)
	go c.gcLoop(ctx) // periodic cleanup of the counter map

	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.once(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[wikipedia_surge] sse: %v; reconnect in %s", err, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	return nil
}

func (c *Collector) once(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")

	client := &http.Client{Timeout: 0} // long-lived SSE
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wikimedia %d", resp.StatusCode)
	}
	log.Println("[wikipedia_surge] SSE connected")

	var frames, surges int
	logT := time.NewTicker(60 * time.Second)
	defer logT.Stop()
	go func() {
		for range logT.C {
			log.Printf("[wikipedia_surge] %d edits / %d surges in last 60 s", frames, surges)
			frames, surges = 0, 0
		}
	}()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		if ctx.Err() != nil {
			return nil
		}
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := line[len("data: "):]
		frames++
		if c.handleEdit(ctx, payload) {
			surges++
		}
	}
	return sc.Err()
}

func (c *Collector) handleEdit(ctx context.Context, payload string) bool {
	var e struct {
		Type       string `json:"type"`
		Namespace  int    `json:"namespace"`
		Bot        bool   `json:"bot"`
		ServerName string `json:"server_name"`
		Title      string `json:"title"`
		Wiki       string `json:"wiki"`
	}
	if err := json.Unmarshal([]byte(payload), &e); err != nil {
		return false
	}
	// English Wikipedia, main namespace, human edits only.
	if e.Type != "edit" || e.Bot || e.Namespace != 0 || e.ServerName != "en.wikipedia.org" {
		return false
	}
	now := time.Now().UTC()
	c.mu.Lock()
	list := append(c.edits[e.Title], now)
	// Trim to window
	cutoff := now.Add(-window)
	pruned := list[:0]
	for _, t := range list {
		if t.After(cutoff) {
			pruned = append(pruned, t)
		}
	}
	c.edits[e.Title] = pruned
	count := len(pruned)
	// Debounce emissions per title — one surge event every `window`.
	lastEmit := c.emitted[e.Title]
	c.mu.Unlock()
	if count < threshold {
		return false
	}
	if !lastEmit.IsZero() && now.Sub(lastEmit) < window {
		return false
	}
	// Geocode (cached).
	geo := c.lookupGeo(ctx, e.Title)
	if geo == nil || !geo.found {
		return false
	}
	c.mu.Lock()
	c.emitted[e.Title] = now
	c.mu.Unlock()

	c.sink.Push(ctx, events.Event{
		Ts:     now,
		Source: "wikipedia_surge",
		ExtID:  e.Title,
		Lat:    geo.lat,
		Lon:    geo.lon,
		Props: map[string]any{
			"title":       e.Title,
			"edits_10min": count,
			"url":         "https://en.wikipedia.org/wiki/" + strings.ReplaceAll(e.Title, " ", "_"),
			"wiki":        e.Wiki,
		},
	})
	return true
}

// lookupGeo fetches coordinates via the MediaWiki API; caches hits AND
// misses (misses expire faster so a later "article got geotagged" case
// still wins eventually).
func (c *Collector) lookupGeo(ctx context.Context, title string) *geoCacheEntry {
	c.mu.Lock()
	hit := c.geocodeHit[title]
	c.mu.Unlock()
	if hit != nil && time.Since(hit.at) < cacheTTL {
		return hit
	}
	reqURL := fmt.Sprintf(
		"https://en.wikipedia.org/w/api.php?action=query&prop=coordinates&format=json&titles=%s",
		url.QueryEscape(title))
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	req.Header.Set("User-Agent", "gordios/0.1")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		entry := &geoCacheEntry{at: time.Now()}
		c.mu.Lock()
		c.geocodeHit[title] = entry
		c.mu.Unlock()
		return entry
	}
	defer resp.Body.Close()
	var mw struct {
		Query struct {
			Pages map[string]struct {
				Coordinates []struct {
					Lat     float64 `json:"lat"`
					Lon     float64 `json:"lon"`
					Primary string  `json:"primary"`
				} `json:"coordinates"`
			} `json:"pages"`
		} `json:"query"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	_ = json.Unmarshal(body, &mw)
	entry := &geoCacheEntry{at: time.Now()}
	for _, p := range mw.Query.Pages {
		for _, c := range p.Coordinates {
			if c.Lat != 0 || c.Lon != 0 {
				entry.lat = c.Lat
				entry.lon = c.Lon
				entry.found = true
				break
			}
		}
		if entry.found {
			break
		}
	}
	c.mu.Lock()
	c.geocodeHit[title] = entry
	c.mu.Unlock()
	return entry
}

// gcLoop trims the in-memory counter maps every few minutes so a
// long-running process doesn't leak entries for never-touched-again titles.
func (c *Collector) gcLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		cutoff := now.Add(-window)
		c.mu.Lock()
		for k, ts := range c.edits {
			out := ts[:0]
			for _, t := range ts {
				if t.After(cutoff) {
					out = append(out, t)
				}
			}
			if len(out) == 0 {
				delete(c.edits, k)
			} else {
				c.edits[k] = out
			}
		}
		for k, t := range c.emitted {
			if now.Sub(t) > 6*time.Hour {
				delete(c.emitted, k)
			}
		}
		for k, e := range c.geocodeHit {
			if now.Sub(e.at) > 24*time.Hour {
				delete(c.geocodeHit, k)
			}
		}
		c.mu.Unlock()
	}
}
