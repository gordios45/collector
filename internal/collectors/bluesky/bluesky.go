// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Bluesky Jetstream collector — firehose WebSocket of atproto events,
// filtered down to feed posts that mention terms in a crisis-zone
// watchlist. No auth, no rate limit, always-on.
//
// Jetstream protocol: each frame is a JSON document with
//   { kind: "commit", did, commit: { collection, rkey, record: { text, createdAt, ... } } }
// We only care about `kind="commit"` + `commit.collection="app.bsky.feed.post"`.
//
// Matched posts land in events with lat=lon=0 (non-geospatial); UI can
// render as a ticker / intel-panel feed. Future enhancement: geocode
// place mentions in the text.
package bluesky

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/sources"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jetstreamURL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

// Default watchlist — override with BSKY_WATCHLIST env (comma-separated).
// Kept short on purpose: Jetstream is the full public firehose, matching
// against too many common words would saturate the collector.
var defaultWatch = []string{
	"airstrike", "ceasefire", "sanction",
	"nuclear", "hezbollah", "hamas",
	"strait of hormuz", "taiwan strait",
	"blackout", "internet outage",
	"gps jamming", "cyberattack", "ransomware",
	"drone strike", "missile strike",
}

type Collector struct {
	pool     *pgxpool.Pool
	sink     *sources.EventSink
	keywords []string
}

func New(pool *pgxpool.Pool) *Collector {
	list := defaultWatch
	if env := strings.TrimSpace(os.Getenv("BSKY_WATCHLIST")); env != "" {
		list = nil
		for _, k := range strings.Split(env, ",") {
			if k = strings.TrimSpace(strings.ToLower(k)); k != "" {
				list = append(list, k)
			}
		}
	}
	return &Collector{
		pool:     pool,
		sink:     sources.NewEventSink(pool, "bluesky", 100, 2*time.Second),
		keywords: list,
	}
}

func (c *Collector) ID() string { return "bluesky" }

func (c *Collector) Run(ctx context.Context) error {
	go c.sink.Run(ctx)

	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.once(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[bluesky] ws: %v; reconnect in %s", err, backoff)
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
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, jetstreamURL, &websocket.DialOptions{
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(1 << 20) // 1 MB — post records can include embeds
	log.Printf("[bluesky] connected — %d keyword(s)", len(c.keywords))

	var frames, matches int
	logT := time.NewTicker(60 * time.Second)
	defer logT.Stop()
	go func() {
		for range logT.C {
			log.Printf("[bluesky] %d frames / %d matches in last 60 s", frames, matches)
			frames, matches = 0, 0
		}
	}()

	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		frames++
		hit, ev, ok := c.handleFrame(data)
		if ok && hit {
			matches++
			c.sink.Push(ctx, ev)
		}
	}
}

// handleFrame inspects one Jetstream frame. Returns (hit, event, ok) —
// ok=false if the frame isn't a post create we care about.
func (c *Collector) handleFrame(data []byte) (bool, events.Event, bool) {
	var f struct {
		Kind   string `json:"kind"`
		DID    string `json:"did"`
		TimeUS int64  `json:"time_us"`
		Commit *struct {
			Collection string `json:"collection"`
			Rkey       string `json:"rkey"`
			Operation  string `json:"operation"`
			Record     struct {
				Text      string `json:"text"`
				Langs     []string `json:"langs"`
				CreatedAt string `json:"createdAt"`
			} `json:"record"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return false, events.Event{}, false
	}
	if f.Kind != "commit" || f.Commit == nil || f.Commit.Operation != "create" {
		return false, events.Event{}, false
	}
	if f.Commit.Collection != "app.bsky.feed.post" {
		return false, events.Event{}, false
	}
	text := strings.ToLower(f.Commit.Record.Text)
	if text == "" {
		return false, events.Event{}, false
	}
	matched := []string{}
	for _, kw := range c.keywords {
		if strings.Contains(text, kw) {
			matched = append(matched, kw)
		}
	}
	if len(matched) == 0 {
		return false, events.Event{}, false
	}
	ts := time.Now().UTC()
	if f.TimeUS > 0 {
		ts = time.UnixMicro(f.TimeUS).UTC()
	}
	extID := f.DID + "/" + f.Commit.Rkey
	ev := events.Event{
		Ts:     ts,
		Source: "bluesky",
		ExtID:  extID,
		Lat:    0, Lon: 0,
		Props: map[string]any{
			"text":      truncate(f.Commit.Record.Text, 512),
			"did":       f.DID,
			"rkey":      f.Commit.Rkey,
			"lang":      f.Commit.Record.Langs,
			"keywords":  matched,
			"url":       "https://bsky.app/profile/" + f.DID + "/post/" + f.Commit.Rkey,
		},
	}
	return true, ev, true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
