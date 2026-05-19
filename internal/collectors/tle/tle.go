// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// TLE collector — Celestrak multi-group TLE catalogue.
//
// One row per NORAD id, stored in the `events` hypertable with ts = TLE
// epoch (parsed from line 1). The scheduler's ON CONFLICT upsert means
// the same TLE poll doesn't blow up row count; when Celestrak publishes a
// new TLE for a sat, ts changes and a new row lands (keeping history).
//
// The client (satellite.js) still does SGP4 propagation in-browser for
// display; this collector only serves the orbital *elements*. Downstream
// analytic jobs can propagate on demand with the same TLE lines.
package tle

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
)

// Celestrak groups that cover essentially all objects of interest without
// fanning out across its per-sat endpoints. Same list the JS layers use.
var groups = []string{
	"stations", "starlink", "gps-ops", "glo-ops", "galileo",
	"weather", "noaa", "goes", "science", "geo", "last-30-days",
	"resource", "military",
}

// Per-group cap — Celestrak returns everything in-group, but we don't need
// to store 5k Starlink every tick. Keep the first N per group.
const maxPerGroup = 200

type Collector struct {
	client *http.Client
}

func New() (*Collector, error) {
	return &Collector{client: &http.Client{Timeout: 20 * time.Second}}, nil
}

func (c *Collector) ID() string               { return "tle" }
func (c *Collector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	type result struct {
		group string
		evs   []events.Event
		err   error
	}
	ch := make(chan result, len(groups))
	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		go func(g string) {
			defer wg.Done()
			evs, err := c.fetchGroup(ctx, g)
			ch <- result{group: g, evs: evs, err: err}
		}(g)
	}
	wg.Wait()
	close(ch)

	seen := make(map[string]struct{}, 1024)
	out := make([]events.Event, 0, 1024)
	for r := range ch {
		if r.err != nil {
			continue // single-group failure is non-fatal; others carry the ingest
		}
		for _, e := range r.evs {
			if _, dup := seen[e.ExtID]; dup {
				continue
			}
			seen[e.ExtID] = struct{}{}
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("every Celestrak group returned empty or 403'd")
	}
	return out, nil
}

func (c *Collector) fetchGroup(ctx context.Context, group string) ([]events.Event, error) {
	u := "https://celestrak.org/NORAD/elements/gp.php?GROUP=" + group + "&FORMAT=tle"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")

	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("celestrak %s: %d", group, r.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20)) // 5 MB ceiling per group
	if err != nil {
		return nil, err
	}
	lines := splitNonEmpty(string(body))

	out := make([]events.Event, 0, 256)
	// TLE triples: name / line1 / line2
	for i := 0; i+2 < len(lines); i += 3 {
		if len(out) >= maxPerGroup {
			break
		}
		name := strings.TrimSpace(lines[i])
		l1 := lines[i+1]
		l2 := lines[i+2]
		if len(l1) < 69 || len(l2) < 69 {
			continue
		}
		if l1[0] != '1' || l2[0] != '2' {
			continue
		}
		noradID := strings.TrimSpace(l1[2:7])
		if noradID == "" {
			continue
		}
		ts, ok := parseEpoch(l1)
		if !ok {
			ts = time.Now().UTC()
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "tle",
			ExtID:  noradID,
			// TLE has no geographic point — set 0,0 and let /api/latest
			// return it; clients key off NORAD id, not lat/lon.
			Lat: 0, Lon: 0,
			Props: map[string]any{
				"name":  name,
				"group": group,
				"line1": l1,
				"line2": l2,
			},
		})
	}
	return out, nil
}

func splitNonEmpty(s string) []string {
	raw := strings.Split(s, "\n")
	out := raw[:0]
	for _, ln := range raw {
		ln = strings.TrimRight(ln, "\r")
		if ln != "" {
			out = append(out, ln)
		}
	}
	return out
}

// TLE line 1 epoch lives at columns 18–32 (0-indexed 18..32): "YYDDD.FFFFFFFF".
// Returns UTC time of the stated epoch.
func parseEpoch(l1 string) (time.Time, bool) {
	if len(l1) < 33 {
		return time.Time{}, false
	}
	yy, err := strconv.Atoi(strings.TrimSpace(l1[18:20]))
	if err != nil {
		return time.Time{}, false
	}
	day, err := strconv.ParseFloat(strings.TrimSpace(l1[20:32]), 64)
	if err != nil {
		return time.Time{}, false
	}
	// 2-digit year: 00–56 → 2000s, 57–99 → 1900s (Celestrak's own convention).
	year := 2000 + yy
	if yy > 56 {
		year = 1900 + yy
	}
	t := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).Add(
		time.Duration((day - 1) * float64(24*time.Hour)),
	)
	return t, true
}
