// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Global Fishing Watch v3 events collector — derived vessel behaviour:
// loitering, encounters, port visits, gaps (dark periods). Gated on
// GFW_TOKEN (free tier with registration at globalfishingwatch.org).
package gfw

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const apiBase = "https://gateway.api.globalfishingwatch.org/v3/events"

var defaultTypes = []string{"loitering", "encounter", "gap", "port_visit"}

type Collector struct {
	token  string
	types  []string
	client *http.Client
}

func New() (*Collector, error) {
	tok := strings.TrimSpace(os.Getenv("GFW_TOKEN"))
	if tok == "" {
		return nil, fmt.Errorf("GFW_TOKEN not set")
	}
	types := defaultTypes
	if env := strings.TrimSpace(os.Getenv("GFW_EVENT_TYPES")); env != "" {
		types = nil
		for _, t := range strings.Split(env, ",") {
			if t = strings.TrimSpace(t); t != "" {
				types = append(types, t)
			}
		}
	}
	return &Collector{
		token: tok, types: types,
		client: &http.Client{Timeout: 45 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "gfw_events" }
func (c *Collector) PollEvery() time.Duration { return 1 * time.Hour }

type gfwResp struct {
	Entries  []gfwEvent       `json:"entries"`
	Data     []gfwEvent       `json:"data"`
	Results  []gfwEvent       `json:"results"`
	Features []gfwJSONFeature `json:"features"`
}

type gfwEvent struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Start    string          `json:"start"`
	End      string          `json:"end"`
	Geometry gfwJSONGeometry `json:"geometry"`
	Position struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"position"`
	Vessel struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		SSVID string `json:"ssvid"`
		Flag  string `json:"flag"`
	} `json:"vessel"`
	DurationMs json.RawMessage `json:"duration"`
	EventInfo  map[string]any  `json:"event_info"`
}

type gfwJSONFeature struct {
	ID         string          `json:"id"`
	Geometry   gfwJSONGeometry `json:"geometry"`
	Properties gfwEvent        `json:"properties"`
}

type gfwJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type gfwDataset struct {
	EventType string
	Dataset   string
}

func datasetForEventType(raw string) (gfwDataset, bool) {
	key := strings.NewReplacer("-", "_", " ", "_").Replace(strings.ToLower(strings.TrimSpace(raw)))
	switch key {
	case "fishing", "apparent_fishing":
		return gfwDataset{EventType: "fishing", Dataset: "public-global-fishing-events:latest"}, true
	case "encounter", "encounters":
		return gfwDataset{EventType: "encounter", Dataset: "public-global-encounters-events:latest"}, true
	case "loitering", "loiter":
		return gfwDataset{EventType: "loitering", Dataset: "public-global-loitering-events:latest"}, true
	case "port_visit", "port_visits", "portvisit":
		return gfwDataset{EventType: "port_visit", Dataset: "public-global-port-visits-events:latest"}, true
	case "gap", "gaps", "ais_off", "ais_gap":
		return gfwDataset{EventType: "gap", Dataset: "public-global-gaps-events:latest"}, true
	default:
		return gfwDataset{}, false
	}
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	// Query the last 24 h of events per type. GFW returns events paginated;
	// we take the default (first page ~100) per tick which is plenty.
	now := time.Now().UTC()
	start := now.Add(-24 * time.Hour).Format(time.RFC3339)
	end := now.Format(time.RFC3339)

	var all []events.Event
	var lastErr error
	for _, typ := range c.types {
		ds, ok := datasetForEventType(typ)
		if !ok {
			lastErr = fmt.Errorf("unsupported GFW event type %q", typ)
			continue
		}
		v := url.Values{}
		v.Set("datasets[0]", ds.Dataset)
		v.Set("start-date", start[:10])
		v.Set("end-date", end[:10])
		v.Set("limit", "100")
		v.Set("offset", "0")
		u := apiBase + "?" + v.Encode()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "gordios/0.1")

		r, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("%s → %d: %s", typ, r.StatusCode, string(body[:min(len(body), 512)]))
			continue
		}
		evs, err := parseGFWEvents(body, ds, now)
		if err != nil {
			lastErr = err
			continue
		}
		all = append(all, evs...)
	}
	if len(all) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return all, nil
}

func parseGFWEvents(body []byte, ds gfwDataset, now time.Time) ([]events.Event, error) {
	var raw gfwResp
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse gfw %s: %w", ds.Dataset, err)
	}
	rows := raw.Entries
	rows = append(rows, raw.Data...)
	rows = append(rows, raw.Results...)
	for _, f := range raw.Features {
		e := f.Properties
		if e.ID == "" {
			e.ID = f.ID
		}
		if e.Geometry.Type == "" {
			e.Geometry = f.Geometry
		}
		rows = append(rows, e)
	}

	out := make([]events.Event, 0, len(rows))
	for _, e := range rows {
		ev, ok := eventFromGFWRow(e, ds, now)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromGFWRow(e gfwEvent, ds gfwDataset, now time.Time) (events.Event, bool) {
	if strings.TrimSpace(e.ID) == "" {
		return events.Event{}, false
	}
	lat, lon := e.Position.Lat, e.Position.Lon
	if !validLatLon(lat, lon) {
		lat, lon = pointFromGFWGeometry(e.Geometry)
	}
	if !validLatLon(lat, lon) {
		return events.Event{}, false
	}
	start, _ := parseGFWTime(e.Start)
	end, _ := parseGFWTime(e.End)
	ts := start
	if ts.IsZero() {
		ts = end
	}
	if ts.IsZero() {
		ts = now
	}
	durationMs := gfwDurationMs(e.DurationMs)
	if durationMs <= 0 && !start.IsZero() && !end.IsZero() && !end.Before(start) {
		durationMs = int64(end.Sub(start) / time.Millisecond)
	}
	eventType := canonicalGFWEventType(firstNonEmptyString(e.Type, ds.EventType))
	activityScore := gfwActivityScore(eventType, durationMs)
	props := map[string]any{
		"event_type":         eventType,
		"source_event_type":  e.Type,
		"source_provider":    "global_fishing_watch",
		"source_dataset":     ds.Dataset,
		"abi_activity_class": gfwActivityClass(eventType),
		"abi_activity_score": activityScore,
		"vessel_name":        e.Vessel.Name,
		"vessel_ssvid":       e.Vessel.SSVID,
		"vessel_flag":        e.Vessel.Flag,
		"vessel_id":          e.Vessel.ID,
		"start":              e.Start,
		"end":                e.End,
		"duration_ms":        durationMs,
	}
	if !start.IsZero() {
		props["activity_start"] = start.Format(time.RFC3339)
	}
	if !end.IsZero() {
		props["activity_end"] = end.Format(time.RFC3339)
	}
	for k, val := range e.EventInfo {
		props["info_"+k] = val
	}
	return events.Event{
		Ts:     ts.UTC(),
		Source: "gfw_events",
		ExtID:  e.ID,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func canonicalGFWEventType(raw string) string {
	key := strings.NewReplacer("-", "_", " ", "_").Replace(strings.ToLower(strings.TrimSpace(raw)))
	switch key {
	case "portvisit", "port_visits":
		return "port_visit"
	case "ais_off", "ais_gap", "gaps":
		return "gap"
	case "encounters":
		return "encounter"
	default:
		if ds, ok := datasetForEventType(key); ok {
			return ds.EventType
		}
		return key
	}
}

func gfwActivityClass(eventType string) string {
	switch eventType {
	case "gap":
		return "maritime_dark_activity"
	case "loitering":
		return "maritime_loitering"
	case "encounter":
		return "maritime_rendezvous"
	case "port_visit":
		return "maritime_port_call"
	case "fishing":
		return "maritime_fishing"
	default:
		return "maritime_activity"
	}
}

func gfwActivityScore(eventType string, durationMs int64) float64 {
	hours := float64(durationMs) / float64(time.Hour/time.Millisecond)
	switch eventType {
	case "gap":
		return clamp(hours/6.0, 0, 3)
	case "loitering", "encounter":
		return clamp(hours/3.0, 0, 3)
	case "port_visit":
		return clamp(hours/24.0, 0, 1)
	default:
		return 0.5
	}
}

func pointFromGFWGeometry(g gfwJSONGeometry) (float64, float64) {
	if !strings.EqualFold(g.Type, "Point") || len(g.Coordinates) == 0 {
		return 0, 0
	}
	var coords []float64
	if err := json.Unmarshal(g.Coordinates, &coords); err != nil || len(coords) < 2 {
		return 0, 0
	}
	return coords[1], coords[0]
}

func gfwDurationMs(raw json.RawMessage) int64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return int64(n)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		s = strings.TrimSpace(s)
		if s == "" {
			return 0
		}
		if d, err := time.ParseDuration(s); err == nil {
			return int64(d / time.Millisecond)
		}
		var f float64
		if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
			return int64(f)
		}
	}
	return 0
}

func parseGFWTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func firstNonEmptyString(xs ...string) string {
	for _, x := range xs {
		if x = strings.TrimSpace(x); x != "" {
			return x
		}
	}
	return ""
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
