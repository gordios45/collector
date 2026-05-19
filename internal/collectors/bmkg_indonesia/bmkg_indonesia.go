// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package bmkg_indonesia ingests official BMKG earthquake feeds for Indonesia.
package bmkg_indonesia

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const sourceID = "bmkg_indonesia"

type Collector struct {
	feeds []feedSpec
}

type feedSpec struct {
	Name string
	URL  string
}

var defaultFeeds = []feedSpec{
	{Name: "autogempa", URL: "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json"},
	{Name: "gempaterkini", URL: "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"},
	{Name: "gempadirasakan", URL: "https://data.bmkg.go.id/DataMKG/TEWS/gempadirasakan.json"},
}

func New() (*Collector, error) {
	feeds := parseFeeds(os.Getenv("BMKG_EARTHQUAKE_FEEDS"))
	if len(feeds) == 0 {
		feeds = defaultFeeds
	}
	return &Collector{feeds: feeds}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("BMKG_POLL_EVERY_S", 300, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, feed := range c.feeds {
		buf, err := httpx.GetBytes(ctx, feed.URL, map[string]string{"Accept": "application/json"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		evs, err := eventsFromFeed(feed, buf)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return dedupe(out), nil
}

type bmkgFeed struct {
	Infogempa struct {
		Gempa json.RawMessage `json:"gempa"`
	} `json:"Infogempa"`
}

type bmkgQuake struct {
	Tanggal     string `json:"Tanggal"`
	Jam         string `json:"Jam"`
	DateTime    string `json:"DateTime"`
	Coordinates string `json:"Coordinates"`
	Lintang     string `json:"Lintang"`
	Bujur       string `json:"Bujur"`
	Magnitude   string `json:"Magnitude"`
	Kedalaman   string `json:"Kedalaman"`
	Wilayah     string `json:"Wilayah"`
	Potensi     string `json:"Potensi"`
	Dirasakan   string `json:"Dirasakan"`
	Shakemap    string `json:"Shakemap"`
}

func eventsFromFeed(feed feedSpec, body []byte) ([]events.Event, error) {
	var raw bmkgFeed
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	if len(raw.Infogempa.Gempa) == 0 {
		return nil, nil
	}
	var rows []bmkgQuake
	if raw.Infogempa.Gempa[0] == '[' {
		if err := json.Unmarshal(raw.Infogempa.Gempa, &rows); err != nil {
			return nil, err
		}
	} else {
		var row bmkgQuake
		if err := json.Unmarshal(raw.Infogempa.Gempa, &row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		ev, ok := eventFromQuake(feed, row)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromQuake(feed feedSpec, row bmkgQuake) (events.Event, bool) {
	lat, lon, ok := parseCoords(row.Coordinates)
	if !ok {
		return events.Event{}, false
	}
	ts := parseTime(row.DateTime)
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	mag, _ := strconv.ParseFloat(strings.TrimSpace(row.Magnitude), 64)
	depthKM := parseLeadingFloat(row.Kedalaman)
	props := map[string]any{
		"source_provider":     "BMKG",
		"source_api_endpoint": feed.URL,
		"feed":                feed.Name,
		"country":             "Indonesia",
		"country_code":        "ID",
		"tanggal":             row.Tanggal,
		"jam":                 row.Jam,
		"datetime":            row.DateTime,
		"coordinates":         row.Coordinates,
		"lintang":             row.Lintang,
		"bujur":               row.Bujur,
		"magnitude":           mag,
		"depth_km":            depthKM,
		"wilayah":             row.Wilayah,
		"potensi":             row.Potensi,
		"dirasakan":           row.Dirasakan,
		"shakemap":            row.Shakemap,
		"hazard_type":         "earthquake",
	}
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  feed.Name + ":" + collectorutil.StableID(fmt.Sprintf("%s|%.4f|%.4f|%s", row.DateTime, lat, lon, row.Magnitude)),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func parseCoords(raw string) (float64, float64, bool) {
	parts := strings.Split(raw, ",")
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	return lat, lon, err1 == nil && err2 == nil && collectorutil.ValidLatLon(lat, lon)
}

func parseLeadingFloat(raw string) float64 {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(fields[0], 64)
	return v
}

func parseTime(raw string) time.Time {
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02T15:04:05-07:00"} {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func parseFeeds(raw string) []feedSpec {
	out := []feedSpec{}
	for _, part := range collectorutil.SplitCSV(raw) {
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			continue
		}
		name := strings.TrimSpace(pieces[0])
		u := strings.TrimSpace(pieces[1])
		if name != "" && strings.HasPrefix(u, "http") {
			out = append(out, feedSpec{Name: name, URL: u})
		}
	}
	return out
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]struct{}{}
	out := make([]events.Event, 0, len(in))
	for _, ev := range in {
		if ev.ExtID == "" {
			continue
		}
		if _, ok := seen[ev.ExtID]; ok {
			continue
		}
		seen[ev.ExtID] = struct{}{}
		out = append(out, ev)
	}
	return out
}
