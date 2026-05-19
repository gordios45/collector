// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package epa_radnet ingests fixed-station EPA RadNet gamma readings for a
// small US watch set. It complements Safecast with official station data.
package epa_radnet

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	baseURL            = "https://radnet.epa.gov/cdx-radnet-rest/api/rest/csv"
	baselineWindowSize = 168
	minBaselineSamples = 48
)

type site struct {
	ID      string
	State   string
	Slug    string
	Name    string
	Lat     float64
	Lon     float64
	Country string
}

var sites = []site{
	{ID: "us-anchorage", State: "AK", Slug: "ANCHORAGE", Name: "Anchorage", Lat: 61.2181, Lon: -149.9003, Country: "United States"},
	{ID: "us-san-francisco", State: "CA", Slug: "SAN%20FRANCISCO", Name: "San Francisco", Lat: 37.7749, Lon: -122.4194, Country: "United States"},
	{ID: "us-washington-dc", State: "DC", Slug: "WASHINGTON", Name: "Washington, DC", Lat: 38.9072, Lon: -77.0369, Country: "United States"},
	{ID: "us-honolulu", State: "HI", Slug: "HONOLULU", Name: "Honolulu", Lat: 21.3099, Lon: -157.8581, Country: "United States"},
	{ID: "us-chicago", State: "IL", Slug: "CHICAGO", Name: "Chicago", Lat: 41.8781, Lon: -87.6298, Country: "United States"},
	{ID: "us-boston", State: "MA", Slug: "BOSTON", Name: "Boston", Lat: 42.3601, Lon: -71.0589, Country: "United States"},
	{ID: "us-albany", State: "NY", Slug: "ALBANY", Name: "Albany", Lat: 42.6526, Lon: -73.7562, Country: "United States"},
	{ID: "us-philadelphia", State: "PA", Slug: "PHILADELPHIA", Name: "Philadelphia", Lat: 39.9526, Lon: -75.1652, Country: "United States"},
	{ID: "us-houston", State: "TX", Slug: "HOUSTON", Name: "Houston", Lat: 29.7604, Lon: -95.3698, Country: "United States"},
	{ID: "us-seattle", State: "WA", Slug: "SEATTLE", Name: "Seattle", Lat: 47.6062, Lon: -122.3321, Country: "United States"},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "epa_radnet" }
func (c *Collector) PollEvery() time.Duration { return 60 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	year := time.Now().UTC().Year()
	out := []events.Event{}
	var firstErr error
	for _, s := range sites {
		url := fmt.Sprintf("%s/%d/fixed/%s/%s", baseURL, year, s.State, s.Slug)
		buf, err := httpx.GetBytes(ctx, url, map[string]string{"Accept": "text/csv,*/*"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ev, ok := eventFromCSV(s, url, buf)
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type reading struct {
	ObservedAt time.Time
	Value      float64
}

func eventFromCSV(s site, url string, buf []byte) (events.Event, bool) {
	readings := parseApprovedReadings(string(buf))
	if len(readings) < 2 {
		return events.Event{}, false
	}
	latest := readings[len(readings)-1]
	baselineStart := len(readings) - 1 - baselineWindowSize
	if baselineStart < 0 {
		baselineStart = 0
	}
	base := readings[baselineStart : len(readings)-1]
	vals := make([]float64, 0, len(base))
	for _, r := range base {
		vals = append(vals, r.Value)
	}
	mean := avg(vals)
	if mean == 0 {
		mean = latest.Value
	}
	sigma := 0.0
	if len(vals) >= minBaselineSamples {
		sigma = stddev(vals, mean)
	}
	z := 0.0
	if sigma > 0 {
		z = (latest.Value - mean) / sigma
	}
	delta := latest.Value - mean
	severity := "normal"
	switch {
	case delta >= 15 || z >= 3:
		severity = "spike"
	case delta >= 8 || z >= 2:
		severity = "elevated"
	}
	props := map[string]any{
		"station_id":          s.ID,
		"station_name":        s.Name,
		"state":               s.State,
		"country":             s.Country,
		"value":               round(latest.Value, 1),
		"unit":                "nSv/h",
		"observed_at":         latest.ObservedAt.Format(time.RFC3339),
		"baseline_value":      round(mean, 1),
		"baseline_samples":    len(vals),
		"delta":               round(delta, 1),
		"z_score":             round(z, 2),
		"severity":            severity,
		"freshness":           freshness(latest.ObservedAt),
		"source_api_endpoint": url,
	}
	return events.Event{
		Ts:     latest.ObservedAt,
		Source: "epa_radnet",
		ExtID:  fmt.Sprintf("%s:%s", s.ID, latest.ObservedAt.Format("20060102T150405")),
		Lat:    s.Lat,
		Lon:    s.Lon,
		Props:  props,
	}, true
}

func parseApprovedReadings(csv string) []reading {
	lines := strings.Split(strings.TrimSpace(csv), "\n")
	if len(lines) < 2 {
		return nil
	}
	out := []reading{}
	for _, raw := range lines[1:] {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		cols := splitCSVLine(line)
		if len(cols) < 3 {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(cols[len(cols)-1]))
		if status != "APPROVED" {
			continue
		}
		ts, ok := parseRadnetTime(cols[1])
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(cols[2]), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out = append(out, reading{ObservedAt: ts, Value: v})
	}
	return out
}

func splitCSVLine(line string) []string {
	out := []string{}
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch ch {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				out = append(out, b.String())
				b.Reset()
				continue
			}
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	out = append(out, b.String())
	return out
}

func parseRadnetTime(raw string) (time.Time, bool) {
	t, err := time.Parse("01/02/2006 15:04:05", strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC), true
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func stddev(vals []float64, mean float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		d := v - mean
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(vals)-1))
}

func freshness(ts time.Time) string {
	age := time.Since(ts)
	if age <= 6*time.Hour {
		return "live"
	}
	if age <= 14*24*time.Hour {
		return "recent"
	}
	return "historical"
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}
