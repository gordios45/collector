// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NWS CAP active alerts. GeoJSON with mixed Point/Polygon/MultiPolygon.
// https://api.weather.gov/alerts/active
package nws_alerts

import (
	"context"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://api.weather.gov/alerts/active?status=actual&severity=Extreme,Severe,Moderate"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nws_alerts" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Features []struct {
			ID         string         `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   map[string]any `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, url, map[string]string{"Accept": "application/geo+json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		if f.Geometry == nil {
			continue
		}
		lat, lon, ok := centroid(f.Geometry)
		if !ok {
			continue
		}
		ts := time.Now().UTC()
		if s, _ := f.Properties["sent"].(string); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "nws_alerts", ExtID: f.ID,
			Lat: lat, Lon: lon, Props: f.Properties,
		})
	}
	return out, nil
}

// centroid of a GeoJSON geometry object (Point / Polygon / MultiPolygon).
func centroid(g map[string]any) (lat, lon float64, ok bool) {
	typ, _ := g["type"].(string)
	coords, _ := g["coordinates"].([]any)
	switch typ {
	case "Point":
		if len(coords) < 2 {
			return 0, 0, false
		}
		x, _ := coords[0].(float64)
		y, _ := coords[1].(float64)
		return y, x, true
	case "Polygon":
		if len(coords) == 0 {
			return 0, 0, false
		}
		ring, _ := coords[0].([]any)
		return ringCentroid(ring)
	case "MultiPolygon":
		if len(coords) == 0 {
			return 0, 0, false
		}
		poly, _ := coords[0].([]any)
		if len(poly) == 0 {
			return 0, 0, false
		}
		ring, _ := poly[0].([]any)
		return ringCentroid(ring)
	}
	return 0, 0, false
}

func ringCentroid(ring []any) (lat, lon float64, ok bool) {
	if len(ring) == 0 {
		return 0, 0, false
	}
	var sx, sy float64
	n := 0
	for _, p := range ring {
		pair, _ := p.([]any)
		if len(pair) < 2 {
			continue
		}
		x, _ := pair[0].(float64)
		y, _ := pair[1].(float64)
		sx += x
		sy += y
		n++
	}
	if n == 0 {
		return 0, 0, false
	}
	return sy / float64(n), sx / float64(n), true
}
