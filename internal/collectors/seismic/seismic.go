// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// USGS earthquakes, last 24h, all magnitudes.
// https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
package seismic

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "seismic" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Features []struct {
			ID         string         `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Coordinates []float64 `json:"coordinates"` // [lon, lat, depth]
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		if len(f.Geometry.Coordinates) < 2 {
			continue
		}
		lon, lat := f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
		var depth float64
		if len(f.Geometry.Coordinates) >= 3 {
			depth = f.Geometry.Coordinates[2]
		}
		ts := time.Now().UTC()
		if v, ok := f.Properties["time"].(float64); ok && v > 0 {
			ts = time.UnixMilli(int64(v)).UTC()
		}
		props := map[string]any{"depth_km": depth}
		for k, v := range f.Properties {
			props[k] = v
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "seismic",
			ExtID:  f.ID,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	_ = fmt.Sprintf // (placeholder to keep import explicit)
	return out, nil
}
