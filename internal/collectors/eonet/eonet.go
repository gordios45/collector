// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NASA EONET — open natural events (fires, storms, floods, etc.).
// https://eonet.gsfc.nasa.gov/api/v3/events
//
// Each event may have multiple geometries (a moving storm). We emit the
// latest geometry as the event location and keep the full event in props.
package eonet

import (
	"context"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=500"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "eonet" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Events []map[string]any `json:"events"`
	}
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Events))
	for _, ev := range raw.Events {
		id, _ := ev["id"].(string)
		if id == "" {
			continue
		}
		geoms, _ := ev["geometry"].([]any)
		if len(geoms) == 0 {
			continue
		}
		last, _ := geoms[len(geoms)-1].(map[string]any)
		coords, _ := last["coordinates"].([]any)
		if len(coords) < 2 {
			continue
		}
		lon, ok1 := coords[0].(float64)
		lat, ok2 := coords[1].(float64)
		if !ok1 || !ok2 {
			continue
		}
		ts := time.Now().UTC()
		if s, ok := last["date"].(string); ok {
			if t, err := time.Parse("2006-01-02T15:04:05Z", s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "eonet", ExtID: id,
			Lat: lat, Lon: lon, Props: ev,
		})
	}
	return out, nil
}
