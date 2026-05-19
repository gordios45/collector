// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Safecast — citizen-science radiation monitoring, last 3 days global.
// https://api.safecast.org/measurements.json
package safecast

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "safecast" }
func (c *Collector) PollEvery() time.Duration { return 60 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	since := time.Now().UTC().Add(-3 * 24 * time.Hour).Format("2006-01-02")
	url := fmt.Sprintf(
		"https://api.safecast.org/measurements.json?since=%s&per_page=500&order=captured_at+desc", since)
	var raw []map[string]any
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw))
	for _, m := range raw {
		lat, _ := m["latitude"].(float64)
		lon, _ := m["longitude"].(float64)
		if lat == 0 && lon == 0 {
			continue
		}
		id := fmt.Sprintf("%v", m["id"])
		ts := time.Now().UTC()
		if s, _ := m["captured_at"].(string); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "safecast", ExtID: id,
			Lat: lat, Lon: lon, Props: m,
		})
	}
	return out, nil
}
