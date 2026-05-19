// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// OpenAQ — global air quality monitoring stations.
// https://api.openaq.org/v3/locations
// Anonymous access is tight but works; OPENAQ_KEY env unlocks higher limits.
package openaq

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://api.openaq.org/v3/locations?limit=500&order_by=lastUpdated&sort_order=desc"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "openaq" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	hdrs := map[string]string{}
	if k := os.Getenv("OPENAQ_KEY"); k != "" {
		hdrs["X-API-Key"] = k
	}
	var raw struct {
		Results []map[string]any `json:"results"`
	}
	if err := httpx.GetJSON(ctx, url, hdrs, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.Results))
	for _, loc := range raw.Results {
		coords, _ := loc["coordinates"].(map[string]any)
		if coords == nil {
			continue
		}
		lat, _ := coords["latitude"].(float64)
		lon, _ := coords["longitude"].(float64)
		if lat == 0 && lon == 0 {
			continue
		}
		id := fmt.Sprintf("%v", loc["id"])
		out = append(out, events.Event{
			Ts: now, Source: "openaq", ExtID: id,
			Lat: lat, Lon: lon, Props: loc,
		})
	}
	return out, nil
}
