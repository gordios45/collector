// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// USGS elevated volcanoes.
// https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes
// Returns a list of objects with latitude/longitude/volcano_name/volcano_id.
package volcanoes

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "volcanoes" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw []map[string]any
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw))
	for _, v := range raw {
		lat, _ := toFloat(v["latitude"])
		lon, _ := toFloat(v["longitude"])
		if lat == 0 && lon == 0 {
			continue
		}
		ext := fmt.Sprintf("%v", firstNonNil(v["volcano_id"], v["vnum"], v["volcano_name"]))
		out = append(out, events.Event{
			Ts: now, Source: "volcanoes", ExtID: ext,
			Lat: lat, Lon: lon, Props: v,
		})
	}
	return out, nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		var f float64
		_, err := fmt.Sscanf(x, "%f", &f)
		return f, err == nil
	}
	return 0, false
}
func firstNonNil(args ...any) any {
	for _, a := range args {
		if a != nil && a != "" {
			return a
		}
	}
	return "—"
}
