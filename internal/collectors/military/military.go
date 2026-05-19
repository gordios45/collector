// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Military aircraft (adsb.lol /v2/mil — public, no auth).
package military

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type endpoint struct {
	provider string
	url      string
}

var endpoints = []endpoint{
	{provider: "adsb_lol", url: "https://api.adsb.lol/v2/mil"},
	{provider: "airplanes_live", url: "https://api.airplanes.live/v2/mil"},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "military" }
func (c *Collector) PollEvery() time.Duration { return 90 * time.Second }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		AC []map[string]any `json:"ac"`
	}
	var used endpoint
	var lastErr error
	for _, ep := range endpoints {
		raw.AC = nil
		if err := httpx.GetJSON(ctx, ep.url, map[string]string{"Accept": "application/json"}, &raw); err != nil {
			lastErr = fmt.Errorf("%s: %w", ep.provider, err)
			continue
		}
		used = ep
		break
	}
	if used.url == "" {
		return nil, lastErr
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.AC))
	for _, a := range raw.AC {
		hex, _ := a["hex"].(string)
		if hex == "" {
			continue
		}
		lat, _ := a["lat"].(float64)
		lon, _ := a["lon"].(float64)
		if lat == 0 && lon == 0 {
			continue
		}
		a["source_provider"] = used.provider
		a["source_api_endpoint"] = used.url
		a["source_provider_kind"] = "public_adsb_military_aircraft_snapshot"
		a["source_payload_validity"] = map[string]any{
			"valid_start":    now.Add(-2 * time.Minute).Format(time.RFC3339),
			"valid_end":      now.Add(2 * time.Minute).Format(time.RFC3339),
			"validity_basis": "adsb_snapshot_freshness",
		}
		out = append(out, events.Event{
			Ts: now, Source: "military", ExtID: hex,
			Lat: lat, Lon: lon, Props: a,
		})
	}
	return out, nil
}
