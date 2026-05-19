// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA SWPC — space weather.
// Two endpoints merged: planetary-k-index (global) + alerts (event list).
// k-index lands as a single "global" event with props.kp; alerts land with
// alert id → props.
package space_weather

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	kpURL     = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
	alertsURL = "https://services.swpc.noaa.gov/products/alerts.json"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "space_weather" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}

	// --- Planetary K-index ---
	var kpRaw [][]any
	if err := httpx.GetJSON(ctx, kpURL, nil, &kpRaw); err == nil && len(kpRaw) > 1 {
		// first row is header; last row is the latest kp reading.
		row := kpRaw[len(kpRaw)-1]
		if len(row) >= 2 {
			tsStr, _ := row[0].(string)
			kp := row[1] // number or string
			ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
			if err != nil {
				ts = time.Now().UTC()
			}
			out = append(out, events.Event{
				Ts: ts.UTC(), Source: "space_weather", ExtID: "kp_latest",
				Lat: 0, Lon: 0, Props: map[string]any{"kp": kp, "metric": "planetary_k_index"},
			})
		}
	}

	// --- Alerts ---
	var alerts []map[string]any
	if err := httpx.GetJSON(ctx, alertsURL, nil, &alerts); err != nil {
		if len(out) == 0 {
			return nil, err
		}
		return out, nil
	}
	for _, a := range alerts {
		ext := fmt.Sprintf("%v", a["product_id"])
		ts := time.Now().UTC()
		if s, _ := a["issue_datetime"].(string); s != "" {
			if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "space_weather", ExtID: "alert_" + ext,
			Lat: 0, Lon: 0, Props: a,
		})
	}
	return out, nil
}
