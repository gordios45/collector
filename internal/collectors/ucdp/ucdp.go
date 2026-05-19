// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// UCDP — Uppsala Conflict Data Program GED candidate events, last 30 days.
// https://ucdpapi.pcr.uu.se/api/gedevents/23.1?pagesize=500&DateStart=YYYY-MM-DD
package ucdp

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "ucdp" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	since := time.Now().UTC().Add(-30 * 24 * time.Hour).Format("2006-01-02")
	url := fmt.Sprintf(
		"https://ucdpapi.pcr.uu.se/api/gedevents/24.1?pagesize=500&DateStart=%s", since)
	var raw struct {
		Result []map[string]any `json:"Result"`
	}
	hdrs := map[string]string{}
	if tok := os.Getenv("UCDP_ACCESS_TOKEN"); tok != "" {
		hdrs["x-ucdp-access-token"] = tok
	}
	if err := httpx.GetJSON(ctx, url, hdrs, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Result))
	for _, e := range raw.Result {
		lat, _ := e["latitude"].(float64)
		lon, _ := e["longitude"].(float64)
		if lat == 0 && lon == 0 {
			continue
		}
		id := fmt.Sprintf("%v", e["id"])
		if id == "<nil>" || id == "" {
			continue
		}
		ts := time.Now().UTC()
		if s, ok := e["date_start"].(string); ok {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "ucdp", ExtID: id,
			Lat: lat, Lon: lon, Props: e,
		})
	}
	return out, nil
}
