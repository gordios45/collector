// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// ReliefWeb (OCHA) disasters — active disasters.
// https://api.reliefweb.int/v1/disasters?appname=gordios&preset=latest&filter[field]=status&filter[value]=alert%2Ccurrent
package reliefweb

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

// ReliefWeb requires an approved `appname`. Set RELIEFWEB_APPNAME in .env
// after registering at https://apidoc.reliefweb.int/parameters#appname.
func buildURL() string {
	app := os.Getenv("RELIEFWEB_APPNAME")
	if app == "" {
		app = "gordios" // unregistered — will 403 until you register
	}
	return "https://api.reliefweb.int/v2/disasters?appname=" + app +
		"&preset=latest&limit=200" +
		"&fields[include][]=name&fields[include][]=status&fields[include][]=date" +
		"&fields[include][]=country&fields[include][]=type&fields[include][]=primary_country"
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "reliefweb" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Data []struct {
			ID     any            `json:"id"`
			Fields map[string]any `json:"fields"`
		} `json:"data"`
	}
	if err := httpx.GetJSON(ctx, buildURL(), nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Data))
	for _, d := range raw.Data {
		f := d.Fields
		pc, _ := f["primary_country"].(map[string]any)
		if pc == nil {
			continue
		}
		loc, _ := pc["location"].(map[string]any)
		lat, _ := loc["lat"].(float64)
		lon, _ := loc["lon"].(float64)
		if lat == 0 && lon == 0 {
			continue
		}
		ts := time.Now().UTC()
		if d, ok := f["date"].(map[string]any); ok {
			if s, _ := d["created"].(string); s != "" {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					ts = t.UTC()
				}
			}
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "reliefweb",
			ExtID:  fmt.Sprintf("%v", d.ID),
			Lat:    lat, Lon: lon, Props: f,
		})
	}
	return out, nil
}
