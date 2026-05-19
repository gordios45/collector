// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NWS Aviation Weather SIGMET/AIRMET collector.
// https://aviationweather.gov/api/data/ — GeoJSON feed of currently-active
// significant meteorological advisories (SIGMETs: severe weather impacting
// flight; AIRMETs: less severe but widespread).
//
// Polygons are stored as WKT in events.geom so the intel panel can draw
// them on top of flights. No auth.
package nws_sigmet

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

const sigmetURL = "https://aviationweather.gov/api/data/airsigmet?format=geojson"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nws_sigmet" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

type fc struct {
	Type     string    `json:"type"`
	Features []feature `json:"features"`
}
type feature struct {
	Type       string          `json:"type"`
	ID         any             `json:"id"`
	Geometry   json.RawMessage `json:"geometry"`
	Properties map[string]any  `json:"properties"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var j fc
	if err := httpx.GetJSON(ctx, sigmetURL, nil, &j); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(j.Features))
	for _, f := range j.Features {
		if len(f.Geometry) == 0 {
			continue
		}
		// Reuse the shared GeoJSON→WKT helper so polygons stay polygons.
		wkt := geo.GeoJSONToWKT(f.Geometry)
		if wkt == "" {
			continue
		}
		ext := fmt.Sprintf("%v", f.ID)
		if ext == "<nil>" || ext == "" {
			// airsigmetId is the stable key when `id` is missing.
			if s, ok := f.Properties["airSigmetId"].(string); ok {
				ext = s
			} else if s, ok := f.Properties["rawAirSigmet"].(string); ok && len(s) > 32 {
				ext = s[:32]
			} else {
				continue
			}
		}
		ts := now
		if s, ok := f.Properties["validTimeFrom"].(string); ok {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "nws_sigmet",
			ExtID:  ext,
			Geom:   wkt, // leave Lat/Lon 0; scheduler uses Geom when present
			Props:  f.Properties,
		})
	}
	return out, nil
}
