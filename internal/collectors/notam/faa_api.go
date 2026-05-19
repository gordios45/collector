// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// FAA NOTAM API — https://external-api.faa.gov/notamapi/v1/notams
// Requires registered client_id + client_secret (NOTAM_CLIENT_ID / NOTAM_CLIENT_SECRET).
// Global US coverage when keyed. Disabled when creds aren't set.
package notam

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

const faaNotamURL = "https://external-api.faa.gov/notamapi/v1/notams?pageSize=500&responseFormat=geoJson"

type APICollector struct {
	clientID     string
	clientSecret string
}

func NewAPI() (*APICollector, error) {
	id := os.Getenv("NOTAM_CLIENT_ID")
	sec := os.Getenv("NOTAM_CLIENT_SECRET")
	if id == "" || sec == "" {
		return nil, fmt.Errorf("NOTAM_CLIENT_ID / NOTAM_CLIENT_SECRET not set")
	}
	return &APICollector{clientID: id, clientSecret: sec}, nil
}

func (c *APICollector) ID() string               { return "notam_faa" }
func (c *APICollector) PollEvery() time.Duration { return 20 * time.Minute }

func (c *APICollector) Fetch(ctx context.Context) ([]events.Event, error) {
	hdrs := map[string]string{
		"client_id":     c.clientID,
		"client_secret": c.clientSecret,
		"Accept":        "application/json",
	}
	// FAA NOTAM API GeoJSON response
	var raw struct {
		Items []struct {
			Properties map[string]any  `json:"properties"`
			Geometry   json.RawMessage `json:"geometry"`
		} `json:"items"`
	}
	if err := httpx.GetJSON(ctx, faaNotamURL, hdrs, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.Items))
	for _, it := range raw.Items {
		var geom struct {
			Type        string `json:"type"`
			Coordinates any    `json:"coordinates"`
		}
		_ = json.Unmarshal(it.Geometry, &geom)
		lat, lon, ok := pointOrCentroid(geom.Type, geom.Coordinates)
		if !ok {
			continue
		}
		wkt := geo.GeoJSONToWKT(it.Geometry)
		core, _ := it.Properties["coreNOTAMData"].(map[string]any)
		if core == nil {
			core = it.Properties
		}
		notam, _ := core["notam"].(map[string]any)
		if notam == nil {
			notam = core
		}
		ext := fmt.Sprintf("%v", firstNonNil(notam["id"], notam["number"], notam["notamID"]))
		if ext == "<nil>" || ext == "" {
			continue
		}
		props := it.Properties
		if score := collectorutil.AirspaceRestrictionDurationScore(props); score > 0 {
			props["long_duration_restriction_score"] = score
		}
		ev := events.Event{
			Ts: now, Source: "notam_faa", ExtID: ext,
			Lat: lat, Lon: lon, Props: props,
		}
		if wkt != "" {
			ev.Geom = wkt
		}
		out = append(out, ev)
	}
	return out, nil
}

func pointOrCentroid(typ string, coords any) (lat, lon float64, ok bool) {
	switch typ {
	case "Point":
		arr, _ := coords.([]any)
		if len(arr) < 2 {
			return 0, 0, false
		}
		lo, _ := arr[0].(float64)
		la, _ := arr[1].(float64)
		return la, lo, true
	case "Polygon":
		outer, _ := coords.([]any)
		if len(outer) == 0 {
			return 0, 0, false
		}
		ring, _ := outer[0].([]any)
		return ringCentroid(ring)
	}
	return 0, 0, false
}

func ringCentroid(ring []any) (lat, lon float64, ok bool) {
	var sx, sy float64
	n := 0
	for _, p := range ring {
		pair, _ := p.([]any)
		if len(pair) < 2 {
			continue
		}
		lo, _ := pair[0].(float64)
		la, _ := pair[1].(float64)
		sx += lo
		sy += la
		n++
	}
	if n == 0 {
		return 0, 0, false
	}
	return sy / float64(n), sx / float64(n), true
}

func firstNonNil(xs ...any) any {
	for _, x := range xs {
		if x != nil && x != "" {
			return x
		}
	}
	return ""
}
