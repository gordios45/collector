// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Flights collector — OpenSky /api/states/all.
//
// Poll cadence: 60 s. OpenSky's authenticated quota is 4 000 credit/day
// (~1 credit per all-states request), so 60 s × 1440 = 1 440 credits —
// comfortably under the cap with headroom for retries.
//
// Data shape (OpenSky array rows are documented at
// https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors):
//
//	0: icao24   1: callsign   2: origin_country   3: time_position
//	4: last_contact   5: lon   6: lat   7: baro_alt   8: on_ground
//	9: velocity_m_s   10: true_track_deg   11: vertical_rate_m_s
//	13: geo_alt_m   14: squawk   16: position_source
package flights

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const statesURL = "https://opensky-network.org/api/states/all"

type Collector struct {
	user, pass string // optional
	client     *http.Client
}

// New never errors — flights is always enabled. With OPENSKY_USER/PASS
// the collector polls fast (60 s). Anonymous → 240 s to stay within the
// 400 credit/day anonymous quota.
func New() (*Collector, error) {
	return &Collector{
		user:   os.Getenv("OPENSKY_USER"),
		pass:   os.Getenv("OPENSKY_PASS"),
		client: &http.Client{Timeout: 25 * time.Second},
	}, nil
}

func (c *Collector) ID() string { return "flights" }

func (c *Collector) PollEvery() time.Duration {
	if c.user != "" && c.pass != "" {
		return 60 * time.Second
	}
	return 240 * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, statesURL, nil)
	if c.user != "" && c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")

	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	if r.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("opensky 429 — backing off")
	}
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 200))
		return nil, fmt.Errorf("opensky %d: %s", r.StatusCode, string(body))
	}

	var payload struct {
		Time   int64   `json:"time"`
		States [][]any `json:"states"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	snapTs := time.Unix(payload.Time, 0).UTC()
	if payload.Time == 0 {
		snapTs = time.Now().UTC()
	}

	out := make([]events.Event, 0, len(payload.States))
	for _, s := range payload.States {
		if len(s) < 17 {
			continue
		}
		icao, _ := s[0].(string)
		if icao == "" {
			continue
		}
		lat, latOK := toFloat(s[6])
		lon, lonOK := toFloat(s[5])
		if !latOK || !lonOK {
			continue
		}
		callsign, _ := s[1].(string)
		country, _ := s[2].(string)
		baroAlt, _ := toFloat(s[7])
		onGround, _ := s[8].(bool)
		velocity, _ := toFloat(s[9])
		heading, _ := toFloat(s[10])
		verticalRate, _ := toFloat(s[11])
		geoAlt, _ := toFloat(s[13])

		alt := baroAlt
		if alt == 0 {
			alt = geoAlt
		}

		props := map[string]any{
			"icao24":          icao,
			"callsign":        trim(callsign),
			"country":         country,
			"alt":             alt,
			"baro_alt_m":      baroAlt,
			"geo_alt_m":       geoAlt,
			"on_ground":       onGround,
			"velocity_m_s":    velocity,
			"heading_deg":     heading,
			"vertical_rate":   verticalRate,
			"source_provider": "opensky",
		}
		if tp, ok := toFloat(s[3]); ok {
			props["time_position"] = tp
		}
		out = append(out, events.Event{
			Ts:     snapTs,
			Source: "flights",
			ExtID:  icao,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out, nil
}

// ---- tiny helpers ----

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func trim(s string) string {
	// OpenSky pads callsigns with spaces.
	for len(s) > 0 && s[len(s)-1] == ' ' {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	return s
}
