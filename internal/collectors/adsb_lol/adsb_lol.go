// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// ADSB.lol collector.
//
// Default mode polls the free /v2/mil endpoint. Operators can constrain
// collection with ADSB_LOL_AOIS="label:lat:lon:dist_nm,..." or override
// endpoints with ADSB_LOL_ENDPOINTS="mil,ladd,lat/38.9/lon/-77/dist/25".
package adsb_lol

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const baseURL = "https://api.adsb.lol/v2"

type Collector struct {
	endpoints []endpoint
	maxRows   int
	client    *http.Client
}

type endpoint struct {
	Label string
	URL   string
}

func New() (*Collector, error) {
	endpoints := parseEndpoints(os.Getenv("ADSB_LOL_ENDPOINTS"))
	endpoints = append(endpoints, parseAOIs(os.Getenv("ADSB_LOL_AOIS"))...)
	if len(endpoints) == 0 {
		endpoints = []endpoint{{Label: "mil", URL: baseURL + "/mil"}}
	}
	return &Collector{
		endpoints: endpoints,
		maxRows:   envInt("ADSB_LOL_MAX_AIRCRAFT", 1200),
		client:    &http.Client{Timeout: 40 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "adsb_lol" }
func (c *Collector) PollEvery() time.Duration { return 90 * time.Second }

type v2Response struct {
	AC    []aircraft `json:"ac"`
	Now   int64      `json:"now"`
	CTime int64      `json:"ctime"`
	Msg   string     `json:"msg"`
	Total int        `json:"total"`
}

type aircraft struct {
	Hex       string          `json:"hex"`
	Type      string          `json:"type"`
	Flight    string          `json:"flight"`
	Reg       string          `json:"r"`
	Aircraft  string          `json:"t"`
	AltBaro   json.RawMessage `json:"alt_baro"`
	AltGeom   json.RawMessage `json:"alt_geom"`
	Lat       *float64        `json:"lat"`
	Lon       *float64        `json:"lon"`
	GS        *float64        `json:"gs"`
	Track     *float64        `json:"track"`
	BaroRate  json.RawMessage `json:"baro_rate"`
	Squawk    string          `json:"squawk"`
	Emergency string          `json:"emergency"`
	Category  string          `json:"category"`
	Seen      *float64        `json:"seen"`
	SeenPos   *float64        `json:"seen_pos"`
	Messages  int             `json:"messages"`
	RSSI      *float64        `json:"rssi"`
	DBFlags   *int            `json:"dbFlags"`
	Alert     *int            `json:"alert"`
	SPI       *int            `json:"spi"`
	MLAT      []string        `json:"mlat"`
	TISB      []string        `json:"tisb"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	var lastErr error
	for _, ep := range c.endpoints {
		evs, err := c.fetchEndpoint(ctx, ep)
		if err != nil {
			lastErr = err
			continue
		}
		out = append(out, evs...)
		if c.maxRows > 0 && len(out) >= c.maxRows {
			out = out[:c.maxRows]
			break
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

func (c *Collector) fetchEndpoint(ctx context.Context, ep endpoint) ([]events.Event, error) {
	body, usedURL, err := c.fetchEndpointBody(ctx, ep.URL)
	if err != nil {
		return nil, err
	}
	var payload v2Response
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("parse adsb_lol %s: %w", ep.Label, err)
	}
	snap := adsbTime(payload.Now)
	if snap.IsZero() {
		snap = time.Now().UTC()
	}
	out := make([]events.Event, 0, min(len(payload.AC), c.maxRows))
	for _, ac := range payload.AC {
		ev, ok := aircraftEvent(ac, ep.Label, snap)
		if ok {
			ev.Props["source_provider"] = providerName(usedURL)
			ev.Props["source_api_endpoint"] = usedURL
			out = append(out, ev)
		}
		if c.maxRows > 0 && len(out) >= c.maxRows {
			break
		}
	}
	return out, nil
}

func (c *Collector) fetchEndpointBody(ctx context.Context, rawURL string) ([]byte, string, error) {
	headers := map[string]string{"Accept": "application/json"}
	body, err := httpx.GetBytesWithClient(ctx, c.client, rawURL, headers)
	if err == nil {
		return body, rawURL, nil
	}
	lastErr := err
	if alt := alternateADSBURL(rawURL); alt != "" {
		if body, altErr := httpx.GetBytesWithClient(ctx, c.client, alt, headers); altErr == nil {
			return body, alt, nil
		} else {
			lastErr = fmt.Errorf("%v; fallback %s: %w", err, alt, altErr)
		}
	}
	return nil, rawURL, lastErr
}

func alternateADSBURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	switch u.Host {
	case "api.adsb.lol":
		u.Host = "api.airplanes.live"
	case "api.airplanes.live":
		u.Host = "api.adsb.lol"
	default:
		return ""
	}
	return u.String()
}

func providerName(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "adsb_lol"
	}
	switch u.Host {
	case "api.airplanes.live":
		return "airplanes_live"
	default:
		return "adsb_lol"
	}
}

func aircraftEvent(ac aircraft, endpointLabel string, snap time.Time) (events.Event, bool) {
	hex := strings.TrimSpace(strings.ToLower(ac.Hex))
	if hex == "" || ac.Lat == nil || ac.Lon == nil || !validLatLon(*ac.Lat, *ac.Lon) {
		return events.Event{}, false
	}
	ts := snap
	if ac.SeenPos != nil && *ac.SeenPos >= 0 && *ac.SeenPos < 3600 {
		ts = snap.Add(-time.Duration(*ac.SeenPos * float64(time.Second)))
	} else if ac.Seen != nil && *ac.Seen >= 0 && *ac.Seen < 3600 {
		ts = snap.Add(-time.Duration(*ac.Seen * float64(time.Second)))
	}
	alt, onGround := altitudeMeters(ac.AltBaro, ac.AltGeom)
	velocity := 0.0
	if ac.GS != nil {
		velocity = *ac.GS * 0.514444
	}
	heading := 0.0
	if ac.Track != nil {
		heading = *ac.Track
	}
	verticalRate := rawNumber(ac.BaroRate) * 0.00508
	military := endpointLabel == "mil" || (ac.DBFlags != nil && *ac.DBFlags&1 == 1)
	props := map[string]any{
		"source_provider": "adsb_lol",
		"endpoint":        endpointLabel,
		"icao24":          hex,
		"hex":             hex,
		"callsign":        strings.TrimSpace(ac.Flight),
		"registration":    strings.TrimSpace(ac.Reg),
		"aircraft_type":   strings.TrimSpace(ac.Aircraft),
		"category":        strings.TrimSpace(ac.Category),
		"adsb_type":       strings.TrimSpace(ac.Type),
		"military":        military,
		"alt":             alt,
		"on_ground":       onGround,
		"velocity_m_s":    velocity,
		"heading_deg":     heading,
		"vertical_rate":   verticalRate,
		"squawk":          strings.TrimSpace(ac.Squawk),
		"emergency":       strings.TrimSpace(ac.Emergency),
		"messages":        ac.Messages,
		"observed_start":  ts.Format(time.RFC3339),
		"observed_end":    snap.Format(time.RFC3339),
		"source_payload_validity": map[string]any{
			"valid_start":    ts.Format(time.RFC3339),
			"valid_end":      snap.Add(2 * time.Minute).Format(time.RFC3339),
			"validity_basis": "adsb_snapshot_freshness",
		},
	}
	if ac.DBFlags != nil {
		props["db_flags"] = *ac.DBFlags
	}
	if ac.RSSI != nil {
		props["rssi"] = *ac.RSSI
	}
	if ac.Alert != nil {
		props["alert"] = *ac.Alert
	}
	if ac.SPI != nil {
		props["spi"] = *ac.SPI
	}
	if len(ac.MLAT) > 0 {
		props["mlat_fields"] = ac.MLAT
	}
	if len(ac.TISB) > 0 {
		props["tisb_fields"] = ac.TISB
	}
	return events.Event{
		Ts:     ts.UTC(),
		Source: "adsb_lol",
		ExtID:  endpointLabel + ":" + hex,
		Lat:    *ac.Lat,
		Lon:    *ac.Lon,
		Props:  props,
	}, true
}

func altitudeMeters(baro, geom json.RawMessage) (float64, bool) {
	if strings.EqualFold(strings.Trim(string(baro), `"`), "ground") {
		return 0, true
	}
	if ft := rawNumber(baro); ft != 0 {
		return ft * 0.3048, false
	}
	if ft := rawNumber(geom); ft != 0 {
		return ft * 0.3048, false
	}
	return 0, false
}

func rawNumber(raw json.RawMessage) float64 {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if _, err := fmt.Sscanf(strings.TrimSpace(s), "%f", &f); err == nil {
			return f
		}
	}
	return 0
}

func adsbTime(raw int64) time.Time {
	switch {
	case raw > 1_000_000_000_000:
		return time.UnixMilli(raw).UTC()
	case raw > 1_000_000_000:
		return time.Unix(raw, 0).UTC()
	default:
		return time.Time{}
	}
}

func parseEndpoints(raw string) []endpoint {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []endpoint
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		label, value := splitLabelValue(item)
		if value == "" {
			continue
		}
		if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
			value = strings.TrimLeft(value, "/")
			value = baseURL + "/" + value
		}
		if label == "" {
			label = endpointLabel(value)
		}
		out = append(out, endpoint{Label: label, URL: value})
	}
	return out
}

func parseAOIs(raw string) []endpoint {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []endpoint
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if len(parts) != 4 {
			continue
		}
		lat, err1 := strconv.ParseFloat(parts[1], 64)
		lon, err2 := strconv.ParseFloat(parts[2], 64)
		dist, err3 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil || err3 != nil || !validLatLon(lat, lon) {
			continue
		}
		if dist < 1 {
			dist = 1
		}
		if dist > 250 {
			dist = 250
		}
		label := strings.TrimSpace(parts[0])
		escapedLat := url.PathEscape(fmt.Sprintf("%.5f", lat))
		escapedLon := url.PathEscape(fmt.Sprintf("%.5f", lon))
		out = append(out, endpoint{
			Label: firstNonEmpty(label, fmt.Sprintf("aoi_%s_%s", escapedLat, escapedLon)),
			URL:   fmt.Sprintf("%s/lat/%s/lon/%s/dist/%d", baseURL, escapedLat, escapedLon, dist),
		})
	}
	return out
}

func endpointLabel(raw string) string {
	raw = strings.TrimRight(raw, "/")
	if idx := strings.LastIndex(raw, "/"); idx >= 0 && idx < len(raw)-1 {
		return strings.ReplaceAll(raw[idx+1:], " ", "_")
	}
	return "adsb_lol"
}

func splitLabelValue(raw string) (string, string) {
	for _, sep := range []string{"|", "="} {
		if idx := strings.Index(raw, sep); idx > 0 {
			return strings.TrimSpace(raw[:idx]), strings.TrimSpace(raw[idx+1:])
		}
	}
	return "", raw
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x = strings.TrimSpace(x); x != "" {
			return x
		}
	}
	return ""
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
