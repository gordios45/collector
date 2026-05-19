// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package avalanche_alerts ingests no-key Avalanche.org public forecast
// polygons as official mountain-weather hazard evidence.
package avalanche_alerts

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "avalanche_alerts"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	const endpoint = "https://api.avalanche.org/v2/public/products/map-layer?day="
	var raw struct {
		Features []struct {
			ID         any            `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string          `json:"type"`
				Coordinates json.RawMessage `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		row := feat.Properties
		danger, _ := floatAny(row["danger_level"])
		dangerText := strings.ToLower(textAny(row["danger"]))
		if danger < 2 && !hasAvalancheWarning(row) && (dangerText == "" || dangerText == "low" || dangerText == "no rating") {
			continue
		}
		lat, lon, ok := geoJSONCentroid(feat.Geometry.Coordinates)
		if !ok {
			continue
		}
		id := firstNonEmpty(textAny(feat.ID), textAny(row["center_id"]), textAny(row["link"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["start_date"])
		end := parseTimeAny(row["end_date"])
		props := map[string]any{
			"source_provider":         firstNonEmpty(textAny(row["center"]), textAny(row["name"]), "Avalanche.org"),
			"source_api_endpoint":     endpoint,
			"source_public_url":       firstNonEmpty(textAny(row["link"]), "https://avalanche.org/"),
			"source_provider_kind":    "official_avalanche_forecast",
			"alert_id":                id,
			"title":                   "Avalanche forecast: " + firstNonEmpty(textAny(row["center"]), textAny(row["name"]), "Avalanche.org"),
			"description":             textAny(row["travel_advice"]),
			"event":                   "Avalanche",
			"severity":                avalancheSeverity(danger, dangerText),
			"danger":                  textAny(row["danger"]),
			"danger_level":            danger,
			"state":                   textAny(row["state"]),
			"avalanche_score":         avalancheScore(danger, dangerText, row),
			"labels":                  []string{"avalanche", "severe_weather", "winter_weather"},
			"source_payload_validity": validity(ts, end),
		}
		copyProps(props, row)
		out = append(out, events.Event{Ts: ts, Source: sourceID, ExtID: stableID("avalanche:" + id), Lat: lat, Lon: lon, Props: props})
	}
	return dedupe(out), nil
}

func hasAvalancheWarning(row map[string]any) bool {
	w, _ := row["warning"].(map[string]any)
	if w == nil {
		return false
	}
	for _, v := range w {
		if strings.TrimSpace(textAny(v)) != "" && !strings.EqualFold(textAny(v), "<nil>") {
			return true
		}
	}
	return false
}

func avalancheScore(danger float64, dangerText string, row map[string]any) float64 {
	score := 0.8 + danger*0.45
	if strings.Contains(dangerText, "considerable") {
		score = math.Max(score, 2.2)
	}
	if strings.Contains(dangerText, "high") || strings.Contains(dangerText, "extreme") || hasAvalancheWarning(row) {
		score = math.Max(score, 2.8)
	}
	return propx.ClampFloat(score, 0, 4)
}

func avalancheSeverity(danger float64, text string) string {
	switch {
	case danger >= 4 || strings.Contains(text, "extreme") || strings.Contains(text, "high"):
		return "Severe"
	case danger >= 3 || strings.Contains(text, "considerable"):
		return "Moderate"
	case danger >= 2:
		return "Minor"
	default:
		return "Unknown"
	}
}

func geoJSONCentroid(raw json.RawMessage) (float64, float64, bool) {
	var coords any
	if err := json.Unmarshal(raw, &coords); err != nil {
		return 0, 0, false
	}
	var latSum, lonSum float64
	var count int
	var walk func(any)
	walk = func(v any) {
		arr, ok := v.([]any)
		if !ok {
			return
		}
		if len(arr) >= 2 {
			if lon, lonOK := floatAny(arr[0]); lonOK {
				if lat, latOK := floatAny(arr[1]); latOK && validLatLon(lat, lon) {
					latSum += lat
					lonSum += lon
					count++
					return
				}
			}
		}
		for _, child := range arr {
			walk(child)
		}
	}
	walk(coords)
	if count == 0 {
		return 0, 0, false
	}
	lat, lon := latSum/float64(count), lonSum/float64(count)
	return lat, lon, validLatLon(lat, lon)
}

func validity(start, end time.Time) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(24 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": "official_avalanche_forecast_window",
	}
}

func parseTimeAny(vals ...any) time.Time {
	for _, v := range vals {
		s := textAny(v)
		if s == "" {
			continue
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Now().UTC()
}

func floatAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func textAny(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func copyProps(dst map[string]any, src map[string]any) {
	for k, v := range src {
		if _, exists := dst[k]; exists {
			dst["raw_"+k] = v
		} else {
			dst[k] = v
		}
	}
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.Source == "" || e.ExtID == "" || !e.HasPoint() {
			continue
		}
		key := e.Source + ":" + e.ExtID
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, e)
	}
	return out
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(h[:])
}
