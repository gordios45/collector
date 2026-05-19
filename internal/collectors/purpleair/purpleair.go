// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package purpleair ingests configured PurpleAir AOIs for sub-municipal PM2.5
// spikes. The collector is deliberately AOI-gated: there is no global scrape.
package purpleair

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const endpoint = "https://api.purpleair.com/v1/sensors"

type aoi struct {
	Label    string
	Lat      float64
	Lon      float64
	RadiusKM float64
}

type Collector struct {
	apiKey string
	aois   []aoi
}

func New() (*Collector, error) {
	key := strings.TrimSpace(os.Getenv("PURPLEAIR_API_KEY"))
	if key == "" {
		return nil, fmt.Errorf("skipped: set PURPLEAIR_API_KEY")
	}
	aois := parseAOIs(os.Getenv("PURPLEAIR_AOIS"))
	if len(aois) == 0 {
		return nil, fmt.Errorf("skipped: set PURPLEAIR_AOIS=label:lat:lon:radius_km")
	}
	return &Collector{apiKey: key, aois: aois}, nil
}

func (c *Collector) ID() string               { return "purpleair" }
func (c *Collector) PollEvery() time.Duration { return 2 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, watch := range c.aois {
		u := queryURL(watch)
		var body apiResponse
		err := httpx.GetJSON(ctx, u, map[string]string{
			"Accept":    "application/json",
			"X-API-Key": c.apiKey,
		}, &body)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, eventsFromResponse(body, watch, u)...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return dedupe(out), nil
}

type apiResponse struct {
	Fields []string `json:"fields"`
	Data   [][]any  `json:"data"`
}

func queryURL(watch aoi) string {
	latD := watch.RadiusKM / 111.0
	lonD := watch.RadiusKM / math.Max(20.0, 111.0*math.Cos(watch.Lat*math.Pi/180.0))
	q := url.Values{}
	q.Set("fields", "sensor_index,name,latitude,longitude,pm2.5_atm,pm2.5_cf_1,humidity,temperature,last_seen,confidence,location_type")
	q.Set("location_type", "0")
	q.Set("nwlng", fmt.Sprintf("%.6f", watch.Lon-lonD))
	q.Set("nwlat", fmt.Sprintf("%.6f", watch.Lat+latD))
	q.Set("selng", fmt.Sprintf("%.6f", watch.Lon+lonD))
	q.Set("selat", fmt.Sprintf("%.6f", watch.Lat-latD))
	return endpoint + "?" + q.Encode()
}

func eventsFromResponse(body apiResponse, watch aoi, sourceURL string) []events.Event {
	idx := fieldIndex(body.Fields)
	out := make([]events.Event, 0, len(body.Data))
	for _, row := range body.Data {
		lat, okLat := rowFloat(row, idx, "latitude")
		lon, okLon := rowFloat(row, idx, "longitude")
		if !okLat || !okLon || math.Abs(lat) > 90 || math.Abs(lon) > 180 {
			continue
		}
		pm25, okPM := rowFloat(row, idx, "pm2.5_atm")
		if !okPM {
			pm25, okPM = rowFloat(row, idx, "pm2.5_cf_1")
		}
		if !okPM {
			continue
		}
		lastSeen := rowTime(row, idx, "last_seen")
		if lastSeen.IsZero() {
			lastSeen = time.Now().UTC()
		}
		sensorID := propx.FirstNonEmpty(rowString(row, idx, "sensor_index"), rowString(row, idx, "id"), rowString(row, idx, "name"))
		if sensorID == "" {
			sensorID = fmt.Sprintf("%.5f:%.5f", lat, lon)
		}
		score, severity := pm25Score(pm25)
		props := map[string]any{
			"sensor_index":            sensorID,
			"name":                    rowString(row, idx, "name"),
			"aoi_label":               watch.Label,
			"pm25_atm_ugm3":           round(pm25, 1),
			"pm25_cf1_ugm3":           roundFromRow(row, idx, "pm2.5_cf_1", 1),
			"humidity":                roundFromRow(row, idx, "humidity", 1),
			"temperature":             roundFromRow(row, idx, "temperature", 1),
			"confidence":              roundFromRow(row, idx, "confidence", 1),
			"severity":                severity,
			"air_particulate_score":   round(score, 2),
			"last_seen":               lastSeen.Format(time.RFC3339),
			"source_api_endpoint":     sourceURL,
			"source_payload_validity": validity(lastSeen, 10*time.Minute, "purpleair_sensor_last_seen"),
		}
		out = append(out, events.Event{
			Ts:     lastSeen,
			Source: "purpleair",
			ExtID:  stableID(sensorID + ":" + lastSeen.Format("20060102T1504")),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out
}

func pm25Score(pm25 float64) (float64, string) {
	switch {
	case pm25 >= 150.5:
		return propx.ClampFloat(2.7+(pm25-150.5)/100.0, 0, 4), "severe"
	case pm25 >= 55.5:
		return propx.ClampFloat(1.4+(pm25-55.5)/70.0, 0, 3.5), "high"
	case pm25 >= 35.5:
		return propx.ClampFloat(0.8+(pm25-35.5)/25.0, 0, 2.5), "elevated"
	default:
		return propx.ClampFloat(pm25/50.0, 0, 1), "normal"
	}
}

func fieldIndex(fields []string) map[string]int {
	out := map[string]int{}
	for i, f := range fields {
		out[strings.ToLower(strings.TrimSpace(f))] = i
	}
	return out
}

func rowFloat(row []any, idx map[string]int, field string) (float64, bool) {
	i, ok := idx[strings.ToLower(field)]
	if !ok || i < 0 || i >= len(row) {
		return 0, false
	}
	return propx.Float(row[i])
}

func rowString(row []any, idx map[string]int, field string) string {
	i, ok := idx[strings.ToLower(field)]
	if !ok || i < 0 || i >= len(row) || row[i] == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(row[i]))
}

func rowTime(row []any, idx map[string]int, field string) time.Time {
	v, ok := rowFloat(row, idx, field)
	if !ok || v <= 0 {
		return time.Time{}
	}
	return time.Unix(int64(v), 0).UTC()
}

func roundFromRow(row []any, idx map[string]int, field string, digits int) any {
	v, ok := rowFloat(row, idx, field)
	if !ok {
		return nil
	}
	return round(v, digits)
}

func parseAOIs(raw string) []aoi {
	out := []aoi{}
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(item, ":")
		if len(parts) != 4 {
			continue
		}
		lat, errLat := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		lon, errLon := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		radius, errRadius := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
		if errLat != nil || errLon != nil || errRadius != nil || math.Abs(lat) > 90 || math.Abs(lon) > 180 || radius <= 0 {
			continue
		}
		out = append(out, aoi{Label: strings.TrimSpace(parts[0]), Lat: lat, Lon: lon, RadiusKM: radius})
	}
	return out
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(strings.ToLower(s))))
	return "purpleair:" + hex.EncodeToString(h[:])
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.ExtID == "" || e.Source == "" || !e.HasPoint() {
			continue
		}
		k := e.Source + "|" + e.ExtID
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, e)
	}
	return out
}
