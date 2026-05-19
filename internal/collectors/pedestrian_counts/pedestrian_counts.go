// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package pedestrian_counts ingests public aggregate pedestrian counter feeds.
// It only stores sensor-level counts, never device- or person-level records.
package pedestrian_counts

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	sourceID              = "pedestrian_counts"
	melbourneLocationsURL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-sensor-locations/records"
	melbournePastHourURL  = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-past-hour-counts-per-minute/records"
)

type Collector struct {
	limit int
}

func New() (*Collector, error) {
	return &Collector{limit: envInt("PEDESTRIAN_COUNTS_LIMIT", 100, 1, 100)}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("PEDESTRIAN_COUNTS_POLL_EVERY_S", 300, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	locations, err := fetchMelbourneLocations(ctx)
	if err != nil {
		return nil, err
	}
	counts, err := fetchMelbourneCounts(ctx, c.limit)
	if err != nil {
		return nil, err
	}
	return melbourneEvents(locations, counts), nil
}

type recordsResponse[T any] struct {
	Results []T `json:"results"`
}

type melbourneLocation struct {
	LocationID        int     `json:"location_id"`
	SensorDescription string  `json:"sensor_description"`
	SensorName        string  `json:"sensor_name"`
	LocationType      string  `json:"location_type"`
	Status            string  `json:"status"`
	Direction1        string  `json:"direction_1"`
	Direction2        string  `json:"direction_2"`
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
}

type melbourneCount struct {
	LocationID        int    `json:"location_id"`
	SensingDatetime   string `json:"sensing_datetime"`
	SensingDate       string `json:"sensing_date"`
	SensingTime       string `json:"sensing_time"`
	Direction1        int    `json:"direction_1"`
	Direction2        int    `json:"direction_2"`
	TotalOfDirections int    `json:"total_of_directions"`
}

func fetchMelbourneLocations(ctx context.Context) (map[int]melbourneLocation, error) {
	q := url.Values{}
	q.Set("limit", "100")
	endpoint := melbourneLocationsURL + "?" + q.Encode()
	var raw recordsResponse[melbourneLocation]
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil, err
	}
	out := map[int]melbourneLocation{}
	for _, row := range raw.Results {
		if row.LocationID != 0 && validLatLon(row.Latitude, row.Longitude) {
			out[row.LocationID] = row
		}
	}
	return out, nil
}

func fetchMelbourneCounts(ctx context.Context, limit int) ([]melbourneCount, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("order_by", "sensing_datetime desc")
	endpoint := melbournePastHourURL + "?" + q.Encode()
	var raw recordsResponse[melbourneCount]
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil, err
	}
	return raw.Results, nil
}

func melbourneEvents(locations map[int]melbourneLocation, counts []melbourneCount) []events.Event {
	out := make([]events.Event, 0, len(counts))
	for _, count := range counts {
		loc, ok := locations[count.LocationID]
		if !ok {
			continue
		}
		ts := parseTime(count.SensingDatetime)
		if ts.IsZero() {
			continue
		}
		props := map[string]any{
			"source_provider":       "City of Melbourne Open Data",
			"feed":                  "pedestrian-counting-system-past-hour-counts-per-minute",
			"location_id":           count.LocationID,
			"sensor_description":    loc.SensorDescription,
			"sensor_name":           loc.SensorName,
			"location_type":         loc.LocationType,
			"sensor_status":         loc.Status,
			"sensing_datetime":      count.SensingDatetime,
			"sensing_date":          count.SensingDate,
			"sensing_time":          count.SensingTime,
			"direction_1_label":     loc.Direction1,
			"direction_2_label":     loc.Direction2,
			"direction_1_count":     count.Direction1,
			"direction_2_count":     count.Direction2,
			"pedestrian_count":      count.TotalOfDirections,
			"density_signal":        densitySignal(count.TotalOfDirections),
			"source_api_endpoint":   melbournePastHourURL,
			"location_api_endpoint": melbourneLocationsURL,
			"privacy_model":         "aggregate_sensor_count_no_identifiers",
			"source_payload_validity": map[string]any{
				"valid_start":    ts.Format(time.RFC3339),
				"valid_end":      ts.Add(15 * time.Minute).Format(time.RFC3339),
				"validity_basis": "pedestrian_sensor_minute_count",
			},
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  fmt.Sprintf("melbourne:%d:%s", count.LocationID, ts.Format("20060102T150405")),
			Lat:    loc.Latitude,
			Lon:    loc.Longitude,
			Props:  props,
		})
	}
	return out
}

func densitySignal(count int) string {
	switch {
	case count >= 250:
		return "very_high"
	case count >= 100:
		return "high"
	case count >= 25:
		return "moderate"
	default:
		return "low"
	}
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z07:00"} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func envInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return def
	}
	if n > max {
		return max
	}
	return n
}
