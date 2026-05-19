// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package ndbc_buoys ingests NOAA NDBC real-time buoy observations near
// chokepoints and storm-prone coastal approaches.
package ndbc_buoys

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const realtimeBase = "https://www.ndbc.noaa.gov/data/realtime2"

type station struct {
	ID   string
	Name string
	Lat  float64
	Lon  float64
}

var stationCatalog = map[string]station{
	"41002": {ID: "41002", Name: "South Hatteras", Lat: 31.862, Lon: -74.835},
	"41010": {ID: "41010", Name: "Canaveral East", Lat: 28.878, Lon: -78.467},
	"42001": {ID: "42001", Name: "Mid Gulf", Lat: 25.888, Lon: -89.658},
	"42040": {ID: "42040", Name: "Luke Offshore Test Platform", Lat: 29.208, Lon: -88.207},
	"44017": {ID: "44017", Name: "Montauk Point", Lat: 40.693, Lon: -72.048},
	"44065": {ID: "44065", Name: "New York Harbor Entrance", Lat: 40.369, Lon: -73.703},
	"46002": {ID: "46002", Name: "West Oregon", Lat: 42.617, Lon: -130.517},
	"46026": {ID: "46026", Name: "San Francisco", Lat: 37.755, Lon: -122.839},
	"51001": {ID: "51001", Name: "Northwest Hawaii", Lat: 24.451, Lon: -162.008},
}

type Collector struct {
	stations []station
}

func New() (*Collector, error) {
	stations := configuredStations()
	if len(stations) == 0 {
		return nil, fmt.Errorf("no NDBC stations configured")
	}
	return &Collector{stations: stations}, nil
}

func (c *Collector) ID() string               { return "ndbc_buoys" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, s := range c.stations {
		u := fmt.Sprintf("%s/%s.txt", realtimeBase, s.ID)
		buf, err := httpx.GetBytes(ctx, u, map[string]string{"Accept": "text/plain,*/*"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ev, ok := eventFromText(s, u, string(buf)); ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type buoyObservation struct {
	TS          time.Time
	WindDirDeg  float64
	WindMS      float64
	GustMS      float64
	WaveHeightM float64
	PressureHPA float64
}

func eventFromText(s station, sourceURL, raw string) (events.Event, bool) {
	obs, ok := parseLatest(raw)
	if !ok {
		return events.Event{}, false
	}
	props := map[string]any{
		"station_id":              s.ID,
		"station_name":            s.Name,
		"observed_at":             obs.TS.Format(time.RFC3339),
		"wind_direction_deg":      round(obs.WindDirDeg, 1),
		"wind_speed_m_s":          round(obs.WindMS, 1),
		"gust_m_s":                round(obs.GustMS, 1),
		"wave_height_m":           round(obs.WaveHeightM, 1),
		"pressure_hpa":            round(obs.PressureHPA, 1),
		"wind_gust_score":         round(gustScore(obs.GustMS), 2),
		"wave_height_score":       round(waveScore(obs.WaveHeightM), 2),
		"low_pressure_score":      round(lowPressureScore(obs.PressureHPA), 2),
		"source_api_endpoint":     sourceURL,
		"source_payload_validity": validity(obs.TS, 20*time.Minute, "ndbc_realtime_observation"),
	}
	return events.Event{
		Ts:     obs.TS,
		Source: "ndbc_buoys",
		ExtID:  fmt.Sprintf("%s:%s", s.ID, obs.TS.Format("20060102T1504")),
		Lat:    s.Lat,
		Lon:    s.Lon,
		Props:  props,
	}, true
}

func parseLatest(raw string) (buoyObservation, bool) {
	lines := strings.Split(raw, "\n")
	fields := map[string]int{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		cols := strings.Fields(line)
		if strings.HasPrefix(line, "#YY") {
			for i, col := range cols {
				fields[strings.TrimPrefix(col, "#")] = i
			}
			continue
		}
		if strings.HasPrefix(line, "#yr") {
			continue
		}
		if len(fields) == 0 || len(cols) < 5 {
			continue
		}
		ts, ok := parseNDBCTime(cols, fields)
		if !ok {
			continue
		}
		return buoyObservation{
			TS:          ts,
			WindDirDeg:  value(cols, fields, "WDIR"),
			WindMS:      value(cols, fields, "WSPD"),
			GustMS:      value(cols, fields, "GST"),
			WaveHeightM: value(cols, fields, "WVHT"),
			PressureHPA: value(cols, fields, "PRES"),
		}, true
	}
	return buoyObservation{}, false
}

func parseNDBCTime(cols []string, fields map[string]int) (time.Time, bool) {
	year := intValue(cols, fields, "YY")
	if year == 0 {
		year = intValue(cols, fields, "yr")
	}
	month := intValue(cols, fields, "MM")
	if month == 0 {
		month = intValue(cols, fields, "mo")
	}
	day := intValue(cols, fields, "DD")
	if day == 0 {
		day = intValue(cols, fields, "dy")
	}
	hour := intValue(cols, fields, "hh")
	minute := intValue(cols, fields, "mm")
	if year < 100 {
		year += 2000
	}
	if year < 2000 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC), true
}

func intValue(cols []string, fields map[string]int, key string) int {
	idx, ok := fields[key]
	if !ok || idx < 0 || idx >= len(cols) {
		return 0
	}
	v, err := strconv.Atoi(cols[idx])
	if err != nil {
		return 0
	}
	return v
}

func value(cols []string, fields map[string]int, key string) float64 {
	idx, ok := fields[key]
	if !ok || idx < 0 || idx >= len(cols) {
		return 0
	}
	raw := strings.TrimSpace(cols[idx])
	if raw == "MM" || raw == "" {
		return 0
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func gustScore(gust float64) float64 {
	return clamp((gust-17.0)/8.0, 0, 4)
}

func waveScore(wave float64) float64 {
	return clamp((wave-3.0)/2.0, 0, 4)
}

func lowPressureScore(pressure float64) float64 {
	if pressure <= 0 {
		return 0
	}
	return clamp((1000.0-pressure)/10.0, 0, 4)
}

func configuredStations() []station {
	raw := strings.TrimSpace(os.Getenv("NDBC_STATIONS"))
	if raw == "" {
		raw = "41002,41010,42001,42040,44017,44065,46002,46026,51001"
	}
	out := []station{}
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.ToUpper(strings.TrimSpace(part))
		if id == "" || seen[id] {
			continue
		}
		s, ok := stationCatalog[id]
		if !ok {
			continue
		}
		seen[id] = true
		out = append(out, s)
	}
	return out
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}
