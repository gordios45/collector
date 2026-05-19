// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package eurdep wires the European Commission EURDEP radiation-monitoring
// layer. The public map exists without login, but deployments should set
// EURDEP_GEOJSON_URL or EURDEP_ARCGIS_QUERY_URL to a permitted machine-readable
// service endpoint before using it as measurement evidence.
package eurdep

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceName = "eurdep_radiation"
	docsURL    = "https://remon.jrc.ec.europa.eu/About/Rad-Data-Exchange"
	mapURL     = "https://remap.jrc.ec.europa.eu/"
)

type Collector struct {
	endpoint string
	maxRows  int
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_EURDEP") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_EURDEP=1")
	}
	endpoint := collectorutil.FirstEnv("EURDEP_GEOJSON_URL", "EURDEP_ARCGIS_QUERY_URL")
	return &Collector{
		endpoint: endpoint,
		maxRows:  collectorutil.EnvInt("EURDEP_MAX_ROWS", 80, 1, 500),
	}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 60 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	if strings.TrimSpace(c.endpoint) == "" {
		now := time.Now().UTC().Truncate(time.Hour)
		return []events.Event{{
			Ts:     now,
			Source: sourceName,
			ExtID:  "integration_state:" + now.Format("2006010215"),
			Lat:    50.8503,
			Lon:    4.3517,
			Props: map[string]any{
				"station_id":            "integration_state",
				"station_name":          "EURDEP public endpoint not configured",
				"country":               "EU",
				"value":                 0,
				"unit":                  "nSv/h",
				"observed_at":           now.Format(time.RFC3339),
				"radiation_value_score": 0,
				"high_radiation_flag":   0,
				"integration_state":     "set EURDEP_GEOJSON_URL or EURDEP_ARCGIS_QUERY_URL to enable live station sampling",
				"source_api_endpoint":   mapURL,
				"docs_url":              docsURL,
			},
		}}, nil
	}
	var fc featureCollection
	if err := httpx.GetJSON(ctx, c.endpoint, map[string]string{"Accept": "application/geo+json,application/json"}, &fc); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := []events.Event{}
	for _, f := range fc.Features {
		if len(out) >= c.maxRows {
			break
		}
		ev, ok := eventFromFeature(f, now, c.endpoint)
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("EURDEP endpoint returned no station features")
	}
	return out, nil
}

type featureCollection struct {
	Type     string    `json:"type"`
	Features []feature `json:"features"`
}

type feature struct {
	ID         any            `json:"id"`
	Geometry   geometry       `json:"geometry"`
	Properties map[string]any `json:"properties"`
	Attributes map[string]any `json:"attributes"`
}

type geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
	X           float64   `json:"x"`
	Y           float64   `json:"y"`
}

func eventFromFeature(f feature, fallbackTS time.Time, endpoint string) (events.Event, bool) {
	props := f.Properties
	if len(props) == 0 {
		props = f.Attributes
	}
	if len(props) == 0 {
		return events.Event{}, false
	}
	lat, lon := geometryPoint(f.Geometry)
	if !collectorutil.ValidLatLon(lat, lon) {
		lat, lon = floatsFromProps(props)
	}
	if !collectorutil.ValidLatLon(lat, lon) {
		return events.Event{}, false
	}
	value, ok := firstFloat(props, "value", "avg", "average", "gdr", "gamma", "dose_rate", "doseRate", "gamma_nsv_h", "GammaDoseRate", "GDR")
	if !ok {
		return events.Event{}, false
	}
	unit := firstString(props, "unit", "units", "Unit")
	if unit == "" {
		unit = "nSv/h"
	}
	ts := parseTime(firstString(props, "observed_at", "updated_at", "timestamp", "time", "datetime", "DateTime", "MeasurementTime"))
	if ts.IsZero() {
		ts = fallbackTS
	}
	stationID := firstString(props, "station_id", "stationId", "id", "ID", "code", "StationId")
	if stationID == "" && f.ID != nil {
		stationID = fmt.Sprint(f.ID)
	}
	if stationID == "" {
		stationID = collectorutil.StableID(fmt.Sprintf("%.5f:%.5f", lat, lon))
	}
	score := radiationScore(value, unit)
	high := highRadiationFlag(value, unit)
	outProps := map[string]any{
		"station_id":            stationID,
		"station_name":          firstString(props, "station_name", "name", "Name", "StationName"),
		"country":               firstString(props, "country", "Country"),
		"value":                 collectorutil.Round(value, 3),
		"unit":                  unit,
		"observed_at":           ts.Format(time.RFC3339),
		"radiation_value_score": collectorutil.Round(score, 2),
		"high_radiation_flag":   high,
		"source_api_endpoint":   endpoint,
		"docs_url":              docsURL,
		"source_terms":          "EURDEP/JRC public map data; verify provider reuse terms before redistribution",
	}
	return events.Event{
		Ts:     ts,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s", stationID, ts.Format("20060102T150405")),
		Lat:    lat,
		Lon:    lon,
		Props:  outProps,
	}, true
}

func geometryPoint(g geometry) (float64, float64) {
	if len(g.Coordinates) >= 2 {
		return g.Coordinates[1], g.Coordinates[0]
	}
	return g.Y, g.X
}

func floatsFromProps(p map[string]any) (float64, float64) {
	lat, _ := firstFloat(p, "lat", "latitude", "Latitude", "y", "Y")
	lon, _ := firstFloat(p, "lon", "lng", "longitude", "Longitude", "x", "X")
	return lat, lon
}

func firstFloat(p map[string]any, keys ...string) (float64, bool) {
	for _, k := range keys {
		if v, ok := propx.Float(p[k]); ok {
			return v, true
		}
	}
	return 0, false
}

func firstString(p map[string]any, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(propx.StringAt(p, k)); v != "" {
			return v
		}
	}
	return ""
}

func radiationScore(v float64, unit string) float64 {
	unit = strings.ToLower(strings.TrimSpace(unit))
	if strings.Contains(unit, "usv") || strings.Contains(unit, "µsv") || strings.Contains(unit, "μsv") {
		return clamp((v-0.20)/0.10, 0, 4)
	}
	return clamp((v-200)/100, 0, 4)
}

func highRadiationFlag(v float64, unit string) float64 {
	unit = strings.ToLower(strings.TrimSpace(unit))
	if strings.Contains(unit, "usv") || strings.Contains(unit, "µsv") || strings.Contains(unit, "μsv") {
		if v >= 1.0 {
			return 2
		}
		if v >= 0.5 {
			return 1
		}
		return 0
	}
	if v >= 500 {
		return 2
	}
	if v >= 300 {
		return 1
	}
	return 0
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

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
