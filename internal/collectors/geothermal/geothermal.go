// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package geothermal ingests fast geostationary active-fire detections exposed
// through NASA FIRMS. It is intentionally gated on FIRMS_MAP_KEY because the
// geostationary products are available through the FIRMS API rather than the
// keyless active_fire CSV mirrors.
package geothermal

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const defaultSensor = "GOES_NRT"

type Collector struct {
	key    string
	sensor string
	area   string
}

func New() (*Collector, error) {
	key := strings.TrimSpace(os.Getenv("FIRMS_MAP_KEY"))
	if key == "" {
		return nil, errors.New("FIRMS_MAP_KEY not set")
	}
	sensor := strings.TrimSpace(os.Getenv("FIRMS_GEO_THERMAL_SENSOR"))
	if sensor == "" {
		sensor = defaultSensor
	}
	area := strings.TrimSpace(os.Getenv("FIRMS_GEO_THERMAL_AREA"))
	if area == "" {
		area = "world"
	}
	return &Collector{key: key, sensor: sensor, area: area}, nil
}

func (c *Collector) ID() string { return "geo_thermal" }

func (c *Collector) PollEvery() time.Duration {
	if s := strings.TrimSpace(os.Getenv("FIRMS_GEO_THERMAL_POLL_EVERY_S")); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 300 {
			return time.Duration(n) * time.Second
		}
	}
	return 10 * time.Minute
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	u := fmt.Sprintf("https://firms.modaps.eosdis.nasa.gov/api/area/csv/%s/%s/%s/1",
		url.PathEscape(c.key), url.PathEscape(c.sensor), url.PathEscape(c.area))
	buf, err := httpx.GetBytes(ctx, u, nil)
	if err != nil {
		return nil, err
	}
	return parseCSV(buf, c.sensor, c.area, redactedEndpoint(c.sensor, c.area), time.Now().UTC())
}

func parseCSV(buf []byte, sensor, area, endpoint string, fallbackTS time.Time) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	hdr, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := make(map[string]int, len(hdr))
	for i, h := range hdr {
		idx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, k := range []string{"latitude", "longitude", "acq_date", "acq_time"} {
		if _, ok := idx[k]; !ok {
			return nil, errors.New("geo_thermal: missing column " + k)
		}
	}

	out := make([]events.Event, 0, 256)
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		lat, _ := strconv.ParseFloat(row[idx["latitude"]], 64)
		lon, _ := strconv.ParseFloat(row[idx["longitude"]], 64)
		if lat == 0 && lon == 0 {
			continue
		}
		tsStr := row[idx["acq_date"]] + " " + row[idx["acq_time"]]
		ts, err := time.Parse("2006-01-02 1504", tsStr)
		if err != nil {
			ts = fallbackTS
		}
		props := map[string]any{
			"data_id":             sensor,
			"area":                area,
			"source_api_endpoint": endpoint,
			"source_kind":         "firms_geostationary_active_fire",
		}
		for k, i := range idx {
			if i < len(row) {
				props[k] = row[i]
			}
		}
		addThermalScores(props, idx, row)
		ext := sensor + "_" + tsStr + "_" + row[idx["latitude"]] + "_" + row[idx["longitude"]]
		out = append(out, events.Event{
			Ts:     ts.UTC(),
			Source: "geo_thermal",
			ExtID:  ext,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out, nil
}

func addThermalScores(props map[string]any, idx map[string]int, row []string) {
	if frp, ok := columnFloat(idx, row, "frp", "power", "fire_power"); ok {
		props["frp_score"] = collectorutil.GeoThermalFRPScore(frp)
	}
	if brightness, ok := columnFloat(idx, row, "bright_ti4", "brightness", "temperature", "temp"); ok {
		props["brightness_score"] = collectorutil.ThermalBrightnessScore(brightness, 18)
	}
}

func columnFloat(idx map[string]int, row []string, keys ...string) (float64, bool) {
	for _, key := range keys {
		i, ok := idx[key]
		if !ok || i >= len(row) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row[i]), 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func redactedEndpoint(sensor, area string) string {
	return fmt.Sprintf("https://firms.modaps.eosdis.nasa.gov/api/area/csv/[FIRMS_MAP_KEY]/%s/%s/1", sensor, area)
}
