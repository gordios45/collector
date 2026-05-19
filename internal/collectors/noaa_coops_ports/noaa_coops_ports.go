// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package noaa_coops_ports ingests NOAA CO-OPS/PORTS latest port-condition
// observations for strategically useful tide stations.
package noaa_coops_ports

import (
	"context"
	"encoding/json"
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
	sourceID = "noaa_coops_ports"
	apiBase  = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
)

type Collector struct {
	stations []station
	products []string
}

type station struct {
	ID   string
	Name string
	Lat  float64
	Lon  float64
}

var defaultStations = []station{
	{ID: "8518750", Name: "The Battery, NY", Lat: 40.7006, Lon: -74.0142},
	{ID: "8638863", Name: "Chesapeake Bay Bridge Tunnel, VA", Lat: 36.9667, Lon: -76.1133},
	{ID: "8665530", Name: "Charleston, SC", Lat: 32.7808, Lon: -79.9236},
	{ID: "8726520", Name: "St. Petersburg, FL", Lat: 27.7606, Lon: -82.6269},
	{ID: "8771450", Name: "Galveston Pier 21, TX", Lat: 29.31, Lon: -94.7933},
	{ID: "9410660", Name: "Los Angeles, CA", Lat: 33.72, Lon: -118.272},
	{ID: "9414290", Name: "San Francisco, CA", Lat: 37.8063, Lon: -122.4659},
	{ID: "9447130", Name: "Seattle, WA", Lat: 47.6026, Lon: -122.3393},
}

func New() (*Collector, error) {
	stations := parseStations(os.Getenv("NOAA_COOPS_STATIONS"))
	if len(stations) == 0 {
		stations = defaultStations
	}
	products := splitCSV(os.Getenv("NOAA_COOPS_PRODUCTS"))
	if len(products) == 0 {
		products = []string{"water_level", "air_pressure", "air_temperature", "water_temperature", "wind"}
	}
	return &Collector{stations: stations, products: products}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("NOAA_COOPS_POLL_EVERY_S", 600, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := make([]events.Event, 0, len(c.stations))
	var firstErr error
	for _, st := range c.stations {
		ev, ok, err := c.stationEvent(ctx, st)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (c *Collector) stationEvent(ctx context.Context, st station) (events.Event, bool, error) {
	props := map[string]any{
		"source_provider":     "NOAA CO-OPS",
		"station_id":          st.ID,
		"station_name":        st.Name,
		"source_api_endpoint": apiBase,
		"products_requested":  c.products,
	}
	productTimes := map[string]string{}
	var latest time.Time
	var firstErr error
	productCount := 0

	for _, product := range c.products {
		obs, endpoint, err := fetchProduct(ctx, st.ID, product)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		productCount++
		productTimes[product] = obs.TS.Format(time.RFC3339)
		props[product+"_endpoint"] = endpoint
		applyProductProps(props, product, obs.Values)
		if latest.IsZero() || obs.TS.After(latest) {
			latest = obs.TS
		}
	}
	if productCount == 0 {
		return events.Event{}, false, firstErr
	}
	if latest.IsZero() {
		latest = time.Now().UTC()
	}
	props["products_observed"] = productCount
	props["product_observed_at"] = productTimes
	props["source_payload_validity"] = map[string]any{
		"valid_start":    latest.Format(time.RFC3339),
		"valid_end":      latest.Add(20 * time.Minute).Format(time.RFC3339),
		"validity_basis": "coops_latest_observation",
	}

	return events.Event{
		Ts:     latest,
		Source: sourceID,
		ExtID:  st.ID + ":" + latest.Format("20060102T1504"),
		Lat:    st.Lat,
		Lon:    st.Lon,
		Props:  props,
	}, true, nil
}

type productObservation struct {
	TS     time.Time
	Values map[string]string
}

type coopsResponse struct {
	Metadata struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Lat  string `json:"lat"`
		Lon  string `json:"lon"`
	} `json:"metadata"`
	Data  []map[string]string `json:"data"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func fetchProduct(ctx context.Context, stationID, product string) (productObservation, string, error) {
	q := url.Values{}
	q.Set("date", "latest")
	q.Set("station", stationID)
	q.Set("product", product)
	q.Set("time_zone", "gmt")
	q.Set("units", "metric")
	q.Set("format", "json")
	if product == "water_level" {
		q.Set("datum", "MLLW")
	}
	endpoint := apiBase + "?" + q.Encode()
	buf, err := httpx.GetBytes(ctx, endpoint, map[string]string{"Accept": "application/json"})
	if err != nil {
		return productObservation{}, endpoint, err
	}
	var raw coopsResponse
	if err := json.Unmarshal(buf, &raw); err != nil {
		return productObservation{}, endpoint, err
	}
	if raw.Error.Message != "" {
		return productObservation{}, endpoint, fmt.Errorf("coops %s/%s: %s", stationID, product, raw.Error.Message)
	}
	if len(raw.Data) == 0 {
		return productObservation{}, endpoint, fmt.Errorf("coops %s/%s: no data", stationID, product)
	}
	row := raw.Data[0]
	ts, err := time.ParseInLocation("2006-01-02 15:04", strings.TrimSpace(row["t"]), time.UTC)
	if err != nil {
		return productObservation{}, endpoint, fmt.Errorf("coops %s/%s time: %w", stationID, product, err)
	}
	return productObservation{TS: ts.UTC(), Values: row}, endpoint, nil
}

func applyProductProps(props map[string]any, product string, row map[string]string) {
	switch product {
	case "water_level":
		setFloat(props, "water_level_m", row["v"])
		setFloat(props, "water_level_sigma_m", row["s"])
		props["water_level_quality"] = row["q"]
		props["water_level_flags"] = row["f"]
	case "air_pressure":
		setFloat(props, "air_pressure_hpa", row["v"])
		props["air_pressure_flags"] = row["f"]
	case "air_temperature":
		setFloat(props, "air_temperature_c", row["v"])
		props["air_temperature_flags"] = row["f"]
	case "water_temperature":
		setFloat(props, "water_temperature_c", row["v"])
		props["water_temperature_flags"] = row["f"]
	case "wind":
		setFloat(props, "wind_speed_m_s", row["s"])
		setFloat(props, "wind_direction_deg", row["d"])
		setFloat(props, "wind_gust_m_s", row["g"])
		props["wind_direction_cardinal"] = row["dr"]
		props["wind_flags"] = row["f"]
	default:
		for k, v := range row {
			if k == "t" {
				continue
			}
			props[product+"_"+k] = strings.TrimSpace(v)
		}
	}
}

func setFloat(props map[string]any, key, raw string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return
	}
	props[key] = v
}

func parseStations(raw string) []station {
	out := []station{}
	for _, part := range strings.Split(raw, ",") {
		fields := strings.Split(part, "|")
		if len(fields) != 4 {
			continue
		}
		lat, errLat := strconv.ParseFloat(strings.TrimSpace(fields[2]), 64)
		lon, errLon := strconv.ParseFloat(strings.TrimSpace(fields[3]), 64)
		if errLat != nil || errLon != nil || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
			continue
		}
		id := strings.TrimSpace(fields[0])
		name := strings.TrimSpace(fields[1])
		if id == "" || name == "" {
			continue
		}
		out = append(out, station{ID: id, Name: name, Lat: lat, Lon: lon})
	}
	return out
}

func splitCSV(raw string) []string {
	var out []string
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		v := strings.ToLower(strings.TrimSpace(part))
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
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
