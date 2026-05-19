// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package noaa_coastal_forecast monitors NOAA NOS/STOFS coastal forecast
// product availability on NOMADS. It deliberately records product metadata
// rather than downloading model grids.
package noaa_coastal_forecast

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	sourceID       = "noaa_coastal_forecast"
	nosofsIndexURL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nosofs/prod/"
	stofsIndexURL  = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/stofs/prod/"
)

type Collector struct{}

func New() (*Collector, error) { return &Collector{}, nil }

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("NOAA_COASTAL_FORECAST_POLL_EVERY_S", 3*3600, 600, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	var firstErr error
	for _, idx := range []string{nosofsIndexURL, stofsIndexURL} {
		buf, err := httpx.GetBytes(ctx, idx, map[string]string{"Accept": "text/html,*/*"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, eventsFromIndex(idx, string(buf))...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type productDir struct {
	Model string
	Date  string
	URL   string
}

var hrefRe = regexp.MustCompile(`href="([a-z0-9_]+)\.([0-9]{8})/"`)

func eventsFromIndex(indexURL, html string) []events.Event {
	latest := latestProductDirs(indexURL, html)
	out := make([]events.Event, 0, len(latest))
	for _, p := range latest {
		def := modelDef(p.Model)
		ts, _ := time.Parse("20060102", p.Date)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		props := map[string]any{
			"source_provider":     "NOAA NOMADS",
			"model":               p.Model,
			"model_name":          def.Name,
			"region":              def.Region,
			"product_date":        p.Date,
			"product_url":         p.URL,
			"source_api_endpoint": indexURL,
			"integration_state":   "product_catalog_available",
			"forecast_products":   []string{"water_level", "currents", "temperature", "salinity", "winds"},
			"download_policy":     "catalog_only_no_grid_download",
		}
		out = append(out, events.Event{
			Ts:     ts.UTC(),
			Source: sourceID,
			ExtID:  p.Model + ":" + p.Date,
			Lat:    def.Lat,
			Lon:    def.Lon,
			Props:  props,
		})
	}
	return out
}

func latestProductDirs(indexURL, html string) []productDir {
	byModel := map[string]productDir{}
	for _, match := range hrefRe.FindAllStringSubmatch(html, -1) {
		model := strings.ToLower(match[1])
		date := match[2]
		cur, ok := byModel[model]
		if !ok || date > cur.Date {
			byModel[model] = productDir{
				Model: model,
				Date:  date,
				URL:   strings.TrimRight(indexURL, "/") + "/" + model + "." + date + "/",
			}
		}
	}
	out := make([]productDir, 0, len(byModel))
	for _, p := range byModel {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

type modelInfo struct {
	Name   string
	Region string
	Lat    float64
	Lon    float64
}

func modelDef(model string) modelInfo {
	defs := map[string]modelInfo{
		"cbofs":        {Name: "Chesapeake Bay Operational Forecast System", Region: "Chesapeake Bay", Lat: 38.3, Lon: -76.4},
		"ciofs":        {Name: "Cook Inlet Operational Forecast System", Region: "Cook Inlet", Lat: 60.5, Lon: -151.5},
		"dbofs":        {Name: "Delaware Bay Operational Forecast System", Region: "Delaware Bay", Lat: 39.1, Lon: -75.1},
		"gomofs":       {Name: "Gulf of Maine Operational Forecast System", Region: "Gulf of Maine", Lat: 43.2, Lon: -68.5},
		"ngofs2":       {Name: "Northern Gulf of Mexico Operational Forecast System", Region: "Northern Gulf of Mexico", Lat: 29.2, Lon: -90.2},
		"sfbofs":       {Name: "San Francisco Bay Operational Forecast System", Region: "San Francisco Bay", Lat: 37.8, Lon: -122.4},
		"tbofs":        {Name: "Tampa Bay Operational Forecast System", Region: "Tampa Bay", Lat: 27.8, Lon: -82.6},
		"stofs_2d_glo": {Name: "Surge and Tide Operational Forecast System 2D Global", Region: "global coast", Lat: 0, Lon: 0},
		"stofs_3d_atl": {Name: "Surge and Tide Operational Forecast System 3D Atlantic", Region: "Atlantic coast", Lat: 31, Lon: -60},
		"wcofs":        {Name: "West Coast Operational Forecast System", Region: "U.S. West Coast", Lat: 37, Lon: -123},
	}
	if d, ok := defs[model]; ok {
		return d
	}
	return modelInfo{Name: fmt.Sprintf("NOAA %s coastal forecast", strings.ToUpper(model)), Region: "coastal forecast domain"}
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
