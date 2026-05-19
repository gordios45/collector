// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package saveecobot ingests keyless city-level Ukrainian gamma readings from
// SaveEcoBot. A keyed all-stations endpoint can be added through the same
// source later without changing the downstream feature namespace.
package saveecobot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	baseURL      = "https://www.saveecobot.com/maps"
	defaultSlugs = "dnipro,kyiv,lviv,zhytomyr,ternopil,vinnytsia,uzhhorod,poltava,cherkasy"
	sourceName   = "saveecobot_radiation"
	docsURL      = "https://docs.saveecobot.com/docs/ukraine-radiation-api-ukrainian"
)

type Collector struct {
	slugs []string
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_SAVE_ECOBOT") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_SAVE_ECOBOT=1")
	}
	slugs := collectorutil.SplitCSV(os.Getenv("SAVEECOBOT_CITY_SLUGS"))
	if len(slugs) == 0 {
		slugs = collectorutil.SplitCSV(defaultSlugs)
	}
	return &Collector{slugs: slugs}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, slug := range c.slugs {
		ev, ok, err := fetchCity(ctx, slug)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
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

type cityPayload struct {
	ID              int    `json:"id"`
	CityName        string `json:"city_name"`
	HromadaName     string `json:"hromada_name"`
	RegionName      string `json:"region_name"`
	CenterLatitude  string `json:"center_latitude"`
	CenterLongitude string `json:"center_longitude"`
	AQI             int    `json:"aqi"`
	AQIUpdatedAt    string `json:"aqi_updated_at"`
	Meteo           struct {
		Gamma reading `json:"gamma"`
	} `json:"meteo"`
	LinkMapsGamma string `json:"link_maps_gamma"`
	Link          string `json:"link"`
}

type reading struct {
	Value     float64 `json:"value"`
	UpdatedAt string  `json:"updated_at"`
	IsOld     bool    `json:"is_old"`
}

func fetchCity(ctx context.Context, slug string) (events.Event, bool, error) {
	url := fmt.Sprintf("%s/%s.json", baseURL, strings.Trim(slug, "/ "))
	var p cityPayload
	if err := httpx.GetJSON(ctx, url, map[string]string{"Accept": "application/json"}, &p); err != nil {
		return events.Event{}, false, err
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(p.CenterLatitude), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(p.CenterLongitude), 64)
	if err1 != nil || err2 != nil || !collectorutil.ValidLatLon(lat, lon) || p.Meteo.Gamma.Value <= 0 {
		return events.Event{}, false, nil
	}
	ts := parseTime(p.Meteo.Gamma.UpdatedAt)
	if ts.IsZero() {
		ts = parseTime(p.AQIUpdatedAt)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	score := radiationScoreNSvH(p.Meteo.Gamma.Value)
	high := highRadiationFlagNSvH(p.Meteo.Gamma.Value)
	props := map[string]any{
		"city_slug":             slug,
		"city_id":               p.ID,
		"city_name":             p.CityName,
		"hromada_name":          p.HromadaName,
		"region_name":           p.RegionName,
		"country":               "Ukraine",
		"value":                 collectorutil.Round(p.Meteo.Gamma.Value, 1),
		"gamma_nsv_h":           collectorutil.Round(p.Meteo.Gamma.Value, 1),
		"unit":                  "nSv/h",
		"observed_at":           ts.Format(time.RFC3339),
		"is_old":                p.Meteo.Gamma.IsOld,
		"radiation_value_score": collectorutil.Round(score, 2),
		"high_radiation_flag":   high,
		"source_api_endpoint":   url,
		"docs_url":              docsURL,
		"source_terms":          "SaveEcoBot CC BY 4.0 attribution required",
		"source_map_url":        p.LinkMapsGamma,
		"source_city_url":       p.Link,
	}
	return events.Event{
		Ts:     ts,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s", slug, ts.Format("20060102T150405")),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true, nil
}

func radiationScoreNSvH(v float64) float64 {
	switch {
	case v >= 500:
		return 4
	case v >= 300:
		return 2.5 + (v-300)/200
	case v >= 200:
		return 1 + (v-200)/100
	default:
		return 0
	}
}

func highRadiationFlagNSvH(v float64) float64 {
	switch {
	case v >= 500:
		return 2
	case v >= 300:
		return 1
	default:
		return 0
	}
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
