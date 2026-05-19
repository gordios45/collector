// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package cams_atmosphere samples CAMS-backed atmospheric composition fields
// through Open-Meteo's free air-quality API over active AOIs.
package cams_atmosphere

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceName = "cams_atmosphere"
	endpoint   = "https://air-quality-api.open-meteo.com/v1/air-quality"
	docsURL    = "https://open-meteo.com/en/docs/air-quality-api"
)

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_CAMS_ATMOSPHERE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_CAMS_ATMOSPHERE=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:    pool,
		maxAOIs: collectorutil.EnvInt("CAMS_ATMOSPHERE_MAX_AOIS", 16, 1, 60),
	}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 7*24*time.Hour, collectorutil.StrategicAOIs)
	out := []events.Event{}
	var firstErr error
	for _, aoi := range aois {
		ev, ok, err := fetchAOI(ctx, aoi)
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

type payload struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Hourly    struct {
		Time                []string  `json:"time"`
		CarbonMonoxide      []float64 `json:"carbon_monoxide"`
		NitrogenDioxide     []float64 `json:"nitrogen_dioxide"`
		SulphurDioxide      []float64 `json:"sulphur_dioxide"`
		PM10                []float64 `json:"pm10"`
		PM25                []float64 `json:"pm2_5"`
		AerosolOpticalDepth []float64 `json:"aerosol_optical_depth"`
		Dust                []float64 `json:"dust"`
	} `json:"hourly"`
	HourlyUnits map[string]string `json:"hourly_units"`
}

func fetchAOI(ctx context.Context, aoi collectorutil.AOI) (events.Event, bool, error) {
	q := url.Values{}
	q.Set("latitude", strconv.FormatFloat(aoi.Lat, 'f', 5, 64))
	q.Set("longitude", strconv.FormatFloat(aoi.Lon, 'f', 5, 64))
	q.Set("hourly", "carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,pm10,pm2_5,aerosol_optical_depth,dust")
	q.Set("past_hours", "24")
	q.Set("forecast_hours", "1")
	q.Set("timezone", "UTC")
	var p payload
	if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &p); err != nil {
		return events.Event{}, false, err
	}
	m, ok := summarize(p)
	if !ok {
		return events.Event{}, false, nil
	}
	props := map[string]any{
		"watch_aoi_id":               aoi.ID,
		"watch_aoi_kind":             aoi.Kind,
		"watch_aoi_label":            aoi.Label,
		"observed_at":                m.TS.Format(time.RFC3339),
		"carbon_monoxide_ugm3":       collectorutil.Round(m.CO, 1),
		"nitrogen_dioxide_ugm3":      collectorutil.Round(m.NO2, 1),
		"sulphur_dioxide_ugm3":       collectorutil.Round(m.SO2, 1),
		"pm10_ugm3":                  collectorutil.Round(m.PM10, 1),
		"pm25_ugm3":                  collectorutil.Round(m.PM25, 1),
		"aerosol_optical_depth":      collectorutil.Round(m.AOD, 3),
		"dust_ugm3":                  collectorutil.Round(m.Dust, 1),
		"co_score":                   collectorutil.Round(m.COScore, 2),
		"no2_score":                  collectorutil.Round(m.NO2Score, 2),
		"so2_score":                  collectorutil.Round(m.SO2Score, 2),
		"pm25_score":                 collectorutil.Round(m.PM25Score, 2),
		"aerosol_score":              collectorutil.Round(m.AerosolScore, 2),
		"dust_score":                 collectorutil.Round(m.DustScore, 2),
		"air_quality_pressure_score": collectorutil.Round(m.Score, 2),
		"dominant_pollutant":         m.Dominant,
		"source_api_endpoint":        endpoint,
		"docs_url":                   docsURL,
		"upstream_model":             "CAMS European Air Quality Forecast / CAMS Global Atmospheric Composition Forecasts via Open-Meteo",
	}
	return events.Event{
		Ts:     m.TS,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s", collectorutil.StableID(aoi.ID), m.TS.Format("20060102T15")),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Props:  props,
	}, true, nil
}

type metric struct {
	TS           time.Time
	CO           float64
	NO2          float64
	SO2          float64
	PM10         float64
	PM25         float64
	AOD          float64
	Dust         float64
	COScore      float64
	NO2Score     float64
	SO2Score     float64
	PM25Score    float64
	AerosolScore float64
	DustScore    float64
	Score        float64
	Dominant     string
}

func summarize(p payload) (metric, bool) {
	n := len(p.Hourly.Time)
	if n == 0 {
		return metric{}, false
	}
	i := n - 1
	for ; i >= 0; i-- {
		ts := parseHour(p.Hourly.Time[i])
		if !ts.IsZero() && !ts.After(time.Now().UTC().Add(90*time.Minute)) {
			break
		}
	}
	if i < 0 {
		i = n - 1
	}
	m := metric{
		TS:   parseHour(p.Hourly.Time[i]),
		CO:   at(p.Hourly.CarbonMonoxide, i),
		NO2:  at(p.Hourly.NitrogenDioxide, i),
		SO2:  at(p.Hourly.SulphurDioxide, i),
		PM10: at(p.Hourly.PM10, i),
		PM25: at(p.Hourly.PM25, i),
		AOD:  at(p.Hourly.AerosolOpticalDepth, i),
		Dust: at(p.Hourly.Dust, i),
	}
	if m.TS.IsZero() {
		m.TS = time.Now().UTC().Truncate(time.Hour)
	}
	m.COScore = propx.ClampFloat((m.CO-500)/500, 0, 3)
	m.NO2Score = propx.ClampFloat((m.NO2-40)/40, 0, 3)
	m.SO2Score = propx.ClampFloat((m.SO2-20)/30, 0, 3)
	m.PM25Score = propx.ClampFloat((m.PM25-35)/40, 0, 3)
	m.AerosolScore = propx.ClampFloat((m.AOD-0.35)/0.35, 0, 3)
	m.DustScore = propx.ClampFloat((m.Dust-50)/100, 0, 3)
	m.Score = math.Max(math.Max(m.COScore, m.NO2Score), math.Max(math.Max(m.SO2Score, m.PM25Score), math.Max(m.AerosolScore, m.DustScore)))
	m.Dominant = dominant(m)
	return m, true
}

func dominant(m metric) string {
	best := m.Score
	switch best {
	case m.COScore:
		return "carbon_monoxide"
	case m.NO2Score:
		return "nitrogen_dioxide"
	case m.SO2Score:
		return "sulphur_dioxide"
	case m.PM25Score:
		return "pm25"
	case m.AerosolScore:
		return "aerosol_optical_depth"
	case m.DustScore:
		return "dust"
	default:
		return "none"
	}
}

func at(xs []float64, i int) float64 {
	if i < 0 || i >= len(xs) {
		return 0
	}
	return xs[i]
}

func parseHour(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{"2006-01-02T15:04", time.RFC3339, time.RFC3339Nano} {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
