// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package eia930 ingests EIA-930 hourly balancing-authority demand and
// interchange data. It is the North America analogue to ENTSO-E grid context.
package eia930

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const endpoint = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

type region struct {
	Code string
	Name string
	Lat  float64
	Lon  float64
}

var regionCatalog = map[string]region{
	"BPAT": {Code: "BPAT", Name: "Bonneville Power Administration", Lat: 45.6, Lon: -122.7},
	"CAL":  {Code: "CAL", Name: "California ISO", Lat: 37.25, Lon: -119.5},
	"CAR":  {Code: "CAR", Name: "Carolinas", Lat: 35.5, Lon: -79.0},
	"ERCO": {Code: "ERCO", Name: "ERCOT", Lat: 31.0, Lon: -99.0},
	"FPL":  {Code: "FPL", Name: "Florida Power & Light", Lat: 27.8, Lon: -81.7},
	"ISNE": {Code: "ISNE", Name: "ISO New England", Lat: 42.3, Lon: -71.8},
	"MISO": {Code: "MISO", Name: "Midcontinent ISO", Lat: 40.0, Lon: -90.0},
	"NYIS": {Code: "NYIS", Name: "New York ISO", Lat: 42.9, Lon: -75.5},
	"PJM":  {Code: "PJM", Name: "PJM", Lat: 40.0, Lon: -77.0},
	"SWPP": {Code: "SWPP", Name: "Southwest Power Pool", Lat: 38.0, Lon: -97.0},
}

type Collector struct {
	apiKey  string
	regions []region
}

func New() (*Collector, error) {
	key := strings.TrimSpace(os.Getenv("EIA_API_KEY"))
	if key == "" {
		return nil, fmt.Errorf("skipped: set EIA_API_KEY")
	}
	regions := configuredRegions()
	if len(regions) == 0 {
		return nil, fmt.Errorf("no EIA930 regions configured")
	}
	return &Collector{apiKey: key, regions: regions}, nil
}

func (c *Collector) ID() string               { return "eia930" }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, r := range c.regions {
		u := eiaURL(c.apiKey, r.Code)
		var body eiaResponse
		if err := httpx.GetJSON(ctx, u, map[string]string{"Accept": "application/json"}, &body); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, eventsFromResponse(body.rows(), r, u)...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func eiaURL(apiKey, respondent string) string {
	q := url.Values{}
	q.Set("api_key", apiKey)
	q.Set("frequency", "hourly")
	q.Add("data[]", "value")
	q.Add("facets[respondent][]", respondent)
	q.Set("sort[0][column]", "period")
	q.Set("sort[0][direction]", "desc")
	q.Set("length", "5000")
	return endpoint + "?" + q.Encode()
}

type eiaResponse struct {
	Response struct {
		Data []eiaRow `json:"data"`
	} `json:"response"`
	Data []eiaRow `json:"data"`
}

func (r eiaResponse) rows() []eiaRow {
	if len(r.Response.Data) > 0 {
		return r.Response.Data
	}
	return r.Data
}

type eiaRow struct {
	Period         string `json:"period"`
	Respondent     string `json:"respondent"`
	RespondentName string `json:"respondent-name"`
	Type           string `json:"type"`
	TypeName       string `json:"type-name"`
	Value          any    `json:"value"`
	Units          string `json:"value-units"`
}

type sample struct {
	Row    eiaRow
	TS     time.Time
	Value  float64
	Metric string
}

func eventsFromResponse(rows []eiaRow, r region, sourceURL string) []events.Event {
	byMetric := map[string][]sample{}
	for _, row := range rows {
		metric := metricKind(row.Type, row.TypeName)
		if metric == "" {
			continue
		}
		ts, ok := parsePeriod(row.Period)
		if !ok {
			continue
		}
		value, ok := propx.Float(row.Value)
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		byMetric[metric] = append(byMetric[metric], sample{Row: row, TS: ts, Value: value, Metric: metric})
	}
	out := make([]events.Event, 0, len(byMetric))
	for metric, samples := range byMetric {
		sort.Slice(samples, func(i, j int) bool { return samples[i].TS.After(samples[j].TS) })
		latest := samples[0]
		base := samples[1:]
		if len(base) > 168 {
			base = base[:168]
		}
		mean, sigma := meanStd(base)
		z := 0.0
		if sigma > 0 {
			z = (latest.Value - mean) / sigma
		}
		score := propx.ClampFloat(math.Abs(z)/1.8, 0, 3)
		props := map[string]any{
			"respondent":              propx.FirstNonEmpty(latest.Row.Respondent, r.Code),
			"respondent_name":         propx.FirstNonEmpty(latest.Row.RespondentName, r.Name),
			"metric":                  metric,
			"type":                    latest.Row.Type,
			"type_name":               latest.Row.TypeName,
			"period":                  latest.TS.Format(time.RFC3339),
			"value_mw":                round(latest.Value, 1),
			"value_units":             propx.FirstNonEmpty(latest.Row.Units, "megawatthours"),
			"baseline_mean_mw":        round(mean, 1),
			"baseline_samples":        len(base),
			"z_score":                 round(z, 2),
			"grid_anomaly_score":      round(score, 2),
			"source_api_endpoint":     sourceURL,
			"source_payload_validity": validity(latest.TS, 2*time.Hour, "eia930_hourly_period"),
		}
		switch metric {
		case "demand":
			props["demand_mw"] = round(latest.Value, 1)
			props["demand_anomaly_score"] = round(score, 2)
		case "forecast_demand":
			props["forecast_demand_mw"] = round(latest.Value, 1)
		case "interchange":
			props["interchange_mw"] = round(latest.Value, 1)
			props["interchange_anomaly_score"] = round(score, 2)
		}
		out = append(out, events.Event{
			Ts:     latest.TS,
			Source: "eia930",
			ExtID:  stableID(r.Code + ":" + metric + ":" + latest.TS.Format(time.RFC3339)),
			Lat:    r.Lat,
			Lon:    r.Lon,
			Props:  props,
		})
	}
	return out
}

func metricKind(code, name string) string {
	c := strings.ToUpper(strings.TrimSpace(code))
	n := strings.ToLower(name + " " + code)
	switch {
	case c == "DF" || strings.Contains(n, "forecast demand"):
		return "forecast_demand"
	case c == "D" || strings.Contains(n, "demand"):
		if strings.Contains(n, "forecast") {
			return "forecast_demand"
		}
		return "demand"
	case c == "TI" || strings.Contains(n, "interchange"):
		return "interchange"
	default:
		return ""
	}
}

func configuredRegions() []region {
	raw := strings.TrimSpace(os.Getenv("EIA930_REGIONS"))
	if raw == "" {
		raw = "CAL,ERCO,MISO,NYIS,PJM,ISNE,SWPP,BPAT"
	}
	out := []region{}
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		code := strings.ToUpper(strings.TrimSpace(part))
		if code == "" || seen[code] {
			continue
		}
		r, ok := regionCatalog[code]
		if !ok {
			continue
		}
		seen[code] = true
		out = append(out, r)
	}
	return out
}

func parsePeriod(raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15",
		"2006-01-02T15Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func meanStd(samples []sample) (float64, float64) {
	if len(samples) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, s := range samples {
		sum += s.Value
	}
	mean := sum / float64(len(samples))
	if len(samples) < 2 {
		return mean, 0
	}
	variance := 0.0
	for _, s := range samples {
		d := s.Value - mean
		variance += d * d
	}
	return mean, math.Sqrt(variance / float64(len(samples)-1))
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(strings.ToLower(s))))
	return "eia930:" + hex.EncodeToString(h[:])
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}
