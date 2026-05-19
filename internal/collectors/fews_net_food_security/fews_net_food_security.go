// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package fews_net_food_security ingests public FEWS NET food-security
// indicators from the FEWS Data Warehouse API. The endpoints used here are
// keyless and marked Public by FEWS NET.
package fews_net_food_security

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID       = "fews_net_food_security"
	apiBase        = "https://fdw.fews.net/api"
	populationPath = apiBase + "/ipcpopulationsize.json"
	marketPath     = apiBase + "/marketpricefacts.json"
	geoUnitPath    = apiBase + "/geographicunit.json"
)

type Collector struct {
	maxEvents int
	countries []string
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_FEWS_NET_FOOD_SECURITY") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_FEWS_NET_FOOD_SECURITY=1")
	}
	countries := collectorutil.SplitCSV(os.Getenv("FEWS_NET_COUNTRIES"))
	if len(countries) == 0 {
		countries = []string{"AF", "ET", "SO", "SD", "SS", "YE", "HT", "NG"}
	}
	return &Collector{
		maxEvents: collectorutil.EnvInt("FEWS_NET_MAX_EVENTS", 140, 20, 500),
		countries: countries,
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	cache := map[string]geoPoint{}
	out := []events.Event{}
	var firstErr error
	if rows, err := fetchRows(ctx, populationURL(time.Now().UTC())); err == nil {
		for _, row := range rows {
			if ev, ok := c.populationEvent(ctx, cache, row); ok {
				out = append(out, ev)
				if len(out) >= c.maxEvents {
					return dedupe(out), nil
				}
			}
		}
	} else {
		firstErr = err
	}
	for _, country := range c.countries {
		rows, err := fetchRows(ctx, marketURL(country, time.Now().UTC()))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, row := range rows {
			if ev, ok := marketEvent(row); ok {
				out = append(out, ev)
				if len(out) >= c.maxEvents {
					return dedupe(out), nil
				}
			}
		}
	}
	out = dedupe(out)
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type page struct {
	Results []map[string]any `json:"results"`
}

func populationURL(now time.Time) string {
	q := url.Values{}
	q.Set("phase", "3+")
	q.Set("scenario", "CS")
	q.Set("start_date", time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
	q.Set("page_size", "35")
	q.Set("fields", "simple")
	return populationPath + "?" + q.Encode()
}

func marketURL(country string, now time.Time) string {
	q := url.Values{}
	q.Set("country_code", strings.ToUpper(strings.TrimSpace(country)))
	q.Set("start_date", time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
	q.Set("page_size", "12")
	q.Set("fields", "simple")
	return marketPath + "?" + q.Encode()
}

func fetchRows(ctx context.Context, rawURL string) ([]map[string]any, error) {
	var p page
	if err := getJSON(ctx, rawURL, &p); err != nil {
		return nil, err
	}
	return p.Results, nil
}

func (c *Collector) populationEvent(ctx context.Context, cache map[string]geoPoint, row map[string]any) (events.Event, bool) {
	fnid := strings.TrimSpace(propx.StringAt(row, "fnid"))
	if fnid == "" {
		return events.Event{}, false
	}
	pt, ok := cache[fnid]
	if !ok {
		var err error
		pt, err = fetchGeoPoint(ctx, fnid)
		if err != nil || !collectorutil.ValidLatLon(pt.Lat, pt.Lon) {
			return events.Event{}, false
		}
		cache[fnid] = pt
	}
	low, _ := propx.Float(row["low_value"])
	high, _ := propx.Float(row["high_value"])
	mid := midpoint(low, high)
	score := foodInsecurityScore(mid, propx.StringAt(row, "phase"))
	if score <= 0 {
		return events.Event{}, false
	}
	start := parseDate(propx.StringAt(row, "projection_start"))
	if start.IsZero() {
		start = parseDate(propx.StringAt(row, "reporting_date"))
	}
	if start.IsZero() {
		start = time.Now().UTC()
	}
	end := parseDate(propx.StringAt(row, "projection_end"))
	props := map[string]any{
		"source_provider":                       "FEWS NET Data Warehouse",
		"source_api_endpoint":                   populationPath,
		"source_provider_kind":                  "public_food_security_forecast",
		"indicator":                             "IPC population size phase 3+",
		"country":                               propx.StringAt(row, "country"),
		"country_code":                          propx.StringAt(row, "country_code"),
		"geographic_group":                      propx.StringAt(row, "geographic_group"),
		"fewsnet_region":                        propx.StringAt(row, "fewsnet_region"),
		"fnid":                                  fnid,
		"admin_0":                               propx.StringAt(row, "admin_0"),
		"admin_1":                               propx.StringAt(row, "admin_1"),
		"admin_2":                               propx.StringAt(row, "admin_2"),
		"phase":                                 propx.StringAt(row, "phase"),
		"phase_name":                            propx.StringAt(row, "phase_name"),
		"scenario":                              propx.StringAt(row, "scenario"),
		"scenario_name":                         propx.StringAt(row, "scenario_name"),
		"projection_start":                      propx.StringAt(row, "projection_start"),
		"projection_end":                        propx.StringAt(row, "projection_end"),
		"reporting_date":                        propx.StringAt(row, "reporting_date"),
		"population_range":                      propx.StringAt(row, "population_range"),
		"acute_food_insecurity_population_low":  round(low, 0),
		"acute_food_insecurity_population_high": round(high, 0),
		"acute_food_insecurity_population_mid":  round(mid, 0),
		"food_insecurity_score":                 round(score, 2),
		"source_payload_validity":               validity(start, end, "fews_net_projection_window"),
		"data_usage_policy":                     propx.StringAt(row, "data_usage_policy"),
	}
	return events.Event{
		Ts:     start,
		Source: sourceID,
		ExtID:  "ipc-pop:" + firstNonEmpty(propx.StringAt(row, "id"), collectorutil.StableID(fnid+start.Format("2006-01-02"))),
		Lat:    pt.Lat,
		Lon:    pt.Lon,
		Props:  props,
	}, true
}

func marketEvent(row map[string]any) (events.Event, bool) {
	lat, latOK := propx.Float(row["latitude"])
	lon, lonOK := propx.Float(row["longitude"])
	if !latOK || !lonOK || !collectorutil.ValidLatLon(lat, lon) {
		return events.Event{}, false
	}
	oneYear, _ := propx.Float(row["pct_change_from_one_year_ago"])
	fiveYear, _ := propx.Float(row["pct_change_from_five_year_average"])
	twoYear, _ := propx.Float(row["pct_change_from_two_year_average"])
	score := marketStressScore(oneYear, twoYear, fiveYear)
	if score < 0.6 {
		return events.Event{}, false
	}
	ts := parseDate(firstNonEmpty(propx.StringAt(row, "period_date"), propx.StringAt(row, "start_date")))
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	props := map[string]any{
		"source_provider":                   "FEWS NET Data Warehouse",
		"source_api_endpoint":               marketPath,
		"source_provider_kind":              "public_market_price_monitor",
		"indicator":                         "market price fact",
		"country":                           propx.StringAt(row, "country"),
		"country_code":                      propx.StringAt(row, "country_code"),
		"admin_1":                           propx.StringAt(row, "admin_1"),
		"admin_2":                           propx.StringAt(row, "admin_2"),
		"market":                            propx.StringAt(row, "market"),
		"product":                           propx.StringAt(row, "product"),
		"price_type":                        propx.StringAt(row, "price_type"),
		"value":                             row["value"],
		"currency":                          propx.StringAt(row, "currency"),
		"unit":                              propx.StringAt(row, "unit"),
		"common_currency_price":             row["common_currency_price"],
		"pct_change_from_one_year_ago":      round(oneYear, 1),
		"pct_change_from_two_year_average":  round(twoYear, 1),
		"pct_change_from_five_year_average": round(fiveYear, 1),
		"market_price_stress_score":         round(score, 2),
		"food_market_stress_context":        true,
		"period_date":                       propx.StringAt(row, "period_date"),
		"start_date":                        propx.StringAt(row, "start_date"),
		"data_usage_policy":                 propx.StringAt(row, "data_usage_policy"),
		"source_document":                   propx.StringAt(row, "source_document"),
	}
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  "market:" + firstNonEmpty(propx.StringAt(row, "id"), collectorutil.StableID(propx.StringAt(row, "dataseries_name")+ts.Format("2006-01-02"))),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

type geoPage struct {
	Results []struct {
		FNID                string    `json:"fnid"`
		FullName            string    `json:"full_name"`
		Centroid            []float64 `json:"centroid"`
		EstimatedPopulation float64   `json:"estimated_population"`
	} `json:"results"`
}

type geoPoint struct {
	Lat  float64
	Lon  float64
	Name string
}

func fetchGeoPoint(ctx context.Context, fnid string) (geoPoint, error) {
	q := url.Values{}
	q.Set("fnid", fnid)
	q.Set("page_size", "1")
	q.Set("fields", "with_population")
	var gp geoPage
	if err := getJSON(ctx, geoUnitPath+"?"+q.Encode(), &gp); err != nil {
		return geoPoint{}, err
	}
	if len(gp.Results) == 0 || len(gp.Results[0].Centroid) < 2 {
		return geoPoint{}, fmt.Errorf("no centroid for fnid %s", fnid)
	}
	lon, lat := gp.Results[0].Centroid[0], gp.Results[0].Centroid[1]
	if !collectorutil.ValidLatLon(lat, lon) {
		return geoPoint{}, fmt.Errorf("invalid centroid for fnid %s", fnid)
	}
	return geoPoint{Lat: lat, Lon: lon, Name: gp.Results[0].FullName}, nil
}

func getJSON(ctx context.Context, rawURL string, out any) error {
	buf, err := exec.CommandContext(ctx, "curl", "-fsSL", "--connect-timeout", "15", "--max-time", "60", "-A", "gordios/0.1", "-H", "Accept: application/json", rawURL).Output()
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func foodInsecurityScore(pop float64, phase string) float64 {
	if pop <= 0 {
		return 0
	}
	score := math.Log10(pop/100000.0) + 0.8
	if strings.Contains(phase, "4") || strings.Contains(phase, "5") {
		score += 0.6
	}
	return propx.ClampFloat(score, 0, 3.5)
}

func marketStressScore(oneYear, twoYear, fiveYear float64) float64 {
	best := math.Max(oneYear, math.Max(twoYear, fiveYear))
	if best <= 0 {
		return 0
	}
	return propx.ClampFloat((best-20.0)/30.0+0.5, 0, 3.0)
}

func midpoint(low, high float64) float64 {
	if low > 0 && high > 0 {
		return (low + high) / 2
	}
	if high > 0 {
		return high
	}
	return low
}

func parseDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time, basis string) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(30 * 24 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]struct{}{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.Source == "" || e.ExtID == "" || !e.HasPoint() {
			continue
		}
		key := e.Source + ":" + e.ExtID + ":" + e.Ts.Format(time.RFC3339)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, e)
	}
	return out
}
