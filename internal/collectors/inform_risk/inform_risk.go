// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package inform_risk ingests the free, keyless INFORM Risk and INFORM
// Severity spreadsheets published by the European Commission JRC. These are
// country/crisis priors, so they are emitted as context features rather than
// fresh incident observations.
package inform_risk

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/xuri/excelize/v2"
)

const (
	sourceID     = "inform_risk_severity"
	riskURL      = "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/2026/INFORM_Risk_2026_v072.xlsx"
	severityURL  = "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/Severity/2026/202603_INFORM_Severity_-_March_2026.xlsx"
	countriesURL = "https://goadmin.ifrc.org/api/v2/country/?limit=400"
)

var (
	riskProductDate     = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	severityProductDate = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	httpClient          = collectorutil.HTTPClient(120 * time.Second)
)

type Collector struct {
	maxEvents int
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_INFORM_RISK") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_INFORM_RISK=1")
	}
	return &Collector{maxEvents: collectorutil.EnvInt("INFORM_RISK_MAX_EVENTS", 160, 20, 500)}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 7 * 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	countries, _ := fetchCountryCentroids(ctx)
	out := []events.Event{}
	var firstErr error
	if buf, err := httpx.GetBytesWithClient(ctx, httpClient, riskURL, map[string]string{"Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}); err == nil {
		out = append(out, parseRiskWorkbook(buf, countries)...)
	} else {
		firstErr = err
	}
	if buf, err := httpx.GetBytesWithClient(ctx, httpClient, severityURL, map[string]string{"Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}); err == nil {
		out = append(out, parseSeverityWorkbook(buf, countries)...)
	} else if firstErr == nil {
		firstErr = err
	}
	sort.SliceStable(out, func(i, j int) bool {
		return eventPriority(out[i]) > eventPriority(out[j])
	})
	out = dedupe(out)
	if len(out) > c.maxEvents {
		out = out[:c.maxEvents]
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func parseRiskWorkbook(buf []byte, countries map[string]countryPoint) []events.Event {
	f, err := excelize.OpenReader(bytes.NewReader(buf))
	if err != nil {
		return nil
	}
	defer f.Close()
	sheet := chooseSheet(f.GetSheetList(), "INFORM Risk 2026 (a-z)", "risk")
	if sheet == "" {
		return nil
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil
	}
	headerRow, header, ok := findHeader(rows, []string{"country"}, []string{"inform risk", "inform risk index"})
	if !ok {
		return nil
	}
	out := []events.Event{}
	for _, row := range rows[headerRow+1:] {
		country := value(row, header, "country")
		iso3 := strings.ToUpper(value(row, header, "iso3", "iso"))
		risk, riskOK := valueFloat(row, header, "inform risk", "inform risk index", "inform risk 2026")
		if !riskOK || risk < 5 {
			continue
		}
		pt, ok := lookupCountry(countries, country, iso3)
		if !ok {
			continue
		}
		hazard, _ := valueFloat(row, header, "hazards & exposure", "hazard & exposure", "hazard exposure")
		vulnerability, _ := valueFloat(row, header, "vulnerability")
		coping, _ := valueFloat(row, header, "lack of coping capacity")
		props := map[string]any{
			"source_provider":                 "INFORM / European Commission JRC",
			"source_api_endpoint":             riskURL,
			"source_public_url":               "https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Risk",
			"source_provider_kind":            "humanitarian_risk_index",
			"indicator":                       "INFORM Risk",
			"country":                         country,
			"country_iso3":                    iso3,
			"product_date":                    riskProductDate.Format("2006-01-02"),
			"inform_risk_score_raw":           round(risk, 2),
			"hazard_exposure_score_raw":       round(hazard, 2),
			"vulnerability_score_raw":         round(vulnerability, 2),
			"coping_capacity_gap_score_raw":   round(coping, 2),
			"inform_risk_score":               normalized10(risk),
			"hazard_exposure_score":           normalized10(hazard),
			"vulnerability_score":             normalized10(vulnerability),
			"coping_capacity_gap_score":       normalized10(coping),
			"humanitarian_risk_prior_score":   normalized10(risk),
			"humanitarian_prior_context_only": true,
			"source_payload_validity":         validity(riskProductDate, riskProductDate.AddDate(1, 0, 0), "inform_risk_annual_index"),
		}
		out = append(out, events.Event{
			Ts:     riskProductDate,
			Source: sourceID,
			ExtID:  "risk:" + firstNonEmpty(iso3, collectorutil.StableID(country)) + ":2026",
			Lat:    pt.Lat,
			Lon:    pt.Lon,
			Props:  props,
		})
	}
	return out
}

func parseSeverityWorkbook(buf []byte, countries map[string]countryPoint) []events.Event {
	f, err := excelize.OpenReader(bytes.NewReader(buf))
	if err != nil {
		return nil
	}
	defer f.Close()
	sheet := chooseSheet(f.GetSheetList(), "INFORM Severity - all crises", "severity")
	if sheet == "" {
		return nil
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil
	}
	headerRow, header, ok := findHeader(rows, []string{"country"}, []string{"inform severity", "severity index"})
	if !ok {
		return nil
	}
	out := []events.Event{}
	for _, row := range rows[headerRow+1:] {
		country := value(row, header, "country")
		iso3 := strings.ToUpper(value(row, header, "iso3", "iso"))
		severity, sevOK := valueFloat(row, header, "inform severity index", "inform severity", "severity index")
		if !sevOK || severity < 3 {
			continue
		}
		pt, ok := lookupCountry(countries, country, iso3)
		if !ok {
			continue
		}
		crisis := value(row, header, "crisis")
		crisisID := value(row, header, "crisis id", "crisis_id")
		impact, _ := valueFloat(row, header, "impact of the crisis", "impact")
		conditions, _ := valueFloat(row, header, "conditions of people affected", "conditions")
		complexity, _ := valueFloat(row, header, "complexity of the crisis", "complexity")
		props := map[string]any{
			"source_provider":                   "INFORM / European Commission JRC",
			"source_api_endpoint":               severityURL,
			"source_public_url":                 "https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Severity",
			"source_provider_kind":              "humanitarian_crisis_severity_index",
			"indicator":                         "INFORM Severity",
			"country":                           country,
			"country_iso3":                      iso3,
			"crisis":                            crisis,
			"crisis_id":                         crisisID,
			"product_date":                      severityProductDate.Format("2006-01-02"),
			"inform_severity_score_raw":         round(severity, 2),
			"impact_score_raw":                  round(impact, 2),
			"conditions_score_raw":              round(conditions, 2),
			"complexity_score_raw":              round(complexity, 2),
			"inform_severity_score":             normalized5(severity),
			"impact_score":                      normalized5(impact),
			"conditions_score":                  normalized5(conditions),
			"complexity_score":                  normalized5(complexity),
			"humanitarian_severity_prior_score": normalized5(severity),
			"humanitarian_prior_context_only":   true,
			"source_payload_validity":           validity(severityProductDate, severityProductDate.Add(120*24*time.Hour), "inform_severity_monthly_index"),
		}
		out = append(out, events.Event{
			Ts:     severityProductDate,
			Source: sourceID,
			ExtID:  "severity:" + firstNonEmpty(crisisID, iso3, collectorutil.StableID(country+crisis)) + ":202603",
			Lat:    pt.Lat,
			Lon:    pt.Lon,
			Props:  props,
		})
	}
	return out
}

func chooseSheet(sheets []string, exact, contains string) string {
	for _, s := range sheets {
		if strings.EqualFold(s, exact) {
			return s
		}
	}
	contains = strings.ToLower(contains)
	for _, s := range sheets {
		if strings.Contains(strings.ToLower(s), contains) && !strings.Contains(strings.ToLower(s), "hidden") {
			return s
		}
	}
	return ""
}

func findHeader(rows [][]string, required []string, anyMetric []string) (int, map[string]int, bool) {
	limit := minInt(len(rows), 80)
	for i := 0; i < limit; i++ {
		h := makeHeader(rows[i])
		if len(h) == 0 {
			continue
		}
		allReq := true
		for _, req := range required {
			if _, ok := colIndex(h, req); !ok {
				allReq = false
				break
			}
		}
		if !allReq {
			continue
		}
		if _, ok := colIndex(h, anyMetric...); ok {
			return i, h, true
		}
	}
	return 0, nil, false
}

func makeHeader(row []string) map[string]int {
	h := map[string]int{}
	for i, cell := range row {
		n := normalize(cell)
		if n == "" {
			continue
		}
		if _, exists := h[n]; !exists {
			h[n] = i
		}
	}
	return h
}

func colIndex(header map[string]int, aliases ...string) (int, bool) {
	for key, i := range header {
		for _, alias := range aliases {
			a := normalize(alias)
			if key == a || strings.Contains(key, a) || strings.Contains(a, key) {
				return i, true
			}
		}
	}
	return 0, false
}

func value(row []string, header map[string]int, aliases ...string) string {
	i, ok := colIndex(header, aliases...)
	if !ok || i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func valueFloat(row []string, header map[string]int, aliases ...string) (float64, bool) {
	raw := strings.ReplaceAll(value(row, header, aliases...), ",", "")
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "n/a") || strings.EqualFold(raw, "no data") {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	return v, err == nil
}

func normalize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

type countryResponse struct {
	Results []countryRecord `json:"results"`
}

type countryRecord struct {
	ISO      string `json:"iso"`
	ISO3     string `json:"iso3"`
	Name     string `json:"name"`
	Centroid struct {
		Coordinates []float64 `json:"coordinates"`
	} `json:"centroid"`
}

type countryPoint struct {
	Lat  float64
	Lon  float64
	Name string
}

func fetchCountryCentroids(ctx context.Context) (map[string]countryPoint, error) {
	out := fallbackCountries()
	var raw countryResponse
	if err := httpx.GetJSONWithClient(ctx, httpClient, countriesURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return out, err
	}
	for _, c := range raw.Results {
		if len(c.Centroid.Coordinates) < 2 {
			continue
		}
		lon, lat := c.Centroid.Coordinates[0], c.Centroid.Coordinates[1]
		if !collectorutil.ValidLatLon(lat, lon) {
			continue
		}
		pt := countryPoint{Lat: lat, Lon: lon, Name: c.Name}
		for _, key := range []string{c.ISO3, c.ISO, c.Name} {
			key = centroidKey(key)
			if key != "" {
				out[key] = pt
			}
		}
	}
	return out, nil
}

func lookupCountry(countries map[string]countryPoint, name, iso3 string) (countryPoint, bool) {
	for _, key := range []string{iso3, name} {
		if pt, ok := countries[centroidKey(key)]; ok {
			return pt, true
		}
	}
	return countryPoint{}, false
}

func centroidKey(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func fallbackCountries() map[string]countryPoint {
	return map[string]countryPoint{
		"afg": {33.94, 67.71, "Afghanistan"},
		"som": {5.15, 46.20, "Somalia"},
		"ssd": {6.88, 31.31, "South Sudan"},
		"sdn": {12.86, 30.22, "Sudan"},
		"yem": {15.55, 48.52, "Yemen"},
		"hti": {18.97, -72.29, "Haiti"},
		"eth": {9.15, 40.49, "Ethiopia"},
		"cod": {-4.04, 21.76, "Democratic Republic of the Congo"},
		"nga": {9.08, 8.68, "Nigeria"},
		"mli": {17.57, -3.99, "Mali"},
		"ner": {17.61, 8.08, "Niger"},
	}
}

func normalized10(v float64) float64 {
	return round(propx.ClampFloat(v/3.0, 0, 3.5), 2)
}

func normalized5(v float64) float64 {
	return round(propx.ClampFloat(v/2.0, 0, 3.0), 2)
}

func eventPriority(e events.Event) float64 {
	best := 0.0
	for _, key := range []string{"humanitarian_severity_prior_score", "humanitarian_risk_prior_score", "inform_risk_score", "inform_severity_score"} {
		if v, ok := propx.Float(e.Props[key]); ok && v > best {
			best = v
		}
	}
	return best
}

func validity(start, end time.Time, basis string) map[string]any {
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
		key := e.Source + ":" + e.ExtID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, e)
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
