// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package unhcr_displacement ingests annual UNHCR population/displacement
// summaries as country-centroid context priors.
package unhcr_displacement

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://api.unhcr.org/population/v1/population/"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "unhcr_displacement" }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	year := time.Now().UTC().Year()
	var items []unhcrItem
	var usedYear int
	var firstErr error
	for y := year; y >= year-2; y-- {
		got, err := fetchYear(ctx, y)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(got) > 0 {
			items = got
			usedYear = y
			break
		}
	}
	if len(items) == 0 {
		return nil, firstErr
	}
	return eventsFromItems(items, usedYear), nil
}

type unhcrItem struct {
	COOISO        string `json:"coo_iso"`
	COOName       string `json:"coo_name"`
	COAISO        string `json:"coa_iso"`
	COAName       string `json:"coa_name"`
	Refugees      any    `json:"refugees"`
	AsylumSeekers any    `json:"asylum_seekers"`
	IDPs          any    `json:"idps"`
	Stateless     any    `json:"stateless"`
}

type countryAgg struct {
	Code          string
	Name          string
	Refugees      int
	AsylumSeekers int
	IDPs          int
	Stateless     int
	HostRefugees  int
	HostAsylum    int
}

func fetchYear(ctx context.Context, year int) ([]unhcrItem, error) {
	all := []unhcrItem{}
	for page := 1; page <= 25; page++ {
		q := url.Values{}
		q.Set("year", strconv.Itoa(year))
		q.Set("limit", "10000")
		q.Set("page", strconv.Itoa(page))
		q.Set("coo_all", "true")
		q.Set("coa_all", "true")
		var body struct {
			Items    []unhcrItem `json:"items"`
			MaxPages int         `json:"maxPages"`
		}
		if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &body); err != nil {
			return nil, err
		}
		if len(body.Items) == 0 {
			break
		}
		all = append(all, body.Items...)
		if body.MaxPages > 0 && page >= body.MaxPages {
			break
		}
		if len(body.Items) < 10000 {
			break
		}
	}
	return all, nil
}

func eventsFromItems(items []unhcrItem, year int) []events.Event {
	byCountry := map[string]*countryAgg{}
	for _, item := range items {
		origin := strings.TrimSpace(item.COOISO)
		host := strings.TrimSpace(item.COAISO)
		refugees := intFrom(item.Refugees)
		asylum := intFrom(item.AsylumSeekers)
		idps := intFrom(item.IDPs)
		stateless := intFrom(item.Stateless)
		if origin != "" {
			a := byCountry[origin]
			if a == nil {
				a = &countryAgg{Code: origin, Name: firstNonEmpty(item.COOName, origin)}
				byCountry[origin] = a
			}
			a.Refugees += refugees
			a.AsylumSeekers += asylum
			a.IDPs += idps
			a.Stateless += stateless
		}
		if host != "" {
			a := byCountry[host]
			if a == nil {
				a = &countryAgg{Code: host, Name: firstNonEmpty(item.COAName, host)}
				byCountry[host] = a
			}
			a.HostRefugees += refugees
			a.HostAsylum += asylum
		}
	}
	rows := make([]*countryAgg, 0, len(byCountry))
	for _, a := range byCountry {
		rows = append(rows, a)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Priority() > rows[j].Priority() })
	if len(rows) > 150 {
		rows = rows[:150]
	}
	ts := time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)
	out := make([]events.Event, 0, len(rows))
	for _, a := range rows {
		c, ok := iso3Centroids[a.Code]
		if !ok {
			continue
		}
		props := map[string]any{
			"country_code":        a.Code,
			"country":             a.Name,
			"year":                year,
			"refugees":            a.Refugees,
			"asylum_seekers":      a.AsylumSeekers,
			"idps":                a.IDPs,
			"stateless":           a.Stateless,
			"total_displaced":     a.TotalDisplaced(),
			"host_refugees":       a.HostRefugees,
			"host_asylum_seekers": a.HostAsylum,
			"host_total":          a.HostTotal(),
			"priority_score":      a.Priority(),
			"source_api_endpoint": endpoint,
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "unhcr_displacement",
			ExtID:  fmt.Sprintf("%s:%d", a.Code, year),
			Lat:    c[0],
			Lon:    c[1],
			Props:  props,
		})
	}
	return out
}

func (a *countryAgg) TotalDisplaced() int {
	return a.Refugees + a.AsylumSeekers + a.IDPs + a.Stateless
}

func (a *countryAgg) HostTotal() int {
	return a.HostRefugees + a.HostAsylum
}

func (a *countryAgg) Priority() int {
	if a.TotalDisplaced() > a.HostTotal() {
		return a.TotalDisplaced()
	}
	return a.HostTotal()
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(x))
		return n
	default:
		return 0
	}
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

var iso3Centroids = map[string][2]float64{
	"AFG": {33.9, 67.7}, "SYR": {35.0, 38.0}, "UKR": {48.4, 31.2}, "SDN": {15.5, 32.5},
	"SSD": {6.9, 31.3}, "SOM": {5.2, 46.2}, "COD": {-4.0, 21.8}, "MMR": {19.8, 96.7},
	"YEM": {15.6, 48.5}, "ETH": {9.1, 40.5}, "VEN": {6.4, -66.6}, "IRQ": {33.2, 43.7},
	"COL": {4.6, -74.1}, "NGA": {9.1, 7.5}, "PSE": {31.9, 35.2}, "TUR": {39.9, 32.9},
	"DEU": {51.2, 10.4}, "PAK": {30.4, 69.3}, "UGA": {1.4, 32.3}, "BGD": {23.7, 90.4},
	"KEN": {0.0, 38.0}, "TCD": {15.5, 19.0}, "JOR": {31.0, 36.0}, "LBN": {33.9, 35.5},
	"EGY": {26.8, 30.8}, "IRN": {32.4, 53.7}, "TZA": {-6.4, 34.9}, "RWA": {-1.9, 29.9},
	"CMR": {7.4, 12.4}, "MLI": {17.6, -4.0}, "BFA": {12.3, -1.6}, "NER": {17.6, 8.1},
	"CAF": {6.6, 20.9}, "MOZ": {-18.7, 35.5}, "USA": {37.1, -95.7}, "FRA": {46.2, 2.2},
	"GBR": {55.4, -3.4}, "IND": {20.6, 79.0}, "CHN": {35.9, 104.2}, "RUS": {61.5, 105.3},
	"IRL": {53.1, -8.2}, "ITA": {41.9, 12.6}, "POL": {51.9, 19.1}, "ESP": {40.5, -3.7},
	"THA": {15.9, 101.0}, "MEX": {23.6, -102.6}, "BRA": {-14.2, -51.9}, "PER": {-9.2, -75.0},
	"ECU": {-1.8, -78.2}, "ZAF": {-30.6, 22.9}, "MAR": {31.8, -7.1}, "DZA": {28.0, 1.7},
	"IDN": {-0.8, 113.9}, "MYS": {4.2, 101.9}, "AUS": {-25.3, 133.8}, "CAN": {56.1, -106.3},
}
