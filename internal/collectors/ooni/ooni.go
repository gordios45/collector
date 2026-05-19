// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// OONI collector — recent anomalous / confirmed censorship measurements.
//
// OONI is not a "report" source in this pipeline. It is a network sensor:
// probes in a country observe blocking, DNS failures, TCP failures, or
// circumvention tool interference. That makes it useful as support for
// network disruption analysis, especially when it co-occurs with
// BGP/Cloudflare/RIPE observations.
package ooni

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/actors"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://api.ooni.org/api/v1/aggregation"

var defaultCountries = []string{
	"IR", "IL", "LB", "UA", "RU", "SY", "IQ", "YE", "SD", "MM", "CN", "KP",
	"VE", "CU", "TR", "PK", "BD", "EG", "ET", "AF", "LY",
}

type Collector struct {
	countries []string
}

func New() (*Collector, error) {
	countries := defaultCountries
	if env := strings.TrimSpace(os.Getenv("OONI_WATCHLIST")); env != "" {
		countries = geo.SplitCountryCodes(env)
	}
	if len(countries) == 0 {
		return nil, fmt.Errorf("OONI_WATCHLIST empty")
	}
	return &Collector{countries: countries}, nil
}

func (c *Collector) ID() string               { return "ooni" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type resp struct {
	Result []aggregateRow `json:"result"`
}

type aggregateRow struct {
	ProbeCC          string `json:"probe_cc"`
	TestName         string `json:"test_name"`
	AnomalyCount     int    `json:"anomaly_count"`
	ConfirmedCount   int    `json:"confirmed_count"`
	FailureCount     int    `json:"failure_count"`
	OKCount          int    `json:"ok_count"`
	MeasurementCount int    `json:"measurement_count"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	// Aggregation API uses date, not datetime, when time_grain=day.
	since := now.Add(-24 * time.Hour).Format("2006-01-02")
	until := now.Format("2006-01-02")

	v := url.Values{}
	v.Set("since", since)
	v.Set("until", until)
	v.Set("time_grain", "day")
	v.Set("axis_x", "probe_cc")
	v.Set("axis_y", "test_name")

	var r resp
	if err := httpx.GetJSON(ctx, endpoint+"?"+v.Encode(), nil, &r); err != nil {
		return nil, err
	}

	watch := map[string]bool{}
	for _, cc := range c.countries {
		watch[cc] = true
	}

	out := make([]events.Event, 0, len(c.countries)*8)
	for _, row := range r.Result {
		cc := strings.ToUpper(row.ProbeCC)
		if !watch[cc] {
			continue
		}
		if row.MeasurementCount <= 0 || (row.AnomalyCount == 0 && row.ConfirmedCount == 0 && row.FailureCount == 0) {
			continue
		}
		ll, ok := geo.Centroids[cc]
		if !ok {
			continue
		}
		anomalyRatio := float64(row.AnomalyCount+row.ConfirmedCount) / float64(row.MeasurementCount)
		failureRatio := float64(row.FailureCount) / float64(row.MeasurementCount)
		// OONI has many tiny test-failure rows (for example one failed
		// echcheck) that are useful to researchers but noisy for operational
		// alerting. Emit only aggregates with enough sample size or confirmed
		// blocking so downstream analysis sees censorship/interference, not probe
		// fragility.
		materialFailure := row.MeasurementCount >= 20 && failureRatio >= 0.5
		materialAnomaly := row.MeasurementCount >= 20 && anomalyRatio >= 0.15
		if row.ConfirmedCount == 0 && !materialAnomaly && !materialFailure {
			continue
		}
		blockingScore := anomalyRatio * 3.0
		if materialFailure {
			blockingScore += 0.5
		}
		if row.ConfirmedCount > 0 {
			blockingScore += 1.0
		}
		props := actors.EnrichNetworkCountryProps(map[string]any{
			"country":           cc,
			"test_name":         row.TestName,
			"measurement_count": row.MeasurementCount,
			"anomaly_count":     row.AnomalyCount,
			"confirmed_count":   row.ConfirmedCount,
			"failure_count":     row.FailureCount,
			"ok_count":          row.OKCount,
			"anomaly_ratio":     anomalyRatio,
			"failure_ratio":     failureRatio,
			"blocking_score":    blockingScore,
			"confirmed":         row.ConfirmedCount > 0,
			"window_start":      since,
			"window_end":        until,
		}, cc)
		out = append(out, events.Event{
			Ts:     now,
			Source: "ooni",
			ExtID:  fmt.Sprintf("%s_%s_%s", cc, row.TestName, now.Format("2006010215")),
			Lat:    ll.Lat,
			Lon:    ll.Lon,
			Props:  props,
		})
	}
	return out, nil
}
