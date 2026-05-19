// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// CAIDA IODA — Internet Outage Detection and Analysis raw country signals.
//
// The former /api/v2/alerts endpoint now serves the SPA shell as HTML, not JSON.
// The live IODA web app reads raw country time series from:
//
//	https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/{CC}
//
// We convert recent country-level drops / high-loss measurements into compact
// sensor events. This is not a report feed; it is network-observation evidence
// that can corroborate BGP/OONI/RIPE/Cloudflare in downstream analysis.
package ioda

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

var defaultCountries = []string{"IR", "IL", "LB", "UA", "RU", "SY", "IQ", "YE", "SD", "MM", "CN", "KP"}

type Collector struct {
	countries []string
}

func New() (*Collector, error) {
	countries := defaultCountries
	if env := strings.TrimSpace(os.Getenv("IODA_WATCHLIST")); env != "" {
		countries = geo.SplitCountryCodes(env)
	}
	if len(countries) == 0 {
		return nil, fmt.Errorf("IODA_WATCHLIST empty")
	}
	return &Collector{countries: countries}, nil
}
func (c *Collector) ID() string               { return "ioda" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type rawResp struct {
	Data [][]signalSeries `json:"data"`
}

type signalSeries struct {
	EntityCode string `json:"entityCode"`
	EntityName string `json:"entityName"`
	Datasource string `json:"datasource"`
	Subtype    string `json:"subtype"`
	From       int64  `json:"from"`
	Step       int64  `json:"step"`
	Values     []any  `json:"values"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	from := now.Add(-6 * time.Hour).Unix()
	until := now.Unix()

	out := make([]events.Event, 0, len(c.countries))
	var lastErr error
	for _, cc := range c.countries {
		ev, ok, err := c.fetchCountry(ctx, strings.ToUpper(cc), from, until, now)
		if err != nil {
			lastErr = err
			continue
		}
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

func (c *Collector) fetchCountry(ctx context.Context, cc string, from, until int64, now time.Time) (events.Event, bool, error) {
	v := url.Values{}
	v.Set("from", fmt.Sprint(from))
	v.Set("until", fmt.Sprint(until))
	u := "https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/" + cc + "?" + v.Encode()

	var raw rawResp
	if err := httpx.GetJSON(ctx, u, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return events.Event{}, false, err
	}

	s := iodaScore{country: cc}
	for _, group := range raw.Data {
		for _, series := range group {
			if strings.ToUpper(series.EntityCode) != cc {
				continue
			}
			s.entityName = firstNonEmpty(s.entityName, series.EntityName)
			s.observe(series)
		}
	}
	if !s.material() {
		return events.Event{}, false, nil
	}
	ll, ok := geo.Centroids[cc]
	if !ok {
		return events.Event{}, false, nil
	}
	ts := s.latest
	if ts.IsZero() {
		ts = now
	}
	props := actors.EnrichNetworkCountryProps(map[string]any{
		"country":             cc,
		"entity_name":         s.entityName,
		"outage_score":        s.score(),
		"latest_gtr_norm":     s.gtrNorm,
		"latest_ping_loss":    s.pingLoss,
		"latest_probe_count":  s.probeCount,
		"bgp_drop_ratio":      s.bgpDropRatio,
		"material_reason":     s.reason(),
		"window_start_unix":   from,
		"window_end_unix":     until,
		"source_api_endpoint": "api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country",
	}, cc)
	return events.Event{
		Ts:     ts,
		Source: "ioda",
		ExtID:  fmt.Sprintf("%s_%s", cc, ts.Format("200601021504")),
		Lat:    ll.Lat,
		Lon:    ll.Lon,
		Props:  props,
	}, true, nil
}

type iodaScore struct {
	country      string
	entityName   string
	latest       time.Time
	gtrNorm      float64
	gtrSeen      bool
	pingLoss     float64
	probeCount   float64
	pingSeen     bool
	bgpDropRatio float64
	bgpSeen      bool
}

func (s *iodaScore) observe(series signalSeries) {
	ds := strings.ToLower(strings.TrimSpace(series.Datasource))
	subtype := strings.ToLower(strings.TrimSpace(series.Subtype))
	switch {
	case ds == "gtr-norm":
		if val, ts, ok := latestFloatValue(series); ok {
			s.gtrNorm = val
			s.gtrSeen = true
			s.keepLatest(ts)
		}
	case ds == "ping-slash24-loss":
		if loss, probes, ts, ok := latestLossValue(series); ok {
			s.pingLoss = loss
			s.probeCount = probes
			s.pingSeen = true
			s.keepLatest(ts)
		}
	case ds == "bgp" && subtype == "":
		if ratio, ts, ok := bgpDropRatio(series); ok {
			s.bgpDropRatio = ratio
			s.bgpSeen = true
			s.keepLatest(ts)
		}
	}
}

func (s *iodaScore) keepLatest(ts time.Time) {
	if ts.After(s.latest) {
		s.latest = ts
	}
}

func (s iodaScore) material() bool {
	return (s.gtrSeen && s.gtrNorm > 0 && s.gtrNorm <= 0.65) ||
		(s.pingSeen && s.pingLoss >= 50 && s.probeCount >= 25) ||
		(s.bgpSeen && s.bgpDropRatio >= 0.03)
}

func (s iodaScore) score() float64 {
	score := 0.0
	if s.gtrSeen && s.gtrNorm > 0 {
		score += clamp((1.0-s.gtrNorm)*4.0, 0, 3)
	}
	if s.pingSeen {
		score += clamp(s.pingLoss/35.0, 0, 3)
	}
	if s.bgpSeen {
		score += clamp(s.bgpDropRatio*20.0, 0, 3)
	}
	return score
}

func (s iodaScore) reason() string {
	parts := []string{}
	if s.gtrSeen && s.gtrNorm > 0 && s.gtrNorm <= 0.65 {
		parts = append(parts, "low Google transparency normalized traffic")
	}
	if s.pingSeen && s.pingLoss >= 50 && s.probeCount >= 25 {
		parts = append(parts, "high active probing loss")
	}
	if s.bgpSeen && s.bgpDropRatio >= 0.03 {
		parts = append(parts, "BGP visible-prefix drop")
	}
	return strings.Join(parts, "; ")
}

func latestFloatValue(series signalSeries) (float64, time.Time, bool) {
	for i := len(series.Values) - 1; i >= 0; i-- {
		v, ok := series.Values[i].(float64)
		if !ok {
			continue
		}
		return v, seriesTimestamp(series, i), true
	}
	return 0, time.Time{}, false
}

func latestLossValue(series signalSeries) (float64, float64, time.Time, bool) {
	for i := len(series.Values) - 1; i >= 0; i-- {
		rows, ok := series.Values[i].([]any)
		if !ok || len(rows) == 0 {
			continue
		}
		bestLoss := 0.0
		bestProbes := 0.0
		for _, row := range rows {
			m, ok := row.(map[string]any)
			if !ok {
				continue
			}
			agg, ok := m["agg_values"].(map[string]any)
			if !ok {
				continue
			}
			loss, _ := agg["loss_pct"].(float64)
			probes, _ := agg["probe_count"].(float64)
			if loss > bestLoss {
				bestLoss = loss
				bestProbes = probes
			}
		}
		if bestLoss > 0 {
			return bestLoss, bestProbes, seriesTimestamp(series, i), true
		}
	}
	return 0, 0, time.Time{}, false
}

func bgpDropRatio(series signalSeries) (float64, time.Time, bool) {
	vals := []float64{}
	idxs := []int{}
	for i, raw := range series.Values {
		v, ok := raw.(float64)
		if !ok || v <= 0 {
			continue
		}
		vals = append(vals, v)
		idxs = append(idxs, i)
	}
	if len(vals) < 3 {
		return 0, time.Time{}, false
	}
	maxv := vals[0]
	for _, v := range vals {
		if v > maxv {
			maxv = v
		}
	}
	last := vals[len(vals)-1]
	if maxv <= 0 {
		return 0, time.Time{}, false
	}
	return (maxv - last) / maxv, seriesTimestamp(series, idxs[len(idxs)-1]), true
}

func seriesTimestamp(series signalSeries, idx int) time.Time {
	step := series.Step
	if step <= 0 {
		step = 300
	}
	return time.Unix(series.From+int64(idx)*step, 0).UTC()
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
