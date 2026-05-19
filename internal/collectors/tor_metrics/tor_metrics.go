// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Tor Metrics collector — country-level direct-user and bridge-user deltas.
//
// Tor Metrics is daily and lagged, so this source is intentionally coarse:
// one event per watched country only when the latest available daily estimate
// shows a material direct-user drop, bridge-demand surge, or both.
package tor_metrics

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/actors"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://metrics.torproject.org/"

var defaultCountries = []string{
	"IR", "RU", "CN", "MM", "SY", "YE", "SD", "KP",
	"VE", "CU", "TR", "PK", "BD", "EG", "ET", "AF", "LY",
	"UA", "LB", "IQ",
}

type Collector struct {
	countries []string
}

func New() (*Collector, error) {
	countries := defaultCountries
	if env := strings.TrimSpace(os.Getenv("TOR_METRICS_WATCHLIST")); env != "" {
		countries = geo.SplitCountryCodes(env)
	}
	if len(countries) == 0 {
		return nil, fmt.Errorf("TOR_METRICS_WATCHLIST empty")
	}
	return &Collector{countries: countries}, nil
}

func (c *Collector) ID() string               { return "tor_metrics" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

type dailyUsers struct {
	Date  time.Time
	Users float64
	Frac  float64
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	end := now.AddDate(0, 0, -2)
	start := end.AddDate(0, 0, -45)

	out := make([]events.Event, 0, len(c.countries))
	var lastErr error
	for _, cc := range c.countries {
		ev, ok, err := c.fetchCountry(ctx, cc, start, end, now)
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

func (c *Collector) fetchCountry(ctx context.Context, cc string, start, end time.Time, now time.Time) (events.Event, bool, error) {
	cc = strings.ToUpper(strings.TrimSpace(cc))
	ll, ok := geo.Centroids[cc]
	if !ok {
		return events.Event{}, false, nil
	}
	direct, err := fetchSeries(ctx, "userstats-relay-country", strings.ToLower(cc), start, end)
	if err != nil {
		return events.Event{}, false, err
	}
	bridge, err := fetchSeries(ctx, "userstats-bridge-country", strings.ToLower(cc), start, end)
	if err != nil {
		return events.Event{}, false, err
	}
	s := scoreCountry(direct, bridge)
	if !s.material() {
		return events.Event{}, false, nil
	}
	ts := s.latestDate
	if ts.IsZero() {
		ts = now
	}
	props := actors.EnrichNetworkCountryProps(map[string]any{
		"country":                      cc,
		"latest_date":                  ts.Format("2006-01-02"),
		"direct_users":                 s.directLatest,
		"direct_prev7_avg":             s.directPrev7,
		"direct_drop_ratio":            s.directDropRatio,
		"bridge_users":                 s.bridgeLatest,
		"bridge_prev7_avg":             s.bridgePrev7,
		"bridge_surge_ratio":           s.bridgeSurgeRatio,
		"direct_connecting_drop_score": s.directDropScore,
		"bridge_demand_surge_score":    s.bridgeSurgeScore,
		"censorship_pressure_score":    s.pressureScore,
		"source_api_endpoint":          endpoint,
	}, cc)
	return events.Event{
		Ts:     ts,
		Source: "tor_metrics",
		ExtID:  fmt.Sprintf("%s_%s", cc, ts.Format("20060102")),
		Lat:    ll.Lat,
		Lon:    ll.Lon,
		Props:  props,
	}, true, nil
}

func fetchSeries(ctx context.Context, graph, country string, start, end time.Time) ([]dailyUsers, error) {
	v := url.Values{}
	v.Set("start", start.Format("2006-01-02"))
	v.Set("end", end.Format("2006-01-02"))
	v.Set("country", country)
	buf, err := httpx.GetBytes(ctx, endpoint+graph+".csv?"+v.Encode(), map[string]string{"Accept": "text/csv"})
	if err != nil {
		return nil, err
	}
	return parseSeries(buf)
}

func parseSeries(buf []byte) ([]dailyUsers, error) {
	lines := make([][]byte, 0, bytes.Count(buf, []byte{'\n'}))
	for _, line := range bytes.Split(buf, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		lines = append(lines, line)
	}
	r := csv.NewReader(bytes.NewReader(bytes.Join(lines, []byte{'\n'})))
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) <= 1 {
		return nil, nil
	}
	header := map[string]int{}
	for i, h := range rows[0] {
		header[strings.ToLower(strings.TrimSpace(h))] = i
	}
	dateIdx, dateOK := header["date"]
	usersIdx, usersOK := header["users"]
	fracIdx, fracOK := header["frac"]
	if !dateOK || !usersOK {
		return nil, fmt.Errorf("tor metrics csv missing date/users columns")
	}
	out := make([]dailyUsers, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) <= usersIdx || len(row) <= dateIdx {
			continue
		}
		d, err := time.Parse("2006-01-02", row[dateIdx])
		if err != nil {
			continue
		}
		users, err := strconv.ParseFloat(strings.TrimSpace(row[usersIdx]), 64)
		if err != nil {
			continue
		}
		var frac float64
		if fracOK && len(row) > fracIdx {
			frac, _ = strconv.ParseFloat(strings.TrimSpace(row[fracIdx]), 64)
		}
		out = append(out, dailyUsers{Date: d, Users: users, Frac: frac})
	}
	return out, nil
}

type torScore struct {
	latestDate time.Time

	directLatest    float64
	directPrev7     float64
	directDropRatio float64
	directDropScore float64

	bridgeLatest     float64
	bridgePrev7      float64
	bridgeSurgeRatio float64
	bridgeSurgeScore float64

	pressureScore float64
}

func scoreCountry(direct, bridge []dailyUsers) torScore {
	var s torScore
	if len(direct) > 0 {
		latest := direct[len(direct)-1]
		s.latestDate = latest.Date
		s.directLatest = latest.Users
		s.directPrev7 = meanUsers(tailBeforeLatest(direct, 7))
		if s.directPrev7 > 0 {
			s.directDropRatio = (s.directPrev7 - s.directLatest) / s.directPrev7
			if s.directLatest >= 25 && s.directDropRatio >= 0.35 {
				s.directDropScore = clamp((s.directDropRatio-0.25)*4.0, 0, 3)
			}
		}
	}
	if len(bridge) > 0 {
		latest := bridge[len(bridge)-1]
		if latest.Date.After(s.latestDate) {
			s.latestDate = latest.Date
		}
		s.bridgeLatest = latest.Users
		s.bridgePrev7 = meanUsers(tailBeforeLatest(bridge, 7))
		if s.bridgePrev7 > 0 {
			s.bridgeSurgeRatio = s.bridgeLatest / s.bridgePrev7
			if s.bridgeLatest >= 25 && s.bridgeSurgeRatio >= 1.45 {
				s.bridgeSurgeScore = clamp((s.bridgeSurgeRatio-1.0)*2.0, 0, 3)
			}
		}
	}
	if s.directDropScore > 0 && s.bridgeSurgeScore > 0 {
		s.pressureScore = clamp(0.5*s.directDropScore+0.8*s.bridgeSurgeScore, 0, 3)
	} else if s.bridgeSurgeScore >= 1.5 {
		s.pressureScore = clamp(0.5*s.bridgeSurgeScore, 0, 2)
	}
	return s
}

func (s torScore) material() bool {
	return s.directDropScore >= 0.5 || s.bridgeSurgeScore >= 0.8 || s.pressureScore >= 0.8
}

func tailBeforeLatest(xs []dailyUsers, n int) []dailyUsers {
	if len(xs) <= 1 {
		return nil
	}
	end := len(xs) - 1
	start := end - n
	if start < 0 {
		start = 0
	}
	return xs[start:end]
}

func meanUsers(xs []dailyUsers) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x.Users
	}
	return sum / float64(len(xs))
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
