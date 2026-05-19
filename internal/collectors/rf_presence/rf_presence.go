// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package rf_presence ingests low-rate public amateur-radio reception density.
//
// The first spike uses WSPR Live as a WSPRNet-derived data source. It does not
// try to scrape the whole network: one bounded ClickHouse query aggregates
// recent spots into coarse receiver/transmitter locations, which the signal
// engine can baseline per cell.
package rf_presence

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	defaultEndpoint  = "https://db1.wspr.live/"
	defaultWindowMin = 30
	defaultLimit     = 300
)

type Collector struct {
	endpoint  string
	windowMin int
	limit     int
}

func New() (*Collector, error) {
	return &Collector{
		endpoint:  strings.TrimRight(envString("WSPR_LIVE_ENDPOINT", defaultEndpoint), "/") + "/",
		windowMin: envInt("RF_PRESENCE_WINDOW_MIN", defaultWindowMin, 15, 180),
		limit:     envInt("RF_PRESENCE_LIMIT", defaultLimit, 25, 1000),
	}, nil
}

func (c *Collector) ID() string               { return "rf_presence" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

type clickhouseResponse struct {
	Data []aggregateRow `json:"data"`
}

type aggregateRow struct {
	Role         string  `json:"role"`
	Bucket       string  `json:"bucket"`
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
	Spots        int     `json:"spots"`
	Receivers    int     `json:"receivers"`
	Transmitters int     `json:"transmitters"`
	Bands        int     `json:"bands"`
	AvgSNR       float64 `json:"avg_snr"`
	FirstSeen    string  `json:"first_seen"`
	LastSeen     string  `json:"last_seen"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	query := c.query()
	raw, err := httpx.GetBytes(ctx, c.endpoint+"?query="+url.QueryEscape(query), map[string]string{"Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	var resp clickhouseResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode wspr live response: %w", err)
	}
	out := make([]events.Event, 0, len(resp.Data))
	for _, row := range resp.Data {
		if ev, ok := eventFromRow(row, c.windowMin); ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (c *Collector) query() string {
	return fmt.Sprintf(`
SELECT role, bucket, lat, lon,
       count() AS spots,
       uniqExact(rx_sign) AS receivers,
       uniqExact(tx_sign) AS transmitters,
       uniqExact(band) AS bands,
       round(avg(snr), 2) AS avg_snr,
       min(time) AS first_seen,
       max(time) AS last_seen
FROM (
  SELECT 'receiver' AS role,
         toStartOfInterval(time, INTERVAL 15 minute) AS bucket,
         round(rx_lat, 1) AS lat,
         round(rx_lon, 1) AS lon,
         rx_sign, tx_sign, band, snr, time
    FROM wspr.rx
   WHERE time >= now() - INTERVAL %d MINUTE
     AND rx_lat BETWEEN -85 AND 85
     AND rx_lon BETWEEN -180 AND 180
  UNION ALL
  SELECT 'transmitter' AS role,
         toStartOfInterval(time, INTERVAL 15 minute) AS bucket,
         round(tx_lat, 1) AS lat,
         round(tx_lon, 1) AS lon,
         rx_sign, tx_sign, band, snr, time
    FROM wspr.rx
   WHERE time >= now() - INTERVAL %d MINUTE
     AND tx_lat BETWEEN -85 AND 85
     AND tx_lon BETWEEN -180 AND 180
)
GROUP BY role, bucket, lat, lon
HAVING (role = 'receiver' AND (spots >= 20 OR receivers >= 2))
    OR (role = 'transmitter' AND transmitters >= 2 AND spots >= 8)
ORDER BY spots DESC
LIMIT %d
FORMAT JSON`, c.windowMin, c.windowMin, c.limit)
}

func eventFromRow(row aggregateRow, windowMin int) (events.Event, bool) {
	role := strings.ToLower(strings.TrimSpace(row.Role))
	if role != "receiver" && role != "transmitter" {
		return events.Event{}, false
	}
	if row.Spots <= 0 || !validCoord(row.Lat, row.Lon) {
		return events.Event{}, false
	}
	ts := parseWSPRTime(row.LastSeen)
	if ts.IsZero() {
		ts = parseWSPRTime(row.Bucket)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	firstSeen := parseWSPRTime(row.FirstSeen)
	props := map[string]any{
		"network":                     "wsprnet",
		"feed":                        "wspr.live",
		"role":                        role,
		"bucket":                      row.Bucket,
		"window_minutes":              windowMin,
		"spots":                       row.Spots,
		"receivers":                   row.Receivers,
		"transmitters":                row.Transmitters,
		"bands":                       row.Bands,
		"avg_snr":                     row.AvgSNR,
		"spot_density_score":          spotDensityScore(row.Spots),
		"receiver_diversity_score":    diversityScore(row.Receivers),
		"transmitter_diversity_score": diversityScore(row.Transmitters),
		"band_diversity_score":        bandDiversityScore(row.Bands),
		"source_api_endpoint":         defaultEndpoint,
	}
	if !firstSeen.IsZero() {
		props["first_seen"] = firstSeen.Format(time.RFC3339)
	}
	props["last_seen"] = ts.Format(time.RFC3339)
	return events.Event{
		Ts:     ts,
		Source: "rf_presence",
		ExtID:  fmt.Sprintf("wspr_%s_%.1f_%.1f_%s", role, row.Lat, row.Lon, ts.Format("20060102T1504")),
		Lat:    row.Lat,
		Lon:    row.Lon,
		Props:  props,
	}, true
}

func spotDensityScore(spots int) float64 {
	if spots <= 0 {
		return 0
	}
	return round2(clamp(math.Log1p(float64(spots))/3.0, 0, 3))
}

func diversityScore(n int) float64 {
	if n <= 0 {
		return 0
	}
	return round2(clamp(math.Log1p(float64(n))/1.5, 0, 3))
}

func bandDiversityScore(n int) float64 {
	if n <= 1 {
		return 0
	}
	return round2(clamp(float64(n)/4.0, 0, 3))
}

func parseWSPRTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func validCoord(lat, lon float64) bool {
	return lat >= -85 && lat <= 85 && lon >= -180 && lon <= 180 && !(lat == 0 && lon == 0)
}

func envString(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}

func envInt(name string, def, minVal, maxVal int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < minVal {
		return minVal
	}
	if n > maxVal {
		return maxVal
	}
	return n
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

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
