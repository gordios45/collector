// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package iata_fuel_monitor ingests the public summary sentence from IATA's
// Jet Fuel Price Monitor page.
package iata_fuel_monitor

import (
	"context"
	"fmt"
	"html"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const (
	sourceID = "iata_fuel_monitor"
	pageURL  = "https://www.iata.org/en/publications/economics/fuel-monitor/"
)

type Collector struct {
	client *http.Client
}

func New() (*Collector, error) {
	return &Collector{client: &http.Client{Timeout: 20 * time.Second}}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, modified, err := c.fetch(ctx, pageURL)
	if err != nil {
		return nil, err
	}
	ev, err := eventFromHTML(buf, modified, pageURL)
	if err != nil {
		return nil, err
	}
	return []events.Event{ev}, nil
}

func (c *Collector) fetch(ctx context.Context, rawURL string) ([]byte, time.Time, error) {
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	req.Header.Set("Accept", "text/html,*/*")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	resp, err := client.Do(req)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, time.Time{}, fmt.Errorf("%s -> %d: %s", rawURL, resp.StatusCode, string(body))
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, time.Time{}, err
	}
	modified := time.Now().UTC()
	if h := strings.TrimSpace(resp.Header.Get("Last-Modified")); h != "" {
		if t, err := http.ParseTime(h); err == nil {
			modified = t.UTC()
		}
	}
	return buf, modified, nil
}

func eventFromHTML(buf []byte, modified time.Time, rawURL string) (events.Event, error) {
	text := htmlText(string(buf))
	price, ok := parsePriceUSDPerBBL(text)
	if !ok {
		return events.Event{}, fmt.Errorf("jet fuel price not found")
	}
	change, direction, hasChange := parseWeekChange(text)
	ts := modified.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	ts = time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
	props := map[string]any{
		"metric":                  "global_average_jet_fuel_price",
		"price_usd_per_bbl":       price,
		"source_api_endpoint":     rawURL,
		"source_terms":            "IATA publishes the monitor under license from S&P Global Energy; verify reuse terms before redistribution.",
		"source_payload_validity": validity(ts),
	}
	if hasChange {
		props["week_over_week_pct"] = change
		props["direction"] = direction
	}
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  "global_average:" + ts.Format("2006-01-02"),
		Lat:    20,
		Lon:    0.1,
		Props:  props,
	}, nil
}

func htmlText(s string) string {
	s = scriptTag.ReplaceAllString(s, " ")
	s = styleTag.ReplaceAllString(s, " ")
	s = tag.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

var (
	scriptTag = regexp.MustCompile(`(?is)<script\b.*?</script>`)
	styleTag  = regexp.MustCompile(`(?is)<style\b.*?</style>`)
	tag       = regexp.MustCompile(`(?s)<[^>]+>`)
	priceRe   = regexp.MustCompile(`(?i)global average jet fuel price.*?\$([0-9]+(?:\.[0-9]+)?)/bbl`)
	anyPrice  = regexp.MustCompile(`(?i)\$([0-9]+(?:\.[0-9]+)?)/bbl`)
	changeRe  = regexp.MustCompile(`(?i)\b(rose|increased|fell|decreased|declined|dropped)\b[^%]*?([0-9]+(?:\.[0-9]+)?)%`)
)

func parsePriceUSDPerBBL(text string) (float64, bool) {
	m := priceRe.FindStringSubmatch(text)
	if len(m) < 2 {
		m = anyPrice.FindStringSubmatch(text)
	}
	if len(m) < 2 {
		return 0, false
	}
	return parseFloat(m[1])
}

func parseWeekChange(text string) (float64, string, bool) {
	m := changeRe.FindStringSubmatch(text)
	if len(m) < 3 {
		return 0, "", false
	}
	v, ok := parseFloat(m[2])
	if !ok {
		return 0, "", false
	}
	direction := strings.ToLower(m[1])
	switch direction {
	case "fell", "decreased", "declined", "dropped":
		v = -math.Abs(v)
		direction = "down"
	case "rose", "increased":
		v = math.Abs(v)
		direction = "up"
	}
	return v, direction, true
}

func parseFloat(s string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), 64)
	return v, err == nil
}

func validity(ts time.Time) map[string]any {
	return map[string]any{
		"valid_start":    ts.Format(time.RFC3339),
		"valid_end":      ts.Add(14 * 24 * time.Hour).Format(time.RFC3339),
		"validity_basis": "iata_fuel_monitor_page_timestamp",
	}
}
