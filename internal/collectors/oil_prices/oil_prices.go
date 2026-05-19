// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Oil prices / market stress — stooq.com single-day CSV snapshots for energy,
// precious metals, and selected FX symbols. Polled frequently (15 min) so a
// small historical series accumulates in the hypertable; /api/latest returns
// the newest row per symbol.
package oil_prices

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

// Front-month futures/FX used by the dashboard and market-stress context.
var defaultSymbols = []string{"cb.f", "cl.f", "gc.f", "si.f", "usdils", "usdrub"}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "oil_prices" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	symbols := configuredSymbols()
	out := make([]events.Event, 0, len(symbols))
	for _, sym := range symbols {
		row, err := fetchOne(ctx, sym)
		if err != nil {
			// Skip this symbol; others may still succeed.
			continue
		}
		out = append(out, *row)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no symbols fetched")
	}
	return out, nil
}

func fetchOne(ctx context.Context, sym string) (*events.Event, error) {
	url := fmt.Sprintf("https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv", sym)
	buf, err := httpx.GetBytes(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(buf))
	_, err = r.Read() // header
	if err != nil {
		return nil, err
	}
	row, err := r.Read()
	if err != nil {
		return nil, err
	}
	// Symbol,Date,Time,Open,High,Low,Close,Volume
	if len(row) < 8 || row[1] == "N/D" {
		return nil, fmt.Errorf("no data for %s", sym)
	}
	date := strings.TrimSpace(row[1])
	clock := strings.TrimSpace(row[2])
	open, _ := strconv.ParseFloat(row[3], 64)
	high, _ := strconv.ParseFloat(row[4], 64)
	low, _ := strconv.ParseFloat(row[5], 64)
	close, _ := strconv.ParseFloat(row[6], 64)
	vol, _ := strconv.ParseFloat(row[7], 64)

	ts := time.Now().UTC()
	if t, err := time.Parse("2006-01-02 15:04:05", date+" "+clock); err == nil {
		ts = t.UTC()
	} else if t, err := time.Parse("2006-01-02", date); err == nil {
		ts = t.UTC()
	}
	pct := 0.0
	if open > 0 {
		pct = (close - open) / open * 100
	}
	stress := marketStressScore(sym, pct, high, low, close)
	props := map[string]any{
		"symbol":                  sym,
		"asset_class":             assetClass(sym),
		"date":                    date,
		"time":                    clock,
		"open":                    open,
		"high":                    high,
		"low":                     low,
		"close":                   close,
		"volume":                  vol,
		"intraday_pct_change":     round(pct, 3),
		"market_stress_score":     round(stress, 2),
		"source_api_endpoint":     url,
		"source_payload_validity": validity(ts, 30*time.Minute, "stooq_snapshot_time"),
	}
	return &events.Event{
		Ts: ts, Source: "oil_prices", ExtID: sym,
		Lat: 20, Lon: 0.1, Props: props,
	}, nil
}

func configuredSymbols() []string {
	raw := strings.TrimSpace(os.Getenv("MARKET_STRESS_SYMBOLS"))
	if raw == "" {
		return defaultSymbols
	}
	out := []string{}
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		sym := strings.ToLower(strings.TrimSpace(part))
		if sym == "" || seen[sym] {
			continue
		}
		seen[sym] = true
		out = append(out, sym)
	}
	return out
}

func assetClass(sym string) string {
	switch strings.ToLower(strings.TrimSpace(sym)) {
	case "cb.f", "cl.f":
		return "energy"
	case "gc.f", "si.f", "xauusd":
		return "precious_metals"
	case "usdils", "usdrub":
		return "fx"
	default:
		return "market"
	}
}

func marketStressScore(sym string, pct, high, low, close float64) float64 {
	score := math.Abs(pct) / 1.8
	if close > 0 && high > low {
		score += ((high - low) / close * 100) / 4.0
	}
	switch assetClass(sym) {
	case "energy", "fx":
		score *= 1.1
	}
	return math.Min(score, 4)
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}
