// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package wfp_food_prices ingests the WFP global food price CSV published on
// HDX. It emits bounded market stress observations rather than every price row.
package wfp_food_prices

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID     = "wfp_food_prices"
	packageURL   = "https://data.humdata.org/api/3/action/package_show?id=global-wfp-food-prices"
	fallbackCSV  = "https://data.humdata.org/dataset/31579af5-3895-4002-9ee3-c50857480785/resource/502190c6-0d3d-4b84-977e-ef062f053662/download/wfp_food_prices_global_2026.csv"
	userAgent    = "gordios/0.1"
	defaultLimit = 300
)

type Collector struct {
	maxEvents int
	client    *http.Client
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_WFP_FOOD_PRICES") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_WFP_FOOD_PRICES=1")
	}
	return &Collector{
		maxEvents: collectorutil.EnvInt("WFP_FOOD_PRICE_MAX_EVENTS", defaultLimit, 20, 2000),
		client:    collectorutil.HTTPClient(120 * time.Second),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 12 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	csvURL, resourceName, err := latestCSVURL(ctx)
	if err != nil {
		csvURL = fallbackCSV
		resourceName = "WFP global food prices CSV fallback"
	}
	buf, err := getBytes(ctx, c.client, csvURL)
	if err != nil {
		return nil, err
	}
	rows, err := parseCSV(buf, c.maxEvents, csvURL, resourceName)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type hdxPackage struct {
	Success bool `json:"success"`
	Result  struct {
		Resources []struct {
			Name         string `json:"name"`
			URL          string `json:"url"`
			Format       string `json:"format"`
			LastModified string `json:"last_modified"`
		} `json:"resources"`
	} `json:"result"`
}

func latestCSVURL(ctx context.Context) (string, string, error) {
	var pkg hdxPackage
	if err := httpx.GetJSON(ctx, packageURL, map[string]string{"Accept": "application/json"}, &pkg); err != nil {
		return "", "", err
	}
	type candidate struct {
		url  string
		name string
		year int
	}
	nowYear := time.Now().UTC().Year()
	cands := []candidate{}
	for _, r := range pkg.Result.Resources {
		u := strings.TrimSpace(r.URL)
		name := strings.TrimSpace(r.Name)
		if u == "" || !strings.Contains(strings.ToLower(u), ".csv") {
			continue
		}
		year := yearFromText(u + " " + name)
		if year == 0 {
			continue
		}
		if year <= nowYear+1 {
			cands = append(cands, candidate{url: u, name: name, year: year})
		}
	}
	if len(cands) == 0 {
		return "", "", fmt.Errorf("no WFP food price CSV resource in HDX package")
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].year > cands[j].year })
	return cands[0].url, cands[0].name, nil
}

func yearFromText(s string) int {
	for y := time.Now().UTC().Year() + 1; y >= 2010; y-- {
		if strings.Contains(s, strconv.Itoa(y)) {
			return y
		}
	}
	return 0
}

func getBytes(ctx context.Context, client *http.Client, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		if buf, curlErr := curlGetBytes(ctx, rawURL); curlErr == nil {
			return buf, nil
		}
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("%s returned %d: %s", rawURL, resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

func curlGetBytes(ctx context.Context, rawURL string) ([]byte, error) {
	return exec.CommandContext(ctx, "curl", "-fsSL", "--connect-timeout", "30", "--max-time", "120", "-A", userAgent, rawURL).Output()
}

type pricePoint struct {
	CountryISO3 string
	Date        time.Time
	Admin1      string
	Admin2      string
	Market      string
	MarketID    string
	Lat         float64
	Lon         float64
	Category    string
	Commodity   string
	CommodityID string
	Unit        string
	PriceType   string
	Currency    string
	Price       float64
	USDPrice    float64
}

type priceSeries struct {
	Key      string
	Earliest pricePoint
	Latest   pricePoint
}

func parseCSV(buf []byte, limit int, csvURL, resourceName string) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := headerIndex(header)
	series := map[string]*priceSeries{}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		p, ok := pricePointFromRow(row, idx)
		if !ok || p.Date.Year() != time.Now().UTC().Year() {
			continue
		}
		key := strings.Join([]string{p.MarketID, p.CommodityID, strings.ToLower(p.PriceType)}, "|")
		if key == "||" {
			key = collectorutil.StableID(p.CountryISO3 + p.Market + p.Commodity + p.PriceType)
		}
		ps := series[key]
		if ps == nil {
			ps = &priceSeries{Key: key, Earliest: p, Latest: p}
			series[key] = ps
			continue
		}
		if p.Date.Before(ps.Earliest.Date) {
			ps.Earliest = p
		}
		if p.Date.After(ps.Latest.Date) || p.Date.Equal(ps.Latest.Date) {
			ps.Latest = p
		}
	}
	evs := make([]events.Event, 0, len(series))
	for _, ps := range series {
		if ev, ok := eventFromSeries(ps, csvURL, resourceName); ok {
			evs = append(evs, ev)
		}
	}
	sort.SliceStable(evs, func(i, j int) bool {
		ai, _ := propx.Float(evs[i].Props["food_price_stress_score"])
		aj, _ := propx.Float(evs[j].Props["food_price_stress_score"])
		return ai > aj
	})
	if len(evs) > limit {
		evs = evs[:limit]
	}
	return evs, nil
}

func headerIndex(header []string) map[string]int {
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	return idx
}

func pricePointFromRow(row []string, idx map[string]int) (pricePoint, bool) {
	lat, latOK := parseFloat(cell(row, idx, "latitude"))
	lon, lonOK := parseFloat(cell(row, idx, "longitude"))
	if !latOK || !lonOK || !collectorutil.ValidLatLon(lat, lon) {
		return pricePoint{}, false
	}
	date := parseDate(cell(row, idx, "date"))
	if date.IsZero() {
		return pricePoint{}, false
	}
	price, priceOK := parseFloat(cell(row, idx, "price"))
	usd, usdOK := parseFloat(cell(row, idx, "usdprice"))
	if !usdOK {
		usd = price
	}
	if !priceOK && !usdOK {
		return pricePoint{}, false
	}
	return pricePoint{
		CountryISO3: strings.ToUpper(cell(row, idx, "countryiso3")),
		Date:        date,
		Admin1:      cell(row, idx, "admin1"),
		Admin2:      cell(row, idx, "admin2"),
		Market:      cell(row, idx, "market"),
		MarketID:    cell(row, idx, "market_id"),
		Lat:         lat,
		Lon:         lon,
		Category:    cell(row, idx, "category"),
		Commodity:   cell(row, idx, "commodity"),
		CommodityID: cell(row, idx, "commodity_id"),
		Unit:        cell(row, idx, "unit"),
		PriceType:   cell(row, idx, "pricetype"),
		Currency:    cell(row, idx, "currency"),
		Price:       price,
		USDPrice:    usd,
	}, true
}

func eventFromSeries(ps *priceSeries, csvURL, resourceName string) (events.Event, bool) {
	if ps == nil || ps.Latest.USDPrice <= 0 || ps.Earliest.USDPrice <= 0 || ps.Latest.Date.Equal(ps.Earliest.Date) {
		return events.Event{}, false
	}
	pct := (ps.Latest.USDPrice - ps.Earliest.USDPrice) / ps.Earliest.USDPrice * 100.0
	score := foodPriceStressScore(pct)
	if score < 0.6 {
		return events.Event{}, false
	}
	p := ps.Latest
	props := map[string]any{
		"source_provider":            "World Food Programme / HDX",
		"source_api_endpoint":        csvURL,
		"source_hdx_package":         packageURL,
		"source_provider_kind":       "public_food_price_monitor",
		"resource_name":              resourceName,
		"country_iso3":               p.CountryISO3,
		"admin1":                     p.Admin1,
		"admin2":                     p.Admin2,
		"market":                     p.Market,
		"market_id":                  p.MarketID,
		"category":                   p.Category,
		"commodity":                  p.Commodity,
		"commodity_id":               p.CommodityID,
		"unit":                       p.Unit,
		"price_type":                 p.PriceType,
		"currency":                   p.Currency,
		"latest_price":               round(p.Price, 3),
		"latest_usd_price":           round(p.USDPrice, 3),
		"baseline_usd_price":         round(ps.Earliest.USDPrice, 3),
		"baseline_date":              ps.Earliest.Date.Format("2006-01-02"),
		"latest_date":                p.Date.Format("2006-01-02"),
		"food_price_pct_change_ytd":  round(pct, 1),
		"food_price_stress_score":    round(score, 2),
		"food_market_stress_context": true,
		"source_payload_validity":    validity(p.Date, p.Date.Add(45*24*time.Hour), "latest_wfp_market_price_window"),
	}
	return events.Event{
		Ts:     p.Date,
		Source: sourceID,
		ExtID:  "market-price:" + collectorutil.StableID(ps.Key+":"+p.Date.Format("2006-01-02")),
		Lat:    p.Lat,
		Lon:    p.Lon,
		Props:  props,
	}, true
}

func foodPriceStressScore(pct float64) float64 {
	if pct <= 0 {
		return 0
	}
	return propx.ClampFloat((pct-15.0)/25.0+0.5, 0, 3.0)
}

func cell(row []string, idx map[string]int, key string) string {
	i, ok := idx[key]
	if !ok || i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func parseFloat(s string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f, err == nil
}

func parseDate(s string) time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}
