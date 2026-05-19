// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package asean_haze_hotspots ingests official ASMC VIIRS daily hotspot counts
// for Southeast Asia haze and fire monitoring.
package asean_haze_hotspots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
)

const (
	sourceID   = "asean_haze_hotspots"
	defaultURL = "https://asmc.asean.org/wp-content/themes/asmctheme/page-functions/functions-ajax-haze-daily-hotspot-count-new.php"
)

type Collector struct {
	client     *http.Client
	endpoint   string
	regions    []string
	pastDays   int
	dayNight   string
	confidence string
}

func New() (*Collector, error) {
	regions := collectorutil.SplitCSV(os.Getenv("ASMC_HAZE_REGIONS"))
	if len(regions) == 0 {
		regions = []string{"Thailand", "Myanmar", "Cambodia", "Vietnam", "Philippines", "P_Malaysia", "SabahSarawak", "Sumatra", "Kalimantan", "Singapore"}
	}
	endpoint := strings.TrimSpace(os.Getenv("ASMC_HAZE_HOTSPOTS_URL"))
	if endpoint == "" {
		endpoint = defaultURL
	}
	return &Collector{
		client:     collectorutil.HTTPClient(45 * time.Second),
		endpoint:   endpoint,
		regions:    regions,
		pastDays:   collectorutil.EnvInt("ASMC_HAZE_PAST_DAYS", 7, 1, 90),
		dayNight:   firstNonEmpty(os.Getenv("ASMC_HAZE_DAYNIGHT"), "day"),
		confidence: firstNonEmpty(os.Getenv("ASMC_HAZE_CONFIDENCE"), "High"),
	}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("ASMC_HAZE_POLL_EVERY_S", 6*3600, 300, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, batch := range batches(c.regions, 5) {
		body, err := c.postBatch(ctx, batch)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		evs, err := eventsFromASMC(c.endpoint, body, batch, c.dayNight, c.confidence)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (c *Collector) postBatch(ctx context.Context, regions []string) ([]byte, error) {
	form := url.Values{}
	form.Set("date", singaporeDate(time.Now()))
	form.Set("pastDays", strconv.Itoa(c.pastDays))
	form.Set("daynight", c.dayNight)
	form.Set("conf", c.confidence)
	for _, region := range regions {
		form.Add("regions[]", region)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json,text/plain,*/*")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("Referer", "https://asmc.asean.org/asmc-haze-hotspot-daily-new/")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("asmc hotspot post: %d %s", resp.StatusCode, string(buf))
	}
	return io.ReadAll(resp.Body)
}

func eventsFromASMC(endpoint string, body []byte, requestedRegions []string, dayNight, confidence string) ([]events.Event, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil {
		return nil, err
	}
	requested := map[string]struct{}{}
	for _, region := range requestedRegions {
		requested[region] = struct{}{}
	}
	out := []events.Event{}
	for _, row := range rows {
		date := strings.TrimSpace(fmt.Sprint(row["date"]))
		ts := parseDate(date)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		for region, value := range row {
			if region == "date" || strings.HasSuffix(region, "LineColor") {
				continue
			}
			if _, ok := requested[region]; !ok {
				continue
			}
			count, ok := numberValue(value)
			if !ok {
				continue
			}
			meta, ok := regionMeta[region]
			if !ok {
				continue
			}
			props := map[string]any{
				"source_provider":     "ASEAN Specialized Meteorological Centre",
				"source_api_endpoint": endpoint,
				"source_url":          "https://asmc.asean.org/asmc-haze-hotspot-daily-new/",
				"region":              region,
				"country":             meta.Country,
				"country_code":        meta.CountryCode,
				"date":                date,
				"hotspot_count":       count,
				"daynight":            dayNight,
				"confidence":          confidence,
				"hazard_type":         "fire_hotspot",
			}
			out = append(out, events.Event{
				Ts:     ts,
				Source: sourceID,
				ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s|%s", region, date, dayNight, confidence)),
				Lat:    meta.Lat,
				Lon:    meta.Lon,
				Props:  props,
			})
		}
	}
	return out, nil
}

type regionInfo struct {
	Country     string
	CountryCode string
	Lat         float64
	Lon         float64
}

var regionMeta = map[string]regionInfo{
	"Thailand":     {"Thailand", "TH", 15.8700, 100.9925},
	"Myanmar":      {"Myanmar", "MM", 21.9162, 95.9560},
	"Cambodia":     {"Cambodia", "KH", 12.5657, 104.9910},
	"Vietnam":      {"Viet Nam", "VN", 14.0583, 108.2772},
	"Philippines":  {"Philippines", "PH", 12.8797, 121.7740},
	"P_Malaysia":   {"Malaysia", "MY", 4.2105, 101.9758},
	"SabahSarawak": {"Malaysia", "MY", 2.5000, 113.5000},
	"Sumatra":      {"Indonesia", "ID", -0.5897, 101.3431},
	"Kalimantan":   {"Indonesia", "ID", -0.9619, 113.9043},
	"Singapore":    {"Singapore", "SG", 1.3521, 103.8198},
}

func batches(in []string, size int) [][]string {
	if size <= 0 {
		return nil
	}
	out := [][]string{}
	for len(in) > 0 {
		n := size
		if len(in) < n {
			n = len(in)
		}
		out = append(out, in[:n])
		in = in[n:]
	}
	return out
}

func numberValue(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	case int:
		return float64(x), true
	default:
		return 0, false
	}
}

func singaporeDate(t time.Time) string {
	loc := time.FixedZone("SGT", 8*3600)
	return t.In(loc).Format("2 Jan, 2006")
}

func parseDate(raw string) time.Time {
	loc := time.FixedZone("SGT", 8*3600)
	if t, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(raw), loc); err == nil {
		return t.UTC()
	}
	return time.Time{}
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
