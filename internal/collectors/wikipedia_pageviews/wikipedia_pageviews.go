// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package wikipedia_pageviews ingests deterministic pageview-watch articles.
// It complements wikipedia_surge edit velocity with demand-side attention.
package wikipedia_pageviews

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const apiBase = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

type articleWatch struct {
	Article string
	Label   string
	Lat     float64
	Lon     float64
}

var defaultArticles = []articleWatch{
	{Article: "Ukraine", Label: "Ukraine", Lat: 49, Lon: 32},
	{Article: "Iran", Label: "Iran", Lat: 32, Lon: 53},
	{Article: "Israel", Label: "Israel", Lat: 31, Lon: 35},
	{Article: "Taiwan", Label: "Taiwan", Lat: 24, Lon: 121},
	{Article: "Pakistan", Label: "Pakistan", Lat: 30, Lon: 69},
	{Article: "Sudan", Label: "Sudan", Lat: 16, Lon: 30},
	{Article: "Myanmar", Label: "Myanmar", Lat: 22, Lon: 96},
	{Article: "Venezuela", Label: "Venezuela", Lat: 7, Lon: -67},
	{Article: "Internet_shutdown", Label: "Internet shutdown", Lat: 20, Lon: 0.1},
	{Article: "Coup_d'etat", Label: "Coup d'etat", Lat: 20, Lon: 0.1},
}

type Collector struct {
	articles []articleWatch
}

func New() (*Collector, error) {
	articles := parseArticles(os.Getenv("WIKIPEDIA_PAGEVIEW_ARTICLES"))
	if len(articles) == 0 {
		articles = defaultArticles
	}
	return &Collector{articles: articles}, nil
}

func (c *Collector) ID() string               { return "wikipedia_pageviews" }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	end := now.Add(-24 * time.Hour).Format("2006010200")
	start := now.Add(-21 * 24 * time.Hour).Format("2006010200")
	out := []events.Event{}
	var firstErr error
	for _, watch := range c.articles {
		u := pageviewURL(watch.Article, start, end)
		var body apiResponse
		if err := httpx.GetJSON(ctx, u, map[string]string{
			"Accept":     "application/json",
			"User-Agent": "gordios-osint/1.0",
		}, &body); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ev, ok := eventFromResponse(body, watch, u); ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func pageviewURL(article, start, end string) string {
	return fmt.Sprintf("%s/en.wikipedia/all-access/user/%s/daily/%s/%s", apiBase, url.PathEscape(article), start, end)
}

type apiResponse struct {
	Items []item `json:"items"`
}

type item struct {
	Project     string `json:"project"`
	Article     string `json:"article"`
	Granularity string `json:"granularity"`
	Timestamp   string `json:"timestamp"`
	Access      string `json:"access"`
	Agent       string `json:"agent"`
	Views       int    `json:"views"`
}

func eventFromResponse(body apiResponse, watch articleWatch, sourceURL string) (events.Event, bool) {
	if len(body.Items) < 2 {
		return events.Event{}, false
	}
	sort.Slice(body.Items, func(i, j int) bool { return body.Items[i].Timestamp < body.Items[j].Timestamp })
	latest := body.Items[len(body.Items)-1]
	ts, ok := parseTimestamp(latest.Timestamp)
	if !ok {
		return events.Event{}, false
	}
	base := body.Items[:len(body.Items)-1]
	if len(base) > 14 {
		base = base[len(base)-14:]
	}
	mean, sigma := meanStd(base)
	z := 0.0
	if sigma > 0 {
		z = (float64(latest.Views) - mean) / sigma
	}
	ratio := 1.0
	if mean > 0 {
		ratio = float64(latest.Views) / mean
	}
	score := clamp(math.Log1p(math.Max(0, ratio-1))*1.8+clamp(z/2.5, 0, 2), 0, 4)
	props := map[string]any{
		"wiki":                    "en.wikipedia",
		"title":                   firstNonEmpty(watch.Label, strings.ReplaceAll(latest.Article, "_", " ")),
		"article":                 latest.Article,
		"mode":                    "pageviews",
		"period":                  ts.Format("2006-01-02"),
		"views":                   latest.Views,
		"baseline_views":          round(mean, 1),
		"baseline_samples":        len(base),
		"view_ratio":              round(ratio, 2),
		"z_score":                 round(z, 2),
		"pageview_surge_score":    round(score, 2),
		"url":                     "https://en.wikipedia.org/wiki/" + url.PathEscape(latest.Article),
		"source_api_endpoint":     sourceURL,
		"source_payload_validity": validity(ts, 24*time.Hour, "wikipedia_pageview_day"),
	}
	return events.Event{
		Ts:     ts,
		Source: "wikipedia_pageviews",
		ExtID:  stableID(latest.Article + ":" + latest.Timestamp),
		Lat:    watch.Lat,
		Lon:    watch.Lon,
		Props:  props,
	}, true
}

func parseArticles(raw string) []articleWatch {
	out := []articleWatch{}
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(item, "|")
		if len(parts) != 4 {
			continue
		}
		lat, errLat := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		lon, errLon := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
		if errLat != nil || errLon != nil || math.Abs(lat) > 90 || math.Abs(lon) > 180 || (lat == 0 && lon == 0) {
			continue
		}
		out = append(out, articleWatch{
			Article: strings.TrimSpace(parts[0]),
			Label:   strings.TrimSpace(parts[1]),
			Lat:     lat,
			Lon:     lon,
		})
	}
	return out
}

func parseTimestamp(raw string) (time.Time, bool) {
	t, err := time.Parse("2006010200", strings.TrimSpace(raw))
	if err == nil {
		return t.UTC(), true
	}
	t, err = time.Parse("20060102", strings.TrimSpace(raw))
	return t.UTC(), err == nil
}

func meanStd(items []item) (float64, float64) {
	if len(items) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, it := range items {
		sum += float64(it.Views)
	}
	mean := sum / float64(len(items))
	if len(items) < 2 {
		return mean, 0
	}
	variance := 0.0
	for _, it := range items {
		d := float64(it.Views) - mean
		variance += d * d
	}
	return mean, math.Sqrt(variance / float64(len(items)-1))
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

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(strings.ToLower(s))))
	return "wiki-pageviews:" + hex.EncodeToString(h[:])
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}
