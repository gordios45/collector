// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package opera_dist tracks OPERA DIST-ALERT-HLS granule availability over
// active AOIs through NASA CMR. It does not download protected rasters; rows
// indicate near-real-time disturbance product coverage that can drive a later
// pixel sampler.
package opera_dist

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceName = "opera_dist_alert"
	cmrURL     = "https://cmr.earthdata.nasa.gov/search/granules.json"
	shortName  = "OPERA_L3_DIST-ALERT-HLS_V1"
	docsURL    = "https://www.earthdata.nasa.gov/about/nasa-support-snwg/solutions/opera-near-global-surface-dist"
)

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
	days    int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_OPERA_DIST") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_OPERA_DIST=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:    pool,
		maxAOIs: collectorutil.EnvInt("OPERA_DIST_MAX_AOIS", 12, 1, 50),
		days:    collectorutil.EnvInt("OPERA_DIST_LOOKBACK_DAYS", 7, 1, 21),
	}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 7*24*time.Hour, collectorutil.StrategicAOIs)
	out := []events.Event{}
	var firstErr error
	for _, aoi := range aois {
		rows, err := queryAOI(ctx, aoi, c.days)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, entry := range rows {
			if ev, ok := eventForGranule(aoi, entry); ok {
				out = append(out, ev)
			}
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type granuleResp struct {
	Feed struct {
		Updated string    `json:"updated"`
		Entry   []granule `json:"entry"`
	} `json:"feed"`
}

type granule struct {
	ID                string     `json:"id"`
	Title             string     `json:"title"`
	ProducerGranuleID string     `json:"producer_granule_id"`
	TimeStart         string     `json:"time_start"`
	TimeEnd           string     `json:"time_end"`
	CloudCover        string     `json:"cloud_cover"`
	DataCenter        string     `json:"data_center"`
	CollectionID      string     `json:"collection_concept_id"`
	OnlineAccess      bool       `json:"online_access_flag"`
	Polygons          [][]string `json:"polygons"`
	Links             []link     `json:"links"`
}

type link struct {
	Title string `json:"title"`
	Href  string `json:"href"`
	Rel   string `json:"rel"`
}

func queryAOI(ctx context.Context, aoi collectorutil.AOI, days int) ([]granule, error) {
	end := time.Now().UTC()
	start := end.AddDate(0, 0, -days)
	q := url.Values{}
	q.Set("short_name", shortName)
	q.Set("page_size", "3")
	q.Set("sort_key", "-start_date")
	q.Set("point", fmt.Sprintf("%.5f,%.5f", aoi.Lon, aoi.Lat))
	q.Set("temporal", start.Format(time.RFC3339)+","+end.Format(time.RFC3339))
	var r granuleResp
	if err := httpx.GetJSON(ctx, cmrURL+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &r); err != nil {
		return nil, err
	}
	return r.Feed.Entry, nil
}

func eventForGranule(aoi collectorutil.AOI, g granule) (events.Event, bool) {
	if g.ID == "" && g.ProducerGranuleID == "" {
		return events.Event{}, false
	}
	ts := parseTime(g.TimeStart)
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	cloud, _ := strconv.ParseFloat(strings.TrimSpace(g.CloudCover), 64)
	score := 0.6
	if g.OnlineAccess {
		score += 0.2
	}
	if g.CloudCover != "" && cloud <= 20 {
		score += 0.3
	}
	props := map[string]any{
		"watch_aoi_id":                        aoi.ID,
		"watch_aoi_kind":                      aoi.Kind,
		"watch_aoi_label":                     aoi.Label,
		"granule_id":                          firstNonEmpty(g.ID, g.ProducerGranuleID),
		"producer_granule_id":                 g.ProducerGranuleID,
		"title":                               g.Title,
		"short_name":                          shortName,
		"collection_concept_id":               g.CollectionID,
		"data_center":                         g.DataCenter,
		"time_start":                          g.TimeStart,
		"time_end":                            g.TimeEnd,
		"cloud_cover":                         g.CloudCover,
		"online_access_flag":                  g.OnlineAccess,
		"disturbance_product_available":       true,
		"disturbance_product_available_score": collectorutil.Round(score, 2),
		"source_api_endpoint":                 cmrURL,
		"docs_url":                            docsURL,
		"sampling_state":                      "catalog_coverage_only_pending_pixel_sampling",
	}
	if href := firstDataHref(g.Links); href != "" {
		props["sample_data_href"] = href
	}
	return events.Event{
		Ts:     ts,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s", collectorutil.StableID(aoi.ID), firstNonEmpty(g.ID, g.ProducerGranuleID)),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Props:  props,
	}, true
}

func firstDataHref(links []link) string {
	for _, l := range links {
		if strings.Contains(l.Rel, "/data#") && strings.TrimSpace(l.Href) != "" {
			return l.Href
		}
	}
	return ""
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
