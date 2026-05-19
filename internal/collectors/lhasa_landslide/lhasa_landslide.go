// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package lhasa_landslide samples NASA LHASA exposure polygons around active
// AOIs. This is static/susceptibility context for landslide-prone terrain, not
// a current landslide detection by itself.
package lhasa_landslide

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceName = "lhasa_landslide"
	endpoint   = "https://gis.earthdata.nasa.gov/gis05/rest/services/Landslides/LHASA_Exposure/MapServer/0/query"
	docsURL    = "https://gis.earthdata.nasa.gov/gis05/rest/services/Landslides/LHASA_Exposure/FeatureServer/layers"
)

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_LHASA_LANDSLIDE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_LHASA_LANDSLIDE=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:    pool,
		maxAOIs: collectorutil.EnvInt("LHASA_MAX_AOIS", 16, 1, 60),
	}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 12 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 7*24*time.Hour, collectorutil.StrategicAOIs)
	out := []events.Event{}
	var firstErr error
	failed := 0
	for _, aoi := range aois {
		rows, err := queryAOI(ctx, aoi)
		if err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, f := range rows {
			if ev, ok := eventFromFeature(aoi, f); ok {
				out = append(out, ev)
			}
		}
	}
	if len(out) == 0 && firstErr != nil {
		return []events.Event{statusEvent(aois, failed, firstErr)}, nil
	}
	return out, nil
}

type featureCollection struct {
	Features []feature `json:"features"`
}

type feature struct {
	ID         any            `json:"id"`
	Properties map[string]any `json:"properties"`
}

func queryAOI(ctx context.Context, aoi collectorutil.AOI) ([]feature, error) {
	pad := 1.0
	q := url.Values{}
	q.Set("f", "geojson")
	q.Set("where", "1=1")
	q.Set("outFields", "objectid,l_haz,m_haz,h_haz,l_haz_pp_f,m_haz_pp_f,h_haz_pp_f,name_0,name_2,gid_2")
	q.Set("returnGeometry", "false")
	q.Set("resultRecordCount", "5")
	q.Set("geometryType", "esriGeometryEnvelope")
	q.Set("spatialRel", "esriSpatialRelIntersects")
	q.Set("inSR", "4326")
	q.Set("geometry", fmt.Sprintf("%.5f,%.5f,%.5f,%.5f", aoi.Lon-pad, aoi.Lat-pad, aoi.Lon+pad, aoi.Lat+pad))
	var fc featureCollection
	if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/geo+json,application/json"}, &fc); err != nil {
		return nil, err
	}
	return fc.Features, nil
}

func eventFromFeature(aoi collectorutil.AOI, f feature) (events.Event, bool) {
	p := f.Properties
	if len(p) == 0 {
		return events.Event{}, false
	}
	objectID, _ := propx.Int(p["objectid"])
	score := hazardScore(p)
	if score <= 0 {
		return events.Event{}, false
	}
	now := time.Now().UTC().Truncate(24 * time.Hour)
	props := map[string]any{
		"watch_aoi_id":               aoi.ID,
		"watch_aoi_kind":             aoi.Kind,
		"watch_aoi_label":            aoi.Label,
		"objectid":                   objectID,
		"country":                    propx.StringAt(p, "name_0"),
		"admin2":                     propx.StringAt(p, "name_2"),
		"gid_2":                      propx.StringAt(p, "gid_2"),
		"l_haz":                      p["l_haz"],
		"m_haz":                      p["m_haz"],
		"h_haz":                      p["h_haz"],
		"m_haz_pp_f":                 p["m_haz_pp_f"],
		"h_haz_pp_f":                 p["h_haz_pp_f"],
		"landslide_hazard_score":     collectorutil.Round(score, 2),
		"landslide_exposure_context": true,
		"source_api_endpoint":        endpoint,
		"docs_url":                   docsURL,
	}
	return events.Event{
		Ts:     now,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%d", collectorutil.StableID(aoi.ID), objectID),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Props:  props,
	}, true
}

func statusEvent(aois []collectorutil.AOI, failed int, err error) events.Event {
	now := time.Now().UTC().Truncate(time.Hour)
	lat, lon := 0.0, 0.0
	if len(aois) > 0 {
		lat, lon = aois[0].Lat, aois[0].Lon
	}
	props := map[string]any{
		"upstream_degraded":    true,
		"sampled_aoi_count":    len(aois),
		"failed_aoi_count":     failed,
		"first_error":          err.Error(),
		"source_api_endpoint":  endpoint,
		"docs_url":             docsURL,
		"context_only":         true,
		"landslide_hazard_set": false,
	}
	return events.Event{
		Ts:     now,
		Source: sourceName,
		ExtID:  "collector-status:" + now.Format("20060102T15"),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}
}

func hazardScore(p map[string]any) float64 {
	hhaz, _ := propx.Float(p["h_haz_pp_f"])
	mhaz, _ := propx.Float(p["m_haz_pp_f"])
	lhaz, _ := propx.Float(p["l_haz_pp_f"])
	score := 0.0
	if hhaz > 0 {
		score = max(score, 1.5+hhaz*6)
	}
	if mhaz > 0 {
		score = max(score, 0.8+mhaz*4)
	}
	if lhaz > 0 {
		score = max(score, lhaz*2)
	}
	if h, ok := propx.Int(p["h_haz"]); ok && h > 0 {
		score = max(score, 1.5)
	}
	if m, ok := propx.Int(p["m_haz"]); ok && m > 0 {
		score = max(score, 0.8)
	}
	if score > 3 {
		return 3
	}
	return score
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
