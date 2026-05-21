// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package deepstate_frontlines ingests the current DeepStateMap Ukraine
// front-line GeoJSON snapshot as a replace-all feature layer.
package deepstate_frontlines

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const defaultEndpoint = "https://deepstatemap.live/api/history/last"

type Collector struct {
	endpoint string
	client   *http.Client
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_DEEPSTATE_FRONTLINES") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_DEEPSTATE_FRONTLINES=1")
	}
	endpoint := strings.TrimSpace(os.Getenv("DEEPSTATE_FRONTLINES_URL"))
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	return &Collector{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "deepstate_frontlines" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type snapshot struct {
	ID       any               `json:"id"`
	Datetime string            `json:"datetime"`
	Map      featureCollection `json:"map"`
}

type featureCollection struct {
	Type     string           `json:"type"`
	Features []geoJSONFeature `json:"features"`
}

type geoJSONFeature struct {
	Type       string         `json:"type"`
	Geometry   geoJSONGeom    `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

type geoJSONGeom struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

func (c *Collector) FetchFeatures(ctx context.Context) ([]features.Feature, error) {
	var raw snapshot
	headers := map[string]string{"Accept": "application/json"}
	if err := httpx.GetJSONWithClient(ctx, c.client, c.endpoint, headers, &raw); err != nil {
		return nil, err
	}
	return parseSnapshot(raw, c.endpoint, time.Now().UTC())
}

func parseSnapshot(raw snapshot, endpoint string, seen time.Time) ([]features.Feature, error) {
	if len(raw.Map.Features) == 0 {
		return nil, fmt.Errorf("deepstate snapshot has no features")
	}
	historyID := strings.TrimSpace(fmt.Sprint(raw.ID))
	out := make([]features.Feature, 0, len(raw.Map.Features))
	for i, f := range raw.Map.Features {
		wkt, err := geometryWKT(f.Geometry)
		if err != nil {
			continue
		}
		props := map[string]any{
			"source_provider":     "DeepStateMap",
			"source_kind":         "frontline_snapshot",
			"source_api_endpoint": endpoint,
			"source_public_url":   "https://deepstatemap.live/",
			"history_id":          historyID,
			"snapshot_datetime":   strings.TrimSpace(raw.Datetime),
			"geometry_type":       f.Geometry.Type,
			"feature_index":       i,
			"last_seen_at":        seen.Format(time.RFC3339),
		}
		for k, v := range f.Properties {
			props[k] = v
		}
		name := propx.FirstNonEmpty(propx.StringAt(f.Properties, "name"), fmt.Sprintf("DeepState feature %d", i))
		props["name"] = name
		extID := fmt.Sprintf("%s:%d:%s", propx.FirstNonEmpty(historyID, "latest"), i, stableFeatureKey(f))
		out = append(out, features.Feature{
			ExtID:   extID,
			GeomWKT: wkt,
			Props:   props,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("deepstate snapshot had %d features but none with supported geometry", len(raw.Map.Features))
	}
	return out, nil
}

func stableFeatureKey(f geoJSONFeature) string {
	name := propx.StringAt(f.Properties, "name")
	fill := propx.StringAt(f.Properties, "fill")
	stroke := propx.StringAt(f.Properties, "stroke")
	return strconv.FormatUint(fnv64(strings.Join([]string{f.Geometry.Type, name, fill, stroke, string(f.Geometry.Coordinates)}, "|")), 36)
}

func fnv64(s string) uint64 {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

func geometryWKT(g geoJSONGeom) (string, error) {
	var v any
	if err := json.Unmarshal(g.Coordinates, &v); err != nil {
		return "", err
	}
	switch strings.ToLower(strings.TrimSpace(g.Type)) {
	case "point":
		p, ok := positionWKT(v)
		if !ok {
			return "", fmt.Errorf("bad point")
		}
		return "POINT(" + p + ")", nil
	case "linestring":
		line, ok := lineWKT(v)
		if !ok {
			return "", fmt.Errorf("bad linestring")
		}
		return "LINESTRING(" + line + ")", nil
	case "polygon":
		poly, ok := polygonWKT(v)
		if !ok {
			return "", fmt.Errorf("bad polygon")
		}
		return "POLYGON(" + poly + ")", nil
	case "multipolygon":
		mp, ok := multiPolygonWKT(v)
		if !ok {
			return "", fmt.Errorf("bad multipolygon")
		}
		return "MULTIPOLYGON(" + mp + ")", nil
	default:
		return "", fmt.Errorf("unsupported geometry type %q", g.Type)
	}
}

func positionWKT(v any) (string, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 {
		return "", false
	}
	lon, ok1 := number(arr[0])
	lat, ok2 := number(arr[1])
	if !ok1 || !ok2 || !validLatLon(lat, lon) {
		return "", false
	}
	return fmtCoord(lon) + " " + fmtCoord(lat), true
}

func lineWKT(v any) (string, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 {
		return "", false
	}
	parts := make([]string, 0, len(arr))
	for _, item := range arr {
		p, ok := positionWKT(item)
		if !ok {
			return "", false
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ","), true
}

func polygonWKT(v any) (string, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", false
	}
	rings := make([]string, 0, len(arr))
	for _, ring := range arr {
		line, ok := lineWKT(ring)
		if !ok {
			return "", false
		}
		rings = append(rings, "("+line+")")
	}
	return strings.Join(rings, ","), true
}

func multiPolygonWKT(v any) (string, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return "", false
	}
	polys := make([]string, 0, len(arr))
	for _, poly := range arr {
		wkt, ok := polygonWKT(poly)
		if !ok {
			return "", false
		}
		polys = append(polys, "("+wkt+")")
	}
	return strings.Join(polys, ","), true
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func fmtCoord(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}
