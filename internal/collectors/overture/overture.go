// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Overture Maps AOI context collector.
//
// Overture's authoritative distribution is monthly cloud-hosted GeoParquet.
// The server does not carry a Parquet execution engine, so this collector
// ingests small AOI exports produced by Overture tooling/Explorer as GeoJSON
// and stores them as static features. These context features are used only by
// proximity joins; they do not create signal observations on their own.
package overture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const featureSource = "overture_maps_context"

type Collector struct {
	pool    *pgxpool.Pool
	inputs  []input
	release string
	limit   int
	client  *http.Client
}

type input struct {
	Theme string
	URL   string
	Path  string
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	inputs := parseInputs(os.Getenv("OVERTURE_MAPS_GEOJSON_URLS"), true)
	inputs = append(inputs, parseInputs(os.Getenv("OVERTURE_MAPS_GEOJSON_FILES"), false)...)
	if len(inputs) == 0 {
		return nil, fmt.Errorf("set OVERTURE_MAPS_GEOJSON_URLS or OVERTURE_MAPS_GEOJSON_FILES to AOI GeoJSON exports")
	}
	return &Collector{
		pool:    pool,
		inputs:  inputs,
		release: propx.FirstNonEmpty(os.Getenv("OVERTURE_MAPS_RELEASE"), "configured_aoi_export"),
		limit:   envInt("OVERTURE_MAPS_CONTEXT_LIMIT", 10000),
		client:  &http.Client{Timeout: 45 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return featureSource }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var all []features.Feature
	for _, in := range c.inputs {
		body, err := c.readInput(ctx, in)
		if err != nil {
			return nil, err
		}
		feats, err := parseGeoJSONFeatures(body, in.Theme, c.release, c.limit-len(all))
		if err != nil {
			return nil, err
		}
		all = append(all, feats...)
		if c.limit > 0 && len(all) >= c.limit {
			break
		}
	}
	if _, err := features.Upsert(ctx, c.pool, featureSource, all); err != nil {
		return nil, err
	}
	return nil, nil
}

func (c *Collector) readInput(ctx context.Context, in input) ([]byte, error) {
	if in.URL != "" {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, in.URL, nil)
		req.Header.Set("Accept", "application/geo+json, application/json")
		req.Header.Set("User-Agent", "gordios/0.1")
		r, err := c.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<20))
		if r.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("overture %s %d: %s", in.URL, r.StatusCode, string(body[:min(len(body), 400)]))
		}
		return body, nil
	}
	body, err := os.ReadFile(in.Path)
	if err != nil {
		return nil, err
	}
	return body, nil
}

type featureCollection struct {
	Type     string       `json:"type"`
	Features []geoFeature `json:"features"`
}

type geoFeature struct {
	ID         any            `json:"id"`
	Type       string         `json:"type"`
	Geometry   geoGeometry    `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

type geoGeometry struct {
	Type        string `json:"type"`
	Coordinates any    `json:"coordinates"`
}

func parseGeoJSONFeatures(body []byte, theme, release string, limit int) ([]features.Feature, error) {
	var fc featureCollection
	if err := json.Unmarshal(body, &fc); err != nil {
		return nil, fmt.Errorf("parse overture geojson: %w", err)
	}
	out := make([]features.Feature, 0, min(len(fc.Features), positiveLimit(limit, len(fc.Features))))
	for i, f := range fc.Features {
		if limit > 0 && len(out) >= limit {
			break
		}
		wkt, ok := geometryWKT(f.Geometry)
		if !ok {
			continue
		}
		props := map[string]any{
			"source_provider":   "overture_maps",
			"source_kind":       "overture_aoi_context",
			"source_release":    release,
			"theme":             propx.FirstNonEmpty(theme, stringFromAny(f.Properties["theme"])),
			"type":              stringFromAny(f.Properties["type"]),
			"abi_context_class": overtureContextClass(theme, stringFromAny(f.Properties["type"]), f.Properties),
		}
		copyStringProp(props, f.Properties, "id")
		copyStringProp(props, f.Properties, "subtype")
		copyStringProp(props, f.Properties, "class")
		copyStringProp(props, f.Properties, "names")
		copyStringProp(props, f.Properties, "categories")
		copyStringProp(props, f.Properties, "confidence")
		id := propx.FirstNonEmpty(stringFromAny(f.ID), stringFromAny(f.Properties["id"]), fmt.Sprintf("%s:%d", propx.FirstNonEmpty(theme, "feature"), i))
		out = append(out, features.Feature{
			ExtID:   id,
			GeomWKT: wkt,
			Props:   props,
		})
	}
	return out, nil
}

func geometryWKT(g geoGeometry) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(g.Type)) {
	case "point":
		pt, ok := asPoint(g.Coordinates)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("POINT(%f %f)", pt[0], pt[1]), true
	case "linestring":
		line, ok := asLine(g.Coordinates)
		if !ok || len(line) < 2 {
			return "", false
		}
		return "LINESTRING(" + joinPoints(line) + ")", true
	case "polygon":
		poly, ok := asPolygon(g.Coordinates)
		if !ok || len(poly) == 0 {
			return "", false
		}
		return "POLYGON(" + joinRings(poly) + ")", true
	case "multilinestring":
		lines, ok := asMultiLine(g.Coordinates)
		if !ok || len(lines) == 0 {
			return "", false
		}
		parts := make([]string, 0, len(lines))
		for _, line := range lines {
			if len(line) >= 2 {
				parts = append(parts, "("+joinPoints(line)+")")
			}
		}
		if len(parts) == 0 {
			return "", false
		}
		return "MULTILINESTRING(" + strings.Join(parts, ",") + ")", true
	case "multipolygon":
		polys, ok := asMultiPolygon(g.Coordinates)
		if !ok || len(polys) == 0 {
			return "", false
		}
		parts := make([]string, 0, len(polys))
		for _, poly := range polys {
			if len(poly) > 0 {
				parts = append(parts, "("+joinRings(poly)+")")
			}
		}
		if len(parts) == 0 {
			return "", false
		}
		return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")", true
	default:
		return "", false
	}
}

type point [2]float64

func asPoint(v any) (point, bool) {
	xs, ok := v.([]any)
	if !ok || len(xs) < 2 {
		return point{}, false
	}
	lon, ok1 := propx.Float(xs[0])
	lat, ok2 := propx.Float(xs[1])
	return point{lon, lat}, ok1 && ok2 && validLatLon(lat, lon)
}

func asLine(v any) ([]point, bool) {
	rows, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]point, 0, len(rows))
	for _, row := range rows {
		pt, ok := asPoint(row)
		if ok {
			out = append(out, pt)
		}
	}
	return out, len(out) > 0
}

func asPolygon(v any) ([][]point, bool) {
	rows, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([][]point, 0, len(rows))
	for _, row := range rows {
		line, ok := asLine(row)
		if ok {
			out = append(out, line)
		}
	}
	return out, len(out) > 0
}

func asMultiLine(v any) ([][]point, bool) {
	return asPolygon(v)
}

func asMultiPolygon(v any) ([][][]point, bool) {
	rows, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([][][]point, 0, len(rows))
	for _, row := range rows {
		poly, ok := asPolygon(row)
		if ok {
			out = append(out, poly)
		}
	}
	return out, len(out) > 0
}

func joinPoints(points []point) string {
	parts := make([]string, 0, len(points))
	for _, pt := range points {
		parts = append(parts, fmt.Sprintf("%f %f", pt[0], pt[1]))
	}
	return strings.Join(parts, ",")
}

func joinRings(poly [][]point) string {
	parts := make([]string, 0, len(poly))
	for _, ring := range poly {
		if len(ring) > 0 {
			parts = append(parts, "("+joinPoints(ring)+")")
		}
	}
	return strings.Join(parts, ",")
}

func parseInputs(raw string, isURL bool) []input {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []input
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		theme, value := splitLabelValue(item)
		if theme == "" && !isURL {
			theme = strings.TrimSuffix(filepath.Base(value), filepath.Ext(value))
		}
		in := input{Theme: theme}
		if isURL {
			in.URL = value
		} else {
			in.Path = value
		}
		out = append(out, in)
	}
	return out
}

func splitLabelValue(raw string) (string, string) {
	for _, sep := range []string{"|", "="} {
		if idx := strings.Index(raw, sep); idx > 0 {
			return strings.TrimSpace(raw[:idx]), strings.TrimSpace(raw[idx+1:])
		}
	}
	return "", raw
}

func overtureContextClass(theme, typ string, props map[string]any) string {
	text := strings.ToLower(theme + " " + typ + " " + stringFromAny(props["subtype"]) + " " + stringFromAny(props["class"]) + " " + stringFromAny(props["categories"]))
	switch {
	case strings.Contains(text, "transport"):
		return "transport_network_context"
	case strings.Contains(text, "building"):
		return "built_environment_context"
	case strings.Contains(text, "place"):
		return "activity_place_context"
	default:
		return "open_map_context"
	}
}

func copyStringProp(dst, src map[string]any, key string) {
	if src == nil {
		return
	}
	if value := strings.TrimSpace(stringFromAny(src[key])); value != "" {
		dst[key] = value
	}
}

func stringFromAny(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func positiveLimit(limit, fallback int) int {
	if limit > 0 && limit < fallback {
		return limit
	}
	return fallback
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
