// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Reads local GeoJSON / JSON files from the gordios repo's data/ directory
// and converts each Feature into features.Feature (WKT + props).
package seeders

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gordios45/collector/internal/features"
)

// LocalFile seeds a source from <dataDir>/<filename>. Format is detected:
//   - FeatureCollection (GeoJSON) → each feature → Feature.
//   - Array of objects with lat/lon/latitude/longitude → Point features.
func LocalFile(dataDir, filename, source string) ([]features.Feature, error) {
	p := filepath.Join(dataDir, filename)
	buf, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p, err)
	}

	// Peek at the shape.
	var probe map[string]any
	if err := json.Unmarshal(buf, &probe); err == nil {
		if t, _ := probe["type"].(string); t == "FeatureCollection" {
			return fromFeatureCollection(buf)
		}
		// Object with an inner array, e.g. {features: [...]}.
		for _, key := range []string{"events", "items", "data", "results", "records"} {
			if arr, ok := probe[key].([]any); ok {
				rows := make([]map[string]any, 0, len(arr))
				for _, a := range arr {
					if m, ok := a.(map[string]any); ok {
						rows = append(rows, m)
					}
				}
				return fromPointArray(rows), nil
			}
		}
	}
	// Fall through: top-level array-of-objects.
	var arr []map[string]any
	if err := json.Unmarshal(buf, &arr); err == nil {
		return fromPointArray(arr), nil
	}
	return nil, fmt.Errorf("unrecognised shape in %s", filename)
}

func fromFeatureCollection(buf []byte) ([]features.Feature, error) {
	var fc struct {
		Features []struct {
			ID         any             `json:"id"`
			Geometry   json.RawMessage `json:"geometry"`
			Properties map[string]any  `json:"properties"`
		} `json:"features"`
	}
	if err := json.Unmarshal(buf, &fc); err != nil {
		return nil, fmt.Errorf("parse FC: %w", err)
	}
	out := make([]features.Feature, 0, len(fc.Features))
	for i, f := range fc.Features {
		wkt, err := geomToWKT(f.Geometry)
		if err != nil || wkt == "" {
			continue
		}
		ext := fmt.Sprintf("%v", firstNonEmpty(f.ID, f.Properties["id"], i))
		props := f.Properties
		if props == nil {
			props = map[string]any{}
		}
		out = append(out, features.Feature{
			ExtID: ext, GeomWKT: wkt, Props: props,
		})
	}
	return out, nil
}

func fromPointArray(arr []map[string]any) []features.Feature {
	out := make([]features.Feature, 0, len(arr))
	for i, row := range arr {
		lat, lon, ok := extractPoint(row)
		if !ok {
			continue
		}
		ext := fmt.Sprintf("%v", firstNonEmpty(row["id"], row["name"], i))
		out = append(out, features.Feature{
			ExtID:   ext,
			GeomWKT: fmt.Sprintf("POINT(%f %f)", lon, lat),
			Props:   row,
		})
	}
	return out
}

// ---- geometry → WKT ----

func geomToWKT(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var g struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return "", err
	}
	switch g.Type {
	case "Point":
		var c [3]float64
		if err := json.Unmarshal(g.Coordinates, &c); err != nil {
			var c2 [2]float64
			if err2 := json.Unmarshal(g.Coordinates, &c2); err2 != nil {
				return "", err
			}
			return fmt.Sprintf("POINT(%f %f)", c2[0], c2[1]), nil
		}
		return fmt.Sprintf("POINT(%f %f)", c[0], c[1]), nil

	case "LineString":
		var ls [][]float64
		if err := json.Unmarshal(g.Coordinates, &ls); err != nil {
			return "", err
		}
		parts := make([]string, 0, len(ls))
		for _, p := range ls {
			if len(p) < 2 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%f %f", p[0], p[1]))
		}
		if len(parts) < 2 {
			return "", nil
		}
		return "LINESTRING(" + strings.Join(parts, ",") + ")", nil

	case "Polygon":
		var poly [][][]float64
		if err := json.Unmarshal(g.Coordinates, &poly); err != nil {
			return "", err
		}
		return polyWKT(poly), nil

	case "MultiLineString":
		var m [][][]float64
		if err := json.Unmarshal(g.Coordinates, &m); err != nil {
			return "", err
		}
		parts := []string{}
		for _, ls := range m {
			pp := []string{}
			for _, p := range ls {
				if len(p) < 2 {
					continue
				}
				pp = append(pp, fmt.Sprintf("%f %f", p[0], p[1]))
			}
			if len(pp) >= 2 {
				parts = append(parts, "("+strings.Join(pp, ",")+")")
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		return "MULTILINESTRING(" + strings.Join(parts, ",") + ")", nil

	case "MultiPolygon":
		var mp [][][][]float64
		if err := json.Unmarshal(g.Coordinates, &mp); err != nil {
			return "", err
		}
		parts := []string{}
		for _, poly := range mp {
			p := polyWKT(poly)
			if p != "" {
				parts = append(parts, strings.TrimPrefix(p, "POLYGON"))
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")", nil
	}
	return "", nil
}

func polyWKT(poly [][][]float64) string {
	rings := []string{}
	for _, ring := range poly {
		pp := []string{}
		for _, p := range ring {
			if len(p) < 2 {
				continue
			}
			pp = append(pp, fmt.Sprintf("%f %f", p[0], p[1]))
		}
		if len(pp) >= 3 {
			rings = append(rings, "("+strings.Join(pp, ",")+")")
		}
	}
	if len(rings) == 0 {
		return ""
	}
	return "POLYGON(" + strings.Join(rings, ",") + ")"
}

func extractPoint(row map[string]any) (lat, lon float64, ok bool) {
	// Permute lowercase AND PascalCase variants — nuclear_facilities uses
	// Latitude/Longitude; some local fixtures use lat/lon or
	// coordinates.latitude, etc.
	pairs := [][2]string{
		{"lat", "lon"}, {"lat", "lng"},
		{"latitude", "longitude"},
		{"Latitude", "Longitude"},
		{"Lat", "Lon"}, {"Lat", "Lng"},
	}
	for _, ks := range pairs {
		if a, aOK := toFloat(row[ks[0]]); aOK {
			if b, bOK := toFloat(row[ks[1]]); bOK {
				return a, b, true
			}
		}
	}
	return 0, 0, false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		var f float64
		_, err := fmt.Sscanf(x, "%f", &f)
		return f, err == nil
	}
	return 0, false
}

func firstNonEmpty(xs ...any) any {
	for _, x := range xs {
		if x == nil {
			continue
		}
		if s, ok := x.(string); ok && s == "" {
			continue
		}
		return x
	}
	return "_"
}
