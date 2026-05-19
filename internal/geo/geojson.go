// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// GeoJSON → WKT converter shared by collectors + seeders. Pure Go, no
// PostGIS round-trip needed to build an INSERT-ready WKT string.
package geo

import (
	"encoding/json"
	"fmt"
	"strings"
)

// GeoJSONToWKT accepts a geometry object (not a Feature or FeatureCollection).
// Returns "" if the shape is unsupported or malformed.
func GeoJSONToWKT(raw []byte) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var g struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return ""
	}
	switch g.Type {
	case "Point":
		var c []float64
		if err := json.Unmarshal(g.Coordinates, &c); err != nil || len(c) < 2 {
			return ""
		}
		return fmt.Sprintf("POINT(%f %f)", c[0], c[1])

	case "LineString":
		var ls [][]float64
		if err := json.Unmarshal(g.Coordinates, &ls); err != nil {
			return ""
		}
		return "LINESTRING(" + coordsList(ls) + ")"

	case "Polygon":
		var poly [][][]float64
		if err := json.Unmarshal(g.Coordinates, &poly); err != nil {
			return ""
		}
		return polyWKT(poly)

	case "MultiPolygon":
		var mp [][][][]float64
		if err := json.Unmarshal(g.Coordinates, &mp); err != nil {
			return ""
		}
		parts := []string{}
		for _, poly := range mp {
			if p := polyWKT(poly); p != "" {
				parts = append(parts, strings.TrimPrefix(p, "POLYGON"))
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")"
	}
	return ""
}

// Unwraps Feature / FeatureCollection to the inner geometry (first feature).
func ExtractFirstGeometry(raw []byte) []byte {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return raw
	}
	if t, _ := unquote(probe["type"]); t == "Feature" {
		return probe["geometry"]
	}
	if t, _ := unquote(probe["type"]); t == "FeatureCollection" {
		var fc struct {
			Features []struct {
				Geometry json.RawMessage `json:"geometry"`
			} `json:"features"`
		}
		if err := json.Unmarshal(raw, &fc); err == nil && len(fc.Features) > 0 {
			return fc.Features[0].Geometry
		}
	}
	return raw
}

func unquote(b []byte) (string, error) {
	var s string
	err := json.Unmarshal(b, &s)
	return s, err
}

func coordsList(ls [][]float64) string {
	parts := make([]string, 0, len(ls))
	for _, p := range ls {
		if len(p) < 2 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%f %f", p[0], p[1]))
	}
	return strings.Join(parts, ",")
}

func polyWKT(poly [][][]float64) string {
	rings := []string{}
	for _, ring := range poly {
		pts := coordsList(ring)
		if len(strings.Split(pts, ",")) >= 3 {
			rings = append(rings, "("+pts+")")
		}
	}
	if len(rings) == 0 {
		return ""
	}
	return "POLYGON(" + strings.Join(rings, ",") + ")"
}
