// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package usgs_shakemap

import (
	"encoding/json"
	"testing"
)

func TestFirstProductAndEnrichmentHelpers(t *testing.T) {
	raw := []any{map[string]any{
		"id":         "urn:test",
		"status":     "UPDATE",
		"updateTime": float64(1777400000000),
		"properties": map[string]any{
			"maxmmi":      "6.4",
			"maxpga-grid": "0.55",
			"map-status":  "reviewed",
		},
		"contents": map[string]any{
			"download/grid.xml": map[string]any{"url": "https://example.test/grid.xml", "contentType": "application/xml"},
		},
	}}
	p := firstProduct(raw)
	if p == nil {
		t.Fatal("product nil")
	}
	props := map[string]any{}
	copyProductFloats(props, "shakemap", p.Properties, "maxmmi", "maxpga-grid")
	copyProductStrings(props, "shakemap", p.Properties, "map-status")
	copyContentURL(props, "shakemap_grid_url", p.Contents, "download/grid.xml")
	if props["shakemap_maxmmi"] != 6.4 {
		t.Fatalf("maxmmi=%v", props["shakemap_maxmmi"])
	}
	if props["shakemap_maxpga_grid"] != 0.55 {
		t.Fatalf("maxpga_grid=%v", props["shakemap_maxpga_grid"])
	}
	if props["shakemap_map_status"] != "reviewed" {
		t.Fatalf("map status=%v", props["shakemap_map_status"])
	}
	if props["shakemap_grid_url"] != "https://example.test/grid.xml" {
		t.Fatalf("grid url=%v", props["shakemap_grid_url"])
	}
}

func TestCopyProductFloatsSkipsNonFiniteValues(t *testing.T) {
	props := map[string]any{}
	copyProductFloats(props, "shakemap", map[string]string{
		"maxmmi":   "nan",
		"maxpga":   "NaN",
		"maxpgv":   "+Inf",
		"maxpsa10": "-Inf",
		"maxpsa03": "6.2",
	}, "maxmmi", "maxpga", "maxpgv", "maxpsa10", "maxpsa03")

	for _, key := range []string{"shakemap_maxmmi", "shakemap_maxpga", "shakemap_maxpgv", "shakemap_maxpsa10"} {
		if _, ok := props[key]; ok {
			t.Fatalf("non-finite value copied for %s: %+v", key, props)
		}
	}
	if props["shakemap_maxpsa03"] != 6.2 {
		t.Fatalf("finite value not copied: %+v", props)
	}
	if _, err := json.Marshal(props); err != nil {
		t.Fatalf("props should remain JSON encodable: %v", err)
	}
}
