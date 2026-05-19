// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package usgs_shakemap

import "testing"

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
