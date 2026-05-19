// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package nifc_fire_perimeters

import "testing"

func TestRingsToWKT(t *testing.T) {
	rings := [][][]float64{{{-85.0, 30.0}, {-84.9, 30.0}, {-84.9, 30.1}, {-85.0, 30.0}}}
	wkt, ok := ringsToWKT(rings)
	if !ok {
		t.Fatal("no wkt")
	}
	if wkt != "POLYGON((-85 30,-84.9 30,-84.9 30.1,-85 30))" {
		t.Fatalf("wkt=%q", wkt)
	}
}

func TestEventsFromFeatures(t *testing.T) {
	f := feature{
		Attributes: map[string]any{
			"GlobalID":                   "abc",
			"poly_IncidentName":          "Test Fire",
			"poly_GISAcres":              42.5,
			"poly_IRWINID":               "{IRWIN}",
			"poly_DateCurrent":           float64(1776458223700),
			"attr_PercentContained":      25.0,
			"attr_IncidentTypeKind":      "Wildfire",
			"attr_ModifiedOnDateTime_dt": float64(1776458223700),
		},
	}
	f.Geometry.Rings = [][][]float64{{{-85.0, 30.0}, {-84.9, 30.0}, {-84.9, 30.1}, {-85.0, 30.0}}}
	evs := eventsFromFeatures([]feature{f})
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Source != "nifc_fire_perimeters" || evs[0].ExtID != "abc" {
		t.Fatalf("identity wrong: %+v", evs[0])
	}
	if evs[0].Geom == "" {
		t.Fatal("missing geom")
	}
	if evs[0].Props["poly_IncidentName"] != "Test Fire" {
		t.Fatalf("props missing: %+v", evs[0].Props)
	}
}
