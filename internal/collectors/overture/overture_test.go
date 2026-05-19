// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package overture

import "testing"

func TestParseGeoJSONFeaturesStoresOvertureContext(t *testing.T) {
	body := []byte(`{
	  "type": "FeatureCollection",
	  "features": [
	    {
	      "id": "place-1",
	      "geometry": {"type":"Point","coordinates":[12.49,41.90]},
	      "properties": {"type":"place", "categories":"transit.station"}
	    },
	    {
	      "id": "building-1",
	      "geometry": {"type":"Polygon","coordinates":[[[12.0,41.0],[12.1,41.0],[12.1,41.1],[12.0,41.0]]]},
	      "properties": {"type":"building"}
	    }
	  ]
	}`)
	feats, err := parseGeoJSONFeatures(body, "places", "2026-04-01.0", 10)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(feats) != 2 {
		t.Fatalf("features=%d", len(feats))
	}
	if feats[0].GeomWKT != "POINT(12.490000 41.900000)" {
		t.Fatalf("point wkt=%s", feats[0].GeomWKT)
	}
	if got := feats[0].Props["source_provider"]; got != "overture_maps" {
		t.Fatalf("provider=%v", got)
	}
	if got := feats[0].Props["source_release"]; got != "2026-04-01.0" {
		t.Fatalf("release=%v", got)
	}
	if feats[1].GeomWKT == "" || feats[1].Props["abi_context_class"] != "built_environment_context" {
		t.Fatalf("building context not parsed: %#v", feats[1])
	}
}
