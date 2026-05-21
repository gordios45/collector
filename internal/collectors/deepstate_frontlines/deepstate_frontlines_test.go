// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package deepstate_frontlines

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseSnapshotPolygonAndPoint(t *testing.T) {
	rawJSON := `{
	  "id": 1779370458,
	  "datetime": "21.05 o 15:34",
	  "map": {
	    "type": "FeatureCollection",
	    "features": [
	      {
	        "type": "Feature",
	        "geometry": {"type": "Polygon", "coordinates": [[[37.1,48.1,0],[37.2,48.1,0],[37.2,48.2,0],[37.1,48.1,0]]]},
	        "properties": {"name": "Unknown status", "fill": "#bcaaa4"}
	      },
	      {
	        "type": "Feature",
	        "geometry": {"type": "Point", "coordinates": [36.5,47.8,0]},
	        "properties": {"name": "Point marker"}
	      }
	    ]
	  }
	}`
	var raw snapshot
	if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
		t.Fatal(err)
	}
	feats, err := parseSnapshot(raw, "https://example.test/deepstate", time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(feats) != 2 {
		t.Fatalf("len(feats) = %d, want 2", len(feats))
	}
	if feats[0].GeomWKT != "POLYGON((37.100000 48.100000,37.200000 48.100000,37.200000 48.200000,37.100000 48.100000))" {
		t.Fatalf("polygon WKT = %q", feats[0].GeomWKT)
	}
	if feats[1].GeomWKT != "POINT(36.500000 47.800000)" {
		t.Fatalf("point WKT = %q", feats[1].GeomWKT)
	}
	if feats[0].Props["source_provider"] != "DeepStateMap" {
		t.Fatalf("source_provider = %v", feats[0].Props["source_provider"])
	}
	if feats[0].Props["snapshot_datetime"] != "21.05 o 15:34" {
		t.Fatalf("snapshot_datetime = %v", feats[0].Props["snapshot_datetime"])
	}
}
