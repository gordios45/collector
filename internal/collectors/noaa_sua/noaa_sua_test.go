// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package noaa_sua

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFeaturesFromCollection(t *testing.T) {
	var raw featureCollection
	body := []byte(`{
	  "type": "FeatureCollection",
	  "features": [{
	    "id": 7,
	    "type": "Feature",
	    "properties": {
	      "objectid": 7,
	      "featurename": "AVON PARK, FL",
	      "specialuseairspacetype": "Restricted",
	      "airspacestatus": "Active",
	      "controllingagency": "FAA"
	    },
	    "geometry": {
	      "type": "Polygon",
	      "coordinates": [[[-81.1,27.7],[-81.2,27.7],[-81.2,27.6],[-81.1,27.7]]]
	    }
	  }]
	}`)
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	feats := featuresFromCollection(raw, defaultEndpoint, 0, 10)
	if len(feats) != 1 {
		t.Fatalf("features = %d, want 1", len(feats))
	}
	if feats[0].ExtID != "7" {
		t.Fatalf("ext id = %q, want 7", feats[0].ExtID)
	}
	if feats[0].GeomWKT == "" {
		t.Fatalf("missing WKT")
	}
	if got := feats[0].Props["airspace_type"]; got != "Restricted" {
		t.Fatalf("airspace_type = %v, want Restricted", got)
	}
	if got := feats[0].Props["context_only"]; got != true {
		t.Fatalf("context_only = %v, want true", got)
	}
}

func TestArcGISQueryURL(t *testing.T) {
	got := arcGISQueryURL(defaultEndpoint, 2000, 2000)
	for _, want := range []string{"f=geojson", "outSR=4326", "resultOffset=2000", "resultRecordCount=2000"} {
		if !strings.Contains(got, want) {
			t.Fatalf("query URL %q missing %s", got, want)
		}
	}
}
