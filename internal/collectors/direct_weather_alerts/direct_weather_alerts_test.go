// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package direct_weather_alerts

import (
	"encoding/json"
	"testing"
)

func TestGeoJSONCentroid(t *testing.T) {
	coords := json.RawMessage(`[[[10,60],[12,60],[12,62],[10,62],[10,60]]]`)
	lat, lon, ok := geoJSONCentroid(geoJSONGeometry{Type: "Polygon", Coordinates: coords})
	if !ok {
		t.Fatal("expected centroid")
	}
	if lat < 60 || lat > 62 || lon < 10 || lon > 12 {
		t.Fatalf("unexpected centroid: %f %f", lat, lon)
	}
}

func TestJMAWarningHelpers(t *testing.T) {
	if !jmaActiveStatus("発表") {
		t.Fatal("expected active status")
	}
	if jmaActiveStatus("解除") {
		t.Fatal("expected cleared status")
	}
	if jmaSeverity("03") != "Severe" {
		t.Fatalf("bad severity")
	}
}
