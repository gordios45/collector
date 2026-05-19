// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package public_facilities

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFacilityPointParsers(t *testing.T) {
	raw := json.RawMessage(`[-77.0365,38.8977]`)
	lat, lon, ok := pointFromGeoJSON("Point", raw)
	if !ok || lat != 38.8977 || lon != -77.0365 {
		t.Fatalf("pointFromGeoJSON=(%f,%f,%v)", lat, lon, ok)
	}

	fields := map[string]any{"geo_point_2d": []any{48.8566, 2.3522}}
	lat, lon, ok = pointFromOpenDataSoft(nil, fields)
	if !ok || lat != 48.8566 || lon != 2.3522 {
		t.Fatalf("pointFromOpenDataSoft=(%f,%f,%v)", lat, lon, ok)
	}
}

func TestArcGISPageURL(t *testing.T) {
	got := arcGISPageURL("https://example.test/FeatureServer/0/query?where=1%3D1&outFields=*&f=json", 2000, 2000)
	if !strings.Contains(got, "resultOffset=2000") || !strings.Contains(got, "resultRecordCount=2000") {
		t.Fatalf("arcGISPageURL missing paging params: %s", got)
	}
	if !strings.Contains(got, "where=1%3D1") || !strings.Contains(got, "outFields=%2A") {
		t.Fatalf("arcGISPageURL dropped base params: %s", got)
	}
}
