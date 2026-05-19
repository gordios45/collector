// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package worldpop_exposure

import (
	"encoding/json"
	"testing"
)

func TestGeoJSONCircleProducesPolygon(t *testing.T) {
	raw, err := geoJSONCircle(45, 9, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	var fc map[string]any
	if err := json.Unmarshal([]byte(raw), &fc); err != nil {
		t.Fatal(err)
	}
	if fc["type"] != "FeatureCollection" {
		t.Fatalf("unexpected geojson: %#v", fc)
	}
}

func TestDeriveExposureMetrics(t *testing.T) {
	m := deriveExposureMetrics(map[float64]populationSample{
		1:  {Population: 18000, Status: "finished"},
		5:  {Population: 240000, Status: "finished"},
		25: {Population: 1800000, Status: "finished"},
	}, 10)
	if m.Population1KM != 18000 || m.Population5KM != 240000 || m.Population25KM != 1800000 {
		t.Fatalf("unexpected populations: %+v", m)
	}
	if m.SettlementPresenceScore <= 0 {
		t.Fatalf("expected settlement score: %+v", m)
	}
	if m.ImpactPriorScore <= 0 {
		t.Fatalf("expected impact score: %+v", m)
	}
	if m.LowPopulationContextScore != 0 {
		t.Fatalf("dense AOI should not be low-pop: %+v", m)
	}
}

func TestLowPopulationContextScore(t *testing.T) {
	if got := lowPopulationContextScore(50, 0); got <= 0 {
		t.Fatalf("expected low population suppression context, got %f", got)
	}
	if got := lowPopulationContextScore(5000, 1); got != 0 {
		t.Fatalf("dense settlement should not be suppressed, got %f", got)
	}
}
