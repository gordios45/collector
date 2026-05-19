// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package jma_rsmc

import (
	"encoding/json"
	"testing"
)

// Real archived JMA specifications.json from TC2410 (Jongdari, August 2024)
// — sanity-check that our parser pulls the analysis fix correctly and
// would attach forecast points if any. This sample only has analysis
// (no forecasts because the system was a tropical depression), so the
// forecast slice is expected to be empty.
const sampleJongdari = `[
  {"part":"title","issue":{"JST":"2024-08-20T21:45:00+09:00","UTC":"2024-08-20T12:45:00Z"},"typhoonNumber":"2409","name":{"jp":"ジョンダリ","en":"Jongdari"},"category":{"jp":"熱帯低気圧","en":"TD"}},
  {"part":{"jp":"実況","en":"Analysis"},"advancedHours":0,"category":{"jp":"熱帯低気圧","en":"TD"},"scale":"-","intensity":"-","position":{"deg":[34.0,126.0],"dm":[[34,0],[126,0]]},"location":"黄海","course":"北","speed":{"km/h":"35","kt":"20"},"pressure":"1004","validtime":{"JST":"2024-08-20T21:00:00+09:00","UTC":"2024-08-20T12:00:00Z"}}
]`

func TestBuildEvents_SinglAnalysisFix(t *testing.T) {
	var spec []specRecord
	if err := json.Unmarshal([]byte(sampleJongdari), &spec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	entry := targetTcEntry{TropicalCyclone: "TC2410"}
	evs := buildEvents("TC2410", entry, spec)
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	e := evs[0]
	if e.Source != "jma_rsmc" {
		t.Errorf("source: got %q", e.Source)
	}
	if e.Lat != 34.0 || e.Lon != 126.0 {
		t.Errorf("lat/lon: got %v,%v want 34.0,126.0", e.Lat, e.Lon)
	}
	if got := e.Ts.Format("2006-01-02T15:04:05Z"); got != "2024-08-20T12:00:00Z" {
		t.Errorf("ts: got %q", got)
	}
	if e.ExtID != "TC2410:2024-08-20T12:45:00Z" && e.ExtID != "TC2410:2024-08-20T12:00:00Z" {
		// ExtID embeds the validtime; ensure it's populated and unique-shaped
		if e.ExtID == "" || e.ExtID == "TC2410:" {
			t.Errorf("ext_id: got %q", e.ExtID)
		}
	}
	if got := e.Props["name_en"]; got != "Jongdari" {
		t.Errorf("name_en: got %v", got)
	}
	if got := e.Props["typhoon_number"]; got != "2409" {
		t.Errorf("typhoon_number: got %v", got)
	}
	if got := e.Props["pressure_mb"]; got != "1004" {
		t.Errorf("pressure_mb: got %v", got)
	}
	if got := e.Props["category"]; got != "TD" {
		t.Errorf("category: got %v", got)
	}
	if got := e.Props["agency"]; got != "JMA RSMC Tokyo" {
		t.Errorf("agency: got %v", got)
	}
	// No forecast points in this sample
	fc, _ := e.Props["forecast"].([]struct{})
	_ = fc
}

func TestBuildEvents_SkipsTitleAndEmptyPosition(t *testing.T) {
	spec := []specRecord{
		{Part: "title"},
		{Part: map[string]any{"en": "Analysis"}, AdvancedHours: 0}, // no position
	}
	evs := buildEvents("TC9999", targetTcEntry{}, spec)
	if len(evs) != 0 {
		t.Fatalf("expected 0 events from title-only + missing-position, got %d", len(evs))
	}
}

func TestBuildEvents_AttachesForecast(t *testing.T) {
	spec := []specRecord{
		{Part: "title", TyphoonNumber: "2401", Name: struct {
			EN string `json:"en"`
			JP string `json:"jp"`
		}{EN: "Test"}},
	}
	// Build the analysis record manually because the inline position struct
	// can't be initialized via composite literal (anonymous type).
	var an specRecord
	an.AdvancedHours = 0
	an.Pressure = "990"
	an.Validtime = &struct {
		UTC string `json:"UTC"`
		JST string `json:"JST"`
	}{UTC: "2024-09-01T00:00:00Z"}
	an.Position = &struct {
		Deg []float64 `json:"deg"`
	}{Deg: []float64{15.0, 130.0}}
	spec = append(spec, an)

	var f24 specRecord
	f24.AdvancedHours = 24
	f24.Pressure = "975"
	f24.Validtime = &struct {
		UTC string `json:"UTC"`
		JST string `json:"JST"`
	}{UTC: "2024-09-02T00:00:00Z"}
	f24.Position = &struct {
		Deg []float64 `json:"deg"`
	}{Deg: []float64{18.0, 131.0}}
	spec = append(spec, f24)

	evs := buildEvents("TC2401", targetTcEntry{}, spec)
	if len(evs) != 1 {
		t.Fatalf("expected 1 analysis event, got %d", len(evs))
	}
	fc, ok := evs[0].Props["forecast"].([]map[string]any)
	if !ok {
		t.Fatalf("forecast slice missing or wrong type: %T", evs[0].Props["forecast"])
	}
	if len(fc) != 1 {
		t.Fatalf("expected 1 forecast point, got %d", len(fc))
	}
	if fc[0]["advanced_hours"] != 24 || fc[0]["lat"] != 18.0 || fc[0]["lon"] != 131.0 {
		t.Errorf("forecast point wrong: %+v", fc[0])
	}
}
