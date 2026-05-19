// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package ifrc_go

import "testing"

func TestEventsFromIFRC(t *testing.T) {
	row := goEvent{
		ID:                42,
		Name:              "Floods",
		DType:             disaster{ID: 1, Name: "Flood"},
		Countries:         []country{{ISO: "SB", ISO3: "SLB", Name: "Solomon Islands"}},
		Severity:          3,
		SeverityDisplay:   "Orange",
		NumAffected:       float64(12000),
		DisasterStartDate: "2026-05-01",
	}
	events := eventsFromIFRC(row, map[string]countryPoint{
		"SLB": {Lat: -9.65, Lon: 160.16, Name: "Solomon Islands"},
	})
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	ev := events[0]
	if ev.Source != sourceID || ev.ExtID == "" || !ev.HasPoint() {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Props["humanitarian_impact_score"].(float64) <= 0 {
		t.Fatalf("missing impact score: %+v", ev.Props)
	}
}
