// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package eia930

import "testing"

func TestEventsFromResponseComputesDemandAnomaly(t *testing.T) {
	rows := []eiaRow{
		{Period: "2026-05-01T10", Respondent: "PJM", RespondentName: "PJM", Type: "D", TypeName: "Demand", Value: 120000.0, Units: "megawatthours"},
		{Period: "2026-05-01T09", Respondent: "PJM", RespondentName: "PJM", Type: "D", TypeName: "Demand", Value: 90000.0, Units: "megawatthours"},
		{Period: "2026-05-01T08", Respondent: "PJM", RespondentName: "PJM", Type: "D", TypeName: "Demand", Value: 91000.0, Units: "megawatthours"},
		{Period: "2026-05-01T07", Respondent: "PJM", RespondentName: "PJM", Type: "D", TypeName: "Demand", Value: 89000.0, Units: "megawatthours"},
		{Period: "2026-05-01T10", Respondent: "PJM", RespondentName: "PJM", Type: "TI", TypeName: "Total interchange", Value: -3200.0, Units: "megawatthours"},
		{Period: "2026-05-01T09", Respondent: "PJM", RespondentName: "PJM", Type: "TI", TypeName: "Total interchange", Value: -2800.0, Units: "megawatthours"},
	}
	evs := eventsFromResponse(rows, regionCatalog["PJM"], endpoint)
	if len(evs) != 2 {
		t.Fatalf("events len = %d, want 2", len(evs))
	}
	var demand map[string]any
	for _, ev := range evs {
		if ev.Source != "eia930" || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
			t.Fatalf("bad event identity/geocode: %#v", ev)
		}
		if ev.Props["metric"] == "demand" {
			demand = ev.Props
		}
	}
	if demand == nil {
		t.Fatal("missing demand event")
	}
	if demand["demand_mw"] == nil {
		t.Fatalf("missing demand_mw: %#v", demand)
	}
	if score, ok := demand["demand_anomaly_score"].(float64); !ok || score <= 0 {
		t.Fatalf("demand anomaly score = %#v, want positive float", demand["demand_anomaly_score"])
	}
	if demand["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestMetricKind(t *testing.T) {
	cases := map[string]string{
		"D":  "demand",
		"DF": "forecast_demand",
		"TI": "interchange",
		"NG": "",
	}
	for code, want := range cases {
		if got := metricKind(code, ""); got != want {
			t.Fatalf("metricKind(%q) = %q, want %q", code, got, want)
		}
	}
}
