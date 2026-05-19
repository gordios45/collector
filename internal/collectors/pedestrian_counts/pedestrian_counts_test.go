// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package pedestrian_counts

import "testing"

func TestMelbourneEventsAggregateOnly(t *testing.T) {
	locations := map[int]melbourneLocation{
		3: {
			LocationID:        3,
			SensorDescription: "Melbourne Central",
			SensorName:        "Swa295_T",
			Direction1:        "North",
			Direction2:        "South",
			Latitude:          -37.81101524,
			Longitude:         144.96429485,
		},
	}
	counts := []melbourneCount{{
		LocationID:        3,
		SensingDatetime:   "2026-05-18T13:57:00+00:00",
		SensingDate:       "2026-05-18",
		SensingTime:       "23:57",
		Direction1:        2,
		Direction2:        1,
		TotalOfDirections: 3,
	}}
	evs := melbourneEvents(locations, counts)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].ExtID != "melbourne:3:20260518T135700" {
		t.Fatalf("ext_id=%s", evs[0].ExtID)
	}
	if evs[0].Props["privacy_model"] != "aggregate_sensor_count_no_identifiers" {
		t.Fatalf("privacy=%v", evs[0].Props["privacy_model"])
	}
	if _, ok := evs[0].Props["pedestrian_count"]; !ok {
		t.Fatalf("missing pedestrian_count")
	}
}

func TestDensitySignal(t *testing.T) {
	if densitySignal(3) != "low" || densitySignal(30) != "moderate" || densitySignal(150) != "high" || densitySignal(300) != "very_high" {
		t.Fatalf("unexpected density thresholds")
	}
}
