// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package purpleair

import "testing"

func TestEventsFromResponseMapsFieldsAndScoresPM25(t *testing.T) {
	body := apiResponse{
		Fields: []string{"sensor_index", "name", "latitude", "longitude", "pm2.5_atm", "pm2.5_cf_1", "humidity", "temperature", "last_seen", "confidence"},
		Data: [][]any{
			{12345.0, "Industrial South", 34.05, -118.25, 82.4, 79.2, 41.0, 73.5, 1777552800.0, 95.0},
		},
	}
	evs := eventsFromResponse(body, aoi{Label: "la", Lat: 34.05, Lon: -118.25, RadiusKM: 25}, endpoint)
	if len(evs) != 1 {
		t.Fatalf("events len = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Source != "purpleair" || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("bad event identity/geocode: %#v", ev)
	}
	if got := ev.Props["severity"]; got != "high" {
		t.Fatalf("severity = %v, want high", got)
	}
	if score, ok := ev.Props["air_particulate_score"].(float64); !ok || score <= 1 {
		t.Fatalf("air_particulate_score = %#v, want > 1", ev.Props["air_particulate_score"])
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestParseAOIs(t *testing.T) {
	got := parseAOIs("la:34.05:-118.25:25,bad,nyc:40.7:-74.0:15")
	if len(got) != 2 {
		t.Fatalf("AOIs len = %d, want 2", len(got))
	}
	if got[0].Label != "la" || got[1].Label != "nyc" {
		t.Fatalf("unexpected AOIs: %#v", got)
	}
}
