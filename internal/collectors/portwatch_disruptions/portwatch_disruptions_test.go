// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package portwatch_disruptions

import "testing"

func TestEventsFromArcPortWatchDisruption(t *testing.T) {
	body := arcResponse{Features: []struct {
		Attributes map[string]any `json:"attributes"`
	}{{Attributes: map[string]any{
		"eventid": "1001", "eventtype": "Closure", "eventname": "Test Port",
		"alertlevel": "ORANGE", "country": "Panama", "lat": 8.9, "long": -79.5,
		"fromdate": float64(1777392000000), "n_affectedports": float64(2),
	}}}}
	evs := eventsFromArc(body, "https://example.test/arcgis")
	if len(evs) != 1 {
		t.Fatalf("events = %d", len(evs))
	}
	if evs[0].Source != "portwatch_disruptions" || evs[0].Props["alertlevel"] != "ORANGE" {
		t.Fatalf("unexpected event: %#v", evs[0])
	}
	if evs[0].Props["disruption_score"].(float64) <= 0 {
		t.Fatalf("missing disruption score: %#v", evs[0].Props)
	}
}
