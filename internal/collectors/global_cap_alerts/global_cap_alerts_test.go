// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package global_cap_alerts

import (
	"encoding/json"
	"testing"
)

func TestEventFromFeature(t *testing.T) {
	coords := json.RawMessage(`[[[-80,25],[-79,25],[-79,26],[-80,26],[-80,25]]]`)
	ev, ok := eventFromFeature(feature{
		Geometry: geoJSONGeometry{Type: "Polygon", Coordinates: coords},
		Properties: map[string]any{
			"identifier":  "cap-1",
			"sent":        "2026-05-06T07:00:00Z",
			"event":       "Flash Flood Warning",
			"headline":    "Flash Flood Warning",
			"description": "Flooding expected",
			"severity":    "Severe",
			"urgency":     "Immediate",
			"certainty":   "Observed",
			"status":      "Actual",
		},
	})
	if !ok {
		t.Fatal("expected event")
	}
	if ev.Source != sourceID || ev.ExtID == "" || !ev.HasPoint() {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Props["civil_alert_score"].(float64) <= 0 {
		t.Fatalf("missing alert score: %+v", ev.Props)
	}
}
