// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gfw

import (
	"testing"
	"time"
)

func TestParseGFWEventsAuditableGap(t *testing.T) {
	body := []byte(`{
	  "entries": [{
	    "id": "gap-1",
	    "type": "gap",
	    "start": "2026-05-02T08:00:00Z",
	    "end": "2026-05-02T20:00:00Z",
	    "duration": 43200000,
	    "position": {"lat": 25.20, "lon": 55.30},
	    "vessel": {"id": "v-1", "name": "TEST VESSEL", "ssvid": "123456789", "flag": "PA"},
	    "event_info": {"distance_from_shore_m": 12000}
	  }]
	}`)
	evs, err := parseGFWEvents(body, gfwDataset{EventType: "gap", Dataset: "public-global-gaps-events:latest"}, time.Date(2026, 5, 2, 21, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	ev := evs[0]
	if ev.Source != "gfw_events" || ev.ExtID != "gap-1" {
		t.Fatalf("event identity = %s/%s", ev.Source, ev.ExtID)
	}
	if got := ev.Props["source_dataset"]; got != "public-global-gaps-events:latest" {
		t.Fatalf("dataset=%v", got)
	}
	if got := ev.Props["abi_activity_class"]; got != "maritime_dark_activity" {
		t.Fatalf("activity class=%v", got)
	}
	score, _ := ev.Props["abi_activity_score"].(float64)
	if score != 2 {
		t.Fatalf("activity score=%.2f, want 2", score)
	}
	if got := ev.Props["info_distance_from_shore_m"]; got == nil {
		t.Fatalf("missing event_info copy")
	}
}

func TestParseGFWFeatureCollectionPoint(t *testing.T) {
	body := []byte(`{
	  "type": "FeatureCollection",
	  "features": [{
	    "id": "loiter-1",
	    "geometry": {"type":"Point","coordinates":[44.1, 12.5]},
	    "properties": {
	      "type": "loitering",
	      "start": "2026-05-02T10:00:00Z",
	      "duration": "3h"
	    }
	  }]
	}`)
	evs, err := parseGFWEvents(body, gfwDataset{EventType: "loitering", Dataset: "public-global-loitering-events:latest"}, time.Now())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Lat != 12.5 || evs[0].Lon != 44.1 {
		t.Fatalf("point=(%.2f, %.2f)", evs[0].Lat, evs[0].Lon)
	}
	if got := evs[0].Props["event_type"]; got != "loitering" {
		t.Fatalf("event_type=%v", got)
	}
}
