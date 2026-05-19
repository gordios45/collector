// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package malaysia_weather_warnings

import "testing"

func TestEventsFromRows(t *testing.T) {
	validTo := "2026-05-21T00:00:00"
	rows := []warningRow{{
		ValidTo:   &validTo,
		HeadingEN: "Warning on Strong Wind and Rough Seas (Third Category)",
		TextEN:    "Strong westerly winds over 60 kmph with waves exceeding 4.5 metres are expected.",
	}}
	rows[0].WarningIssue.Issued = "2026-05-19T01:00:00"
	rows[0].WarningIssue.TitleEN = "Third Category Warning on Strong Winds and Rough Seas"

	evs := eventsFromRows(defaultURL, rows)
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Source != sourceID || evs[0].Props["severity"] != "high" {
		t.Fatalf("unexpected event: %+v", evs[0])
	}
	if evs[0].Props["hazard_type"] != "marine_weather" {
		t.Fatalf("hazard not classified: %#v", evs[0].Props["hazard_type"])
	}
}
