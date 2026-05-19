// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package wmo_alert_hub

import "testing"

func TestEventFromItem(t *testing.T) {
	ev, ok := eventFromItem(alertItem{
		ID:        "IN-1777410481148017_45",
		Event:     "Thunder shower with strong wind",
		Headline:  "Light to moderate Thunderstorm",
		Sent:      "2026-04-28 21:37:32",
		Expires:   "2026-04-29 00:35:00",
		AreaDesc:  "Bankura districts",
		MID:       "066",
		Region:    "2",
		Severity:  2,
		Urgency:   3,
		Certainty: 2,
		URL:       "in-ndma-xx/2026/04/28/21/37/32-e2755ec65901173e388622366bfe65e6.xml",
	}, map[string]member{
		"066": {MID: "066", Name: "India", Dept: "India Meteorological Department", Lat: 20.593684, Lng: 78.96288, Code: "IND", Reg: 2},
	}, "2026-04-28 21:16:07")
	if !ok {
		t.Fatal("event skipped")
	}
	if ev.Source != "wmo_alert_hub" || ev.ExtID != "IN-1777410481148017_45" {
		t.Fatalf("identity wrong: %+v", ev)
	}
	if ev.Props["severity"] != "Severe" || ev.Props["member_country"] != "India" {
		t.Fatalf("props wrong: %+v", ev.Props)
	}
	if ev.Lat != 20.593684 || ev.Lon != 78.96288 {
		t.Fatalf("lat/lon wrong: %.6f %.6f", ev.Lat, ev.Lon)
	}
}
