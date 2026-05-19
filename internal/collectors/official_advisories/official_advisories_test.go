// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package official_advisories

import "testing"

func TestParseSmartravellerArray(t *testing.T) {
	raw := []byte(`[{"id":"1","title":"Ukraine","url":"/destination/ukraine","summary":"Exercise a high degree of caution","changed":"2026-04-28T12:00:00Z","overall_advice_level":"Reconsider your need to travel"}]`)
	rows, err := parseSmartraveller(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Title != "Ukraine" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	lat, lon, country, ok := findCountry(rows[0].Title)
	if !ok || country != "Ukraine" || lat == 0 || lon == 0 {
		t.Fatalf("country lookup failed: %v %v %q %v", lat, lon, country, ok)
	}
}

func TestEmbassyAlertExtractsCitySeverityAndFlags(t *testing.T) {
	f := feedSpec{Name: "US Embassy Ukraine", SourceCountry: "US", TargetCountry: "UA", DefaultCity: "Kyiv", URL: "https://ua.usembassy.gov/category/alert/feed/"}
	ev := eventFromFeedItem(
		f,
		"Security Alert: U.S. Embassy Kyiv, Ukraine",
		"Location: Kyiv, Ukraine. Event: Missile and drone attacks are ongoing. Actions to Take: Shelter in place and monitor local media.",
		"https://ua.usembassy.gov/security-alert-kyiv/",
		"2026-04-30T10:00:00Z",
		"security-alert-kyiv",
	)
	if ev.Source != "official_advisories" || ev.ExtID == "" {
		t.Fatalf("unexpected event identity: %#v", ev)
	}
	if got := ev.Props["city"]; got != "Kyiv" {
		t.Fatalf("city = %v, want Kyiv", got)
	}
	if ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("expected city geocode, got lat=%v lon=%v", ev.Lat, ev.Lon)
	}
	if got := ev.Props["severity"]; got != "severe" {
		t.Fatalf("severity = %v, want severe", got)
	}
	if !hasString(ev.Props["action_flags"], "shelter_in_place") {
		t.Fatalf("missing shelter action: %#v", ev.Props["action_flags"])
	}
	if !hasString(ev.Props["hazard_flags"], "missile_or_drone") {
		t.Fatalf("missing missile hazard: %#v", ev.Props["hazard_flags"])
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestEmbassyAlertUsesLocationSectionBeforeDefaultCity(t *testing.T) {
	f := feedSpec{Name: "US Embassy Mexico", SourceCountry: "US", TargetCountry: "MX", DefaultCity: "Mexico City", URL: "https://mx.usembassy.gov/category/alert/feed/"}
	ev := eventFromFeedItem(
		f,
		"Demonstration Alert: U.S. Embassy Mexico",
		"Locations: Tijuana and nearby border crossings. Event: Demonstrations may cause roadblocks. Actions to Take: Avoid the area.",
		"https://mx.usembassy.gov/demonstration-alert/",
		"Thu, 30 Apr 2026 10:00:00 +0000",
		"demonstration-alert-tijuana",
	)
	if got := ev.Props["city"]; got != "Tijuana" {
		t.Fatalf("city = %v, want Tijuana", got)
	}
	if got := ev.Props["alert_kind"]; got != "demonstration" {
		t.Fatalf("alert_kind = %v, want demonstration", got)
	}
	if got := ev.Props["severity"]; got != "elevated" {
		t.Fatalf("severity = %v, want elevated", got)
	}
	if !hasString(ev.Props["action_flags"], "avoid_area") {
		t.Fatalf("missing avoid_area action: %#v", ev.Props["action_flags"])
	}
	if !hasString(ev.Props["hazard_flags"], "roadblocks") {
		t.Fatalf("missing roadblocks hazard: %#v", ev.Props["hazard_flags"])
	}
}

func hasString(v any, want string) bool {
	xs, ok := v.([]string)
	if !ok {
		return false
	}
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
