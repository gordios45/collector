// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package global_disaster_reports

import (
	"encoding/xml"
	"testing"
	"time"
)

func TestParseCharterActivations(t *testing.T) {
	raw := `activationId\":\"1029\",\"title\":\"Volcanic eruption in Philippines\",\"dateAsTimestamp\":1777766400000,\"centerPointLatitude\":\"13.226893875\",\"centerPointLongitude\":\"123.6795605\",\"country\":\"Philippines\",\"slug\":\"volcanic-eruption-in-philippines\",\"externalReferenceCode\":\"VO-2026-000001-PHL\",\"disasterTypes\":[{\"title\":\"Volcanic eruption\"}]`
	events := parseCharterActivations(raw, 10)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	ev := events[0]
	if ev.Source != sourceID || ev.ExtID != "charter:1029" || !ev.HasPoint() {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Props["disaster_type"] != "Volcanic eruption" {
		t.Fatalf("bad disaster type: %+v", ev.Props)
	}
}

func TestSmithsonianRSSParse(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?><rss version="2.0" xmlns:georss="http://www.georss.org/georss"><channel><item><title>Masaya - New Eruptive Activity</title><description>&lt;p&gt;Ash plume reported.&lt;/p&gt;</description><link>https://example.test</link><guid>g1</guid><pubDate>Thu, 30 Apr 2026 03:42:02 -0400</pubDate><georss:point>11.9844 -86.1688</georss:point></item></channel></rss>`)
	var parsed volcanoRSS
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Items) != 1 {
		t.Fatalf("expected one item, got %d", len(parsed.Items))
	}
	lat, lon, ok := parsePoint(parsed.Items[0].Point)
	if !ok || lat <= 0 || lon >= 0 {
		t.Fatalf("bad point: %f %f", lat, lon)
	}
}

func TestReliefWebEventsFromRowsSortsBeforeApplyingLimit(t *testing.T) {
	header := []string{
		"id", "status", "date-changed", "date-event", "date-created",
		"country-location-lat", "country-location-lon", "primary_country-location-lat", "primary_country-location-lon",
		"type-name", "primary_type-name", "name", "country-name", "primary_country-name",
		"country-iso3", "primary_country-iso3", "description", "glide", "type-code", "primary_type-code",
		"url_alias", "url",
	}
	rows := [][]string{header}
	for i := 0; i < 180; i++ {
		rows = append(rows, []string{
			"old", "past", "2021-12-07", "2021-09-06", "2021-09-06",
			"6.4", "2.3", "", "", "Flood", "", "Benin: Floods - Sep 2021", "Benin", "",
			"BEN", "", "Old flooding", "", "FL", "", "", "https://reliefweb.int/disaster/old",
		})
	}
	rows = append(rows, []string{
		"recent", "ongoing", "2026-05-06", "2026-03-25", "2026-03-25",
		"33.9", "67.7", "", "", "Flash Flood, Flood", "", "Afghanistan: Floods - Mar 2026", "Afghanistan", "",
		"AFG", "", "Recent flooding", "", "FL", "", "", "https://reliefweb.int/disaster/recent",
	})

	now := time.Date(2026, 5, 11, 15, 0, 0, 0, time.UTC)
	events := reliefWebEventsFromRows(rows, now, 180)
	if len(events) == 0 {
		t.Fatal("expected reliefweb events")
	}
	if got := events[0].Props["report_id"]; got != "recent" {
		t.Fatalf("first report_id = %v, want recent", got)
	}
	if want := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC); !events[0].Ts.Equal(want) {
		t.Fatalf("first ts = %s, want %s", events[0].Ts, want)
	}
	foundRecent := false
	for _, ev := range events {
		if ev.Props["report_id"] == "recent" {
			foundRecent = true
			break
		}
	}
	if !foundRecent {
		t.Fatal("recent row was dropped by limit")
	}
}
