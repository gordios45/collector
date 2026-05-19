// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package wmo_cap_alert_areas

import "testing"

func TestEventsFromCAPPolygon(t *testing.T) {
	buf := []byte(`<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
	  <identifier>cap-1</identifier>
	  <sender>met.example</sender>
	  <sent>2026-04-28T10:00:00Z</sent>
	  <status>Actual</status>
	  <msgType>Alert</msgType>
	  <scope>Public</scope>
	  <info>
	    <language>en-US</language>
	    <category>Met</category>
	    <event>Severe thunderstorm</event>
	    <urgency>Expected</urgency>
	    <severity>Severe</severity>
	    <certainty>Likely</certainty>
	    <effective>2026-04-28T10:00:00Z</effective>
	    <expires>2026-04-28T12:00:00Z</expires>
	    <headline>Severe thunderstorm warning</headline>
	    <area>
	      <areaDesc>Test area</areaDesc>
	      <polygon>10.0,20.0 10.0,21.0 11.0,21.0 11.0,20.0</polygon>
	    </area>
	  </info>
	</alert>`)
	item := alertItem{ID: "item-1", Event: "Thunderstorm", Severity: 2}
	evs, err := eventsFromCAP(buf, item, member{MID: "001", Name: "Example", Lat: 1, Lng: 2}, "https://example.test/cap.xml", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	ev := evs[0]
	if ev.ExtID != "cap-1:0:0" || ev.Geom == "" {
		t.Fatalf("bad event: %#v", ev)
	}
	if ev.Props["areaDesc"] != "Test area" || ev.Props["geometry_source"] != "cap_polygon" {
		t.Fatalf("bad props: %#v", ev.Props)
	}
}

func TestEventsFromCAPFallbackCentroid(t *testing.T) {
	buf := []byte(`<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
	  <identifier>cap-2</identifier>
	  <sent>2026-04-28T10:00:00Z</sent>
	  <info><event>Rainstorm</event><severity>Severe</severity><area><areaDesc>No polygon</areaDesc></area></info>
	</alert>`)
	evs, err := eventsFromCAP(buf, alertItem{ID: "item-2", Severity: 2}, member{Lat: 45, Lng: 9}, "https://example.test/cap.xml", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Props["geometry_source"] != "member_centroid" || evs[0].Lat != 45 || evs[0].Lon != 9 {
		t.Fatalf("bad fallback: %#v", evs[0])
	}
}
