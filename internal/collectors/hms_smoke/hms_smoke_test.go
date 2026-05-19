// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package hms_smoke

import (
	"testing"
	"time"
)

func TestEventsFromKML(t *testing.T) {
	raw := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
<name>HMS Smoke Mapping-20260428</name>
<Folder>
<name>Smoke (Light)</name>
<Placemark>
<description><![CDATA[<div>Start Time: 2026118 1200UTC<br>End Time: 2026118 1500UTC<br>Density: Light<br>Satellite: GOES-WEST</div>]]></description>
<styleUrl>#Smoke_Light_style</styleUrl>
<Polygon><outerBoundaryIs><LinearRing><coordinates>
-101.0,32.0,0 -100.0,32.0,0 -100.0,33.0,0 -101.0,32.0,0
</coordinates></LinearRing></outerBoundaryIs></Polygon>
</Placemark>
</Folder>
</Document>
</kml>`)
	rows, err := eventsFromKML(raw, time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC), "https://example.test/hms.kml")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	ev := rows[0]
	if ev.Source != "hms_smoke" || ev.Props["density"] != "Light" || ev.Props["satellite"] != "GOES-WEST" {
		t.Fatalf("event wrong: %+v", ev)
	}
	if ev.Ts.Format(time.RFC3339) != "2026-04-28T15:00:00Z" {
		t.Fatalf("ts=%s", ev.Ts.Format(time.RFC3339))
	}
	if ev.Geom == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("geometry not set: %+v", ev)
	}
}

func TestParseHMSJulianTime(t *testing.T) {
	got := parseHMSJulianTime("2026118 1500UTC")
	if got.Format(time.RFC3339) != "2026-04-28T15:00:00Z" {
		t.Fatalf("got %s", got.Format(time.RFC3339))
	}
}
