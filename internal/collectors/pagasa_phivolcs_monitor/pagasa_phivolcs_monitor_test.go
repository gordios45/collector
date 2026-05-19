// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package pagasa_phivolcs_monitor

import "testing"

func TestParsePHIVOLCSEarthquakes(t *testing.T) {
	raw := `<table><tr>
<td><a href="2026_Earthquake_Information\May\2026_0518_2000_B1.html">19 May 2026 - 04:00 AM</a></td>
<td>07.88</td><td>123.30</td><td>033</td><td>1.4</td>
<td>010 km N 44 W of Dumalinao (Zamboanga Del Sur)</td>
</tr></table>`
	rows, err := parsePHIVOLCSEarthquakes(raw, phivolcsQuakeURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows", len(rows))
	}
	if rows[0].Lat != 7.88 || rows[0].DepthKM != 33 {
		t.Fatalf("bad row: %+v", rows[0])
	}
	evs := earthquakeEvents(rows)
	if len(evs) != 1 || evs[0].Props["hazard_type"] != "earthquake" {
		t.Fatalf("bad events: %+v", evs)
	}
}

func TestParseVolcanoStatuses(t *testing.T) {
	rows := parseVolcanoStatuses(`<span>Alert Level Status:</span><span>Taal - 1</span> <span>Kanlaon - 2</span>`)
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	if rows[1].Name != "Kanlaon" || rows[1].Level != 2 {
		t.Fatalf("bad rows: %+v", rows)
	}
}

func TestPAGASANoActiveEvent(t *testing.T) {
	evs := pagasaEvents(`<h3>No Active Tropical Cyclone within the Philippine Area of Responsibility</h3>`, pagasaTCURL)
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Props["active_tropical_cyclone"] != false {
		t.Fatalf("bad event: %+v", evs[0])
	}
}
