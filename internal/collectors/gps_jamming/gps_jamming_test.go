// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gps_jamming

import "testing"

func TestParseGPSJamCSV(t *testing.T) {
	const sample = `hex,count_good_aircraft,count_bad_aircraft
8444113ffffffff,90,10
8444111ffffffff,15,0
bad-h3,3,2
`
	evs, err := parseGPSJamCSV([]byte(sample), "2026-04-23")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Source != "gps_jamming" {
		t.Fatalf("source = %q, want gps_jamming", ev.Source)
	}
	if ev.Ts.Format("2006-01-02") != "2026-04-23" {
		t.Fatalf("date = %s", ev.Ts.Format("2006-01-02"))
	}
	if got := ev.Props["count_bad_aircraft"]; got != 10 {
		t.Fatalf("bad count = %#v, want 10", got)
	}
	if got := ev.Props["intensity"]; got != 0.1 {
		t.Fatalf("intensity = %#v, want 0.1", got)
	}
	if got := ev.Props["intensity_score"]; got != 0.75 {
		t.Fatalf("intensity_score = %#v, want 0.75", got)
	}
	if ev.Lat == 0 && ev.Lon == 0 {
		t.Fatal("expected H3 centroid lat/lon")
	}
}

func TestParseGPSJamCSVRequiresHex(t *testing.T) {
	_, err := parseGPSJamCSV([]byte("count_good_aircraft,count_bad_aircraft\n1,2\n"), "2026-04-23")
	if err == nil {
		t.Fatal("expected missing hex error")
	}
}
