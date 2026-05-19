// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package ibtracs

import (
	"testing"
	"time"
)

func TestParseCSV(t *testing.T) {
	const sample = `SID,SEASON,BASIN,SUBBASIN,NAME,ISO_TIME,NATURE,LAT,LON,WMO_WIND,WMO_PRES,USA_WIND,USA_PRES,WMO_AGENCY
,,,,,,,,,,,,,
2026117N08137,2026,WP,MM,JONGDARI,2026-04-27 06:00:00,TS,12.3,138.4,45,,50,990,RJTD
2020001S01010,2020,SI,MM,OLD,2020-01-01 00:00:00,TS,-1.0,10.0,35,1000,,,`
	now := time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC)
	evs, err := parseCSV([]byte(sample), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Source != "ibtracs" {
		t.Fatalf("source = %q, want ibtracs", ev.Source)
	}
	if ev.ExtID != "2026117N08137:20260427T0600" {
		t.Fatalf("extID = %q", ev.ExtID)
	}
	if ev.Lat != 12.3 || ev.Lon != 138.4 {
		t.Fatalf("lat/lon = %.1f/%.1f", ev.Lat, ev.Lon)
	}
	if got := ev.Props["wind_kt"]; got != 45 {
		t.Fatalf("wind = %#v, want 45", got)
	}
	if got := ev.Props["pres_mb"]; got != 990 {
		t.Fatalf("pressure = %#v, want USA_PRES fallback 990", got)
	}
}

func TestParseCSVRequiresCoreColumns(t *testing.T) {
	_, err := parseCSV([]byte("SID,ISO_TIME,LAT\n,,\n"), time.Now())
	if err == nil {
		t.Fatal("expected required-column error")
	}
}
