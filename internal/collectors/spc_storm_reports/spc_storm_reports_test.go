// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package spc_storm_reports

import (
	"testing"
	"time"
)

func TestParseCSV(t *testing.T) {
	raw := []byte(`Time,F_Scale,Location,County,State,Lat,Lon,Comments
0130,EF0,2 N Test,Example,TX,31.1,-97.2,brief tornado
Time,Speed,Location,County,State,Lat,Lon,Comments
1415,UNK,2 N Saint Jo,Montague,TX,33.72,-97.52,trees and power lines down
Time,Size,Location,County,State,Lat,Lon,Comments
1429,100,Leon,Love,OK,33.88,-97.43,quarter hail
`)
	rows, err := parseCSV(raw, time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC), "today", time.Date(2026, 4, 28, 15, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Props["type"] != "tornado" || rows[1].Props["type"] != "wind" || rows[2].Props["type"] != "hail" {
		t.Fatalf("types wrong: %+v %+v %+v", rows[0].Props, rows[1].Props, rows[2].Props)
	}
	if rows[1].Ts.Format(time.RFC3339) != "2026-04-28T14:15:00Z" {
		t.Fatalf("timestamp=%s", rows[1].Ts.Format(time.RFC3339))
	}
}

func TestParseReportTimePads(t *testing.T) {
	ts, ok := parseReportTime(time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC), "45")
	if !ok {
		t.Fatal("not parsed")
	}
	if ts.Format("15:04") != "00:45" {
		t.Fatalf("got %s", ts.Format("15:04"))
	}
}
