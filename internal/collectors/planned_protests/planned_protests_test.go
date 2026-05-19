// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package planned_protests

import "testing"

func TestParseICSUnfoldsAndUnescapesEvents(t *testing.T) {
	raw := []byte("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-1\r\nDTSTART:20260506T180000Z\r\nSUMMARY:Transit Rally\r\nDESCRIPTION:Line one\\n line two\\, with comma\r\nLOCATION:Union Station\\, Washington DC\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	items := parseICS(raw)
	if len(items) != 1 {
		t.Fatalf("len(items)=%d, want 1", len(items))
	}
	if items[0]["UID"] != "test-1" {
		t.Fatalf("UID=%q", items[0]["UID"])
	}
	if got := items[0]["LOCATION"]; got != "Union Station, Washington DC" {
		t.Fatalf("LOCATION=%q", got)
	}
	if parseICSTime(items[0]["DTSTART"]).IsZero() {
		t.Fatal("DTSTART did not parse")
	}
}
