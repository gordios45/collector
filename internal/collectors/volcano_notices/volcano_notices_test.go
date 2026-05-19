// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package volcano_notices

import "testing"

func TestParseSection(t *testing.T) {
	raw := `<b>GREAT SITKIN</b> (VNUM #311120)<br/>52°04'35" N 176°06'39" W, Summit Elevation 5709 ft (1740 m)<br/>Current Volcano Alert Level: WATCH<br/>Current Aviation Color Code: ORANGE<br/>`
	v, ok := parseSection(raw)
	if !ok {
		t.Fatal("section not parsed")
	}
	if v.Name != "Great Sitkin" || v.VNum != "311120" {
		t.Fatalf("identity wrong: %+v", v)
	}
	if v.AlertLevel != "WATCH" || v.ColorCode != "ORANGE" {
		t.Fatalf("alert/color wrong: %+v", v)
	}
	if v.Lat < 52.07 || v.Lat > 52.08 || v.Lon > -176.10 || v.Lon < -176.12 {
		t.Fatalf("lat/lon wrong: %.6f %.6f", v.Lat, v.Lon)
	}
}

func TestEventsFromDetail(t *testing.T) {
	d := noticeDetail{
		NoticeIdentifier: "DOI-USGS-AVO-2026-04-28T19:07:33+00:00",
		NoticeTitle:      "ALASKA VOLCANO OBSERVATORY DAILY UPDATE",
		SentUnixTime:     1777406404,
		ObsFullName:      "Alaska Volcano Observatory",
		Sections: []noticeSection{{
			SectionHTML: `<b>SHISHALDIN</b> (VNUM #311360)<br/>54°45'19" N 163°58'16" W, Summit Elevation 9373 ft (2857 m)<br/>Current Volcano Alert Level: ADVISORY<br/>Current Aviation Color Code: YELLOW<br/>`,
			Summary:     `<p>Unrest persists.</p>`,
		}},
	}
	evs := eventsFromDetail(d)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Source != "volcano_notices" {
		t.Fatalf("source=%q", evs[0].Source)
	}
	if evs[0].Props["volcano_name"] != "Shishaldin" {
		t.Fatalf("name=%v", evs[0].Props["volcano_name"])
	}
}
