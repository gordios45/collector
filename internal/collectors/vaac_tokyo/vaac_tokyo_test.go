// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package vaac_tokyo

import "testing"

func TestEventFromTextPage(t *testing.T) {
	raw := `<!-- VAA Text Start -->FVFE01 RJTD 282100<BR>VA ADVISORY<BR>DTG: 20260428/2100Z<BR>VAAC: TOKYO<BR>VOLCANO: MAYON 273030<BR>PSN: N1315 E12341<BR>AREA: PHILIPPINES<BR>SOURCE ELEV: 2462M AMSL<BR>ADVISORY NR: 2026/539<BR>INFO SOURCE: HIMAWARI-9 PHIVOLCS<BR>ERUPTION DETAILS: ERUPTION AT 20260428/2039Z VA CLD UNKNOWN REPORTED<BR>OBS VA DTG: 28/2040Z<BR>OBS VA CLD: VA NOT IDENTIFIABLE FM SATELLITE DATA WIND FL180 090/10KT<BR>NXT ADVISORY: NO FURTHER ADVISORIES=<BR><!-- VAA Text End -->`
	ev, ok, err := eventFromTextPage(raw, "https://example.test/vaa.html")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("event skipped")
	}
	if ev.Source != "vaac_tokyo" {
		t.Fatalf("source=%q", ev.Source)
	}
	if ev.Lat < 13.24 || ev.Lat > 13.26 || ev.Lon < 123.68 || ev.Lon > 123.69 {
		t.Fatalf("lat/lon wrong: %.6f %.6f", ev.Lat, ev.Lon)
	}
	if ev.Props["volcano"] != "MAYON" || ev.Props["advisory_nr"] != "2026/539" {
		t.Fatalf("props wrong: %+v", ev.Props)
	}
}

func TestLatestLinksDedupes(t *testing.T) {
	got := latestLinks(`<a href="TextData/2026/20260428_27303000_0539_Text.html">A</a><a href="TextData/2026/20260428_27303000_0539_Text.html">A</a><a href="TextData/2026/20260428_27303000_0540_Text.html">B</a>`)
	if len(got) != 2 || got[0] != "TextData/2026/20260428_27303000_0539_Text.html" || got[1] != "TextData/2026/20260428_27303000_0540_Text.html" {
		t.Fatalf("links=%v", got)
	}
}
