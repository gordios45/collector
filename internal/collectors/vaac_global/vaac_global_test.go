// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package vaac_global

import "testing"

func TestEventFromBlock(t *testing.T) {
	block := `Received FVAG01 at 05:09 UTC, 05/05/26 from SABM
VA ADVISORY
DTG: 20260505/0520Z
VAAC: BUENOS AIRES
VOLCANO: SABANCAYA 354006
PSN: S1547 W07150
AREA: PERU
ADVISORY NR: 2026/310
ERUPTION DETAILS: NO VA EMS
OBS VA DTG: 05/0500Z
OBS VA CLD: VA NOT IDENTIFIABLE FROM SATELLITE DATA
RMK: TEST
NXT ADVISORY: NO FURTHER ADVISORIES=`
	ev, ok := eventFromBlock(block)
	if !ok {
		t.Fatal("expected event")
	}
	if ev.Source != sourceID || ev.ExtID == "" || !ev.HasPoint() {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Props["volcano"] != "SABANCAYA" || ev.Props["vaac"] != "BUENOS AIRES" {
		t.Fatalf("bad props: %+v", ev.Props)
	}
	if ev.Lat >= 0 || ev.Lon >= 0 {
		t.Fatalf("expected southern/western coordinates, got %f %f", ev.Lat, ev.Lon)
	}
}

func TestAdvisoryBlocks(t *testing.T) {
	raw := `<html><body>Received FVXX at 01:00 UTC, 01/05/26 from TEST<br>VA ADVISORY<br>DTG: 20260501/0100Z<br>VAAC: TEST<br>VOLCANO: TEST 123<br>PSN: N0129 E12738<br>Received FVYY at 02:00 UTC, 01/05/26 from TEST<br>VA ADVISORY<br>DTG: 20260501/0200Z<br>VAAC: TEST<br>VOLCANO: TEST 123<br>PSN: N0129 E12738</body></html>`
	if got := len(advisoryBlocks(raw)); got != 2 {
		t.Fatalf("expected 2 blocks, got %d", got)
	}
}
