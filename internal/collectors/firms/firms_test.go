// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package firms

import (
	"testing"
	"time"
)

func TestParseFIRMSCSVAddsFeedIdentity(t *testing.T) {
	buf := []byte(`latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight
10.1,20.2,351.5,0.4,0.4,2026-04-28,0130,N21,nominal,2.0NRT,300.1,12.5,N
`)
	evs, err := parseFIRMSCSV(feed{
		ID:         "VIIRS_NOAA21_NRT",
		URL:        "https://example.test/f.csv",
		Platform:   "NOAA-21",
		Instrument: "VIIRS",
		Product:    "noaa-21-viirs-c2",
	}, buf, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("events=%d, want 1", len(evs))
	}
	if evs[0].ExtID == "2026-04-28 0130_10.1_20.2" {
		t.Fatalf("ext id did not include feed identity: %q", evs[0].ExtID)
	}
	if got := evs[0].Props["data_id"]; got != "VIIRS_NOAA21_NRT" {
		t.Fatalf("data_id=%v", got)
	}
	if got := evs[0].Props["platform"]; got != "NOAA-21" {
		t.Fatalf("platform=%v", got)
	}
	if got := evs[0].Props["thermal_pass_id"]; got == "" {
		t.Fatal("missing thermal_pass_id")
	}
}
