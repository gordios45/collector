// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package geothermal

import (
	"testing"
	"time"
)

func TestParseCSV(t *testing.T) {
	buf := []byte(`latitude,longitude,acq_date,acq_time,satellite,confidence,frp
34.5,35.6,2026-04-28,0120,G16,nominal,21.4
`)
	evs, err := parseCSV(buf, "GOES_NRT", "world", "redacted", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("events=%d, want 1", len(evs))
	}
	if evs[0].Source != "geo_thermal" {
		t.Fatalf("source=%q", evs[0].Source)
	}
	if got := evs[0].Props["source_kind"]; got != "firms_geostationary_active_fire" {
		t.Fatalf("source_kind=%v", got)
	}
	if got := evs[0].Props["source_api_endpoint"]; got != "redacted" {
		t.Fatalf("endpoint=%v", got)
	}
}
