// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package asean_haze_hotspots

import "testing"

func TestEventsFromASMC(t *testing.T) {
	body := []byte(`[{"date":"2026-05-11","Thailand":5,"Myanmar":3,"ThailandLineColor":"#C72C95"},{"date":"2026-05-12","Thailand":0,"Myanmar":7}]`)
	evs, err := eventsFromASMC(defaultURL, body, []string{"Thailand", "Myanmar"}, "day", "High")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 4 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Props["hotspot_count"] != 5.0 || evs[0].Props["country_code"] != "TH" {
		t.Fatalf("bad event: %+v", evs[0])
	}
}

func TestBatches(t *testing.T) {
	got := batches([]string{"a", "b", "c", "d", "e", "f"}, 5)
	if len(got) != 2 || len(got[0]) != 5 || len(got[1]) != 1 {
		t.Fatalf("bad batches: %#v", got)
	}
}
