// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package unhcr_displacement

import "testing"

func TestEventsFromItemsAggregatesDisplacement(t *testing.T) {
	items := []unhcrItem{{
		COOISO: "SDN", COOName: "Sudan", COAISO: "TCD", COAName: "Chad",
		Refugees: "1000", AsylumSeekers: "50", IDPs: "2000", Stateless: "0",
	}}
	evs := eventsFromItems(items, 2025)
	if len(evs) < 1 {
		t.Fatal("expected events")
	}
	var sudan map[string]any
	for _, ev := range evs {
		if ev.Props["country_code"] == "SDN" {
			sudan = ev.Props
		}
	}
	if sudan == nil || sudan["total_displaced"].(int) != 3050 {
		t.Fatalf("bad Sudan aggregate: %#v", sudan)
	}
}
