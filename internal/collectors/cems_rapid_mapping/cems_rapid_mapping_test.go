// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package cems_rapid_mapping

import "testing"

func TestEventFromActivation(t *testing.T) {
	ev, ok := eventFromActivation(activation{
		Code:           "EMSR872",
		Countries:      []string{"Micronesia"},
		EventTime:      "2026-04-08T22:00:00",
		Name:           "Tropical Cyclone Sinlaku in Micronesia",
		Centroid:       "POINT (151.75069008400766 7.375840981096835)",
		ActivationTime: "2026-04-21T14:34:00",
		Category:       "Storm",
		LastUpdate:     "2026-04-28T17:07:10.281181",
		NAOIs:          19,
		NProducts:      16,
	})
	if !ok {
		t.Fatal("activation skipped")
	}
	if ev.Source != "cems_rapid_mapping" || ev.ExtID != "EMSR872" {
		t.Fatalf("identity wrong: %+v", ev)
	}
	if ev.Lat < 7.37 || ev.Lat > 7.38 || ev.Lon < 151.75 || ev.Lon > 151.76 {
		t.Fatalf("lat/lon wrong: %.6f %.6f", ev.Lat, ev.Lon)
	}
	if ev.Props["category"] != "Storm" || ev.Props["n_products"] != 16 {
		t.Fatalf("props wrong: %+v", ev.Props)
	}
}
