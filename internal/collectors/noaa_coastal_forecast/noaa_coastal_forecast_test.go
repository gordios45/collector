// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package noaa_coastal_forecast

import "testing"

func TestLatestProductDirsKeepsNewestPerModel(t *testing.T) {
	html := `<a href="cbofs.20260517/">cbofs.20260517/</a>
<a href="cbofs.20260518/">cbofs.20260518/</a>
<a href="stofs_2d_glo.20260518/">stofs_2d_glo.20260518/</a>`
	got := latestProductDirs(nosofsIndexURL, html)
	if len(got) != 2 {
		t.Fatalf("dirs=%d", len(got))
	}
	if got[0].Model != "cbofs" || got[0].Date != "20260518" {
		t.Fatalf("first=%#v", got[0])
	}
	if got[1].Model != "stofs_2d_glo" {
		t.Fatalf("second=%#v", got[1])
	}
}

func TestEventsFromIndex(t *testing.T) {
	html := `<a href="sfbofs.20260518/">sfbofs.20260518/</a>`
	evs := eventsFromIndex(nosofsIndexURL, html)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Source != sourceID || evs[0].ExtID != "sfbofs:20260518" {
		t.Fatalf("identity=%s/%s", evs[0].Source, evs[0].ExtID)
	}
	if evs[0].Lat == 0 || evs[0].Lon == 0 {
		t.Fatalf("expected model centroid, got %.2f %.2f", evs[0].Lat, evs[0].Lon)
	}
}
