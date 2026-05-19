// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package bmkg_indonesia

import "testing"

func TestEventsFromFeedSingleQuake(t *testing.T) {
	body := []byte(`{"Infogempa":{"gempa":{"Tanggal":"15 Mei 2026","Jam":"00:53:15 WIB","DateTime":"2026-05-14T17:53:15+00:00","Coordinates":"-6.09,130.56","Lintang":"6.09 LS","Bujur":"130.56 BT","Magnitude":"6.7","Kedalaman":"163 km","Wilayah":"224 km BaratLaut TANIMBAR","Potensi":"Tidak berpotensi tsunami"}}}`)
	evs, err := eventsFromFeed(feedSpec{Name: "autogempa", URL: "https://example.test/autogempa.json"}, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Source != sourceID || evs[0].Lat != -6.09 || evs[0].Lon != 130.56 {
		t.Fatalf("unexpected event: %+v", evs[0])
	}
	if evs[0].Props["magnitude"] != 6.7 {
		t.Fatalf("magnitude not parsed: %#v", evs[0].Props["magnitude"])
	}
}

func TestEventsFromFeedQuakeList(t *testing.T) {
	body := []byte(`{"Infogempa":{"gempa":[{"DateTime":"2026-05-14T17:53:15+00:00","Coordinates":"-6.09,130.56","Magnitude":"6.7","Kedalaman":"163 km","Wilayah":"A"},{"DateTime":"2026-05-13T10:00:00+00:00","Coordinates":"-1.2,120.1","Magnitude":"5.1","Kedalaman":"10 km","Wilayah":"B"}]}}`)
	evs, err := eventsFromFeed(feedSpec{Name: "gempaterkini", URL: "https://example.test/gempaterkini.json"}, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[1].Props["depth_km"] != 10.0 {
		t.Fatalf("depth not parsed: %#v", evs[1].Props["depth_km"])
	}
}
