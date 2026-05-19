// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package nhc_gis_cones

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestEventsFromKML(t *testing.T) {
	kml := []byte(`<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Placemark>
	  <name>Cone</name>
	  <MultiGeometry>
	    <Polygon><outerBoundaryIs><LinearRing><coordinates>
	      -80,25,0 -79,25,0 -79,26,0 -80,26,0 -80,25,0
	    </coordinates></LinearRing></outerBoundaryIs></Polygon>
	  </MultiGeometry>
	</Placemark></Document></kml>`)
	evs := eventsFromKML(kml, storm{ID: "AL012026", BinNumber: "001", Name: "TEST", LastUpdate: "2026-06-01T12:00:00Z"}, "https://example.test/cone.kmz")
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	if evs[0].Source != "nhc_gis_cones" || evs[0].Geom == "" {
		t.Fatalf("bad event: %#v", evs[0])
	}
	if evs[0].Props["storm_id"] != "AL012026" || evs[0].Props["product_type"] != "forecast_cone" {
		t.Fatalf("bad props: %#v", evs[0].Props)
	}
}

func TestKMLBytesFromKMZ(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("doc.kml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("<kml/>")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := kmlBytes(buf.Bytes(), "https://example.test/cone.kmz")
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "<kml/>" {
		t.Fatalf("bad kml: %q", string(out))
	}
}
