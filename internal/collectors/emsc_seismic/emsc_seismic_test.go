// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package emsc_seismic

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventsFromFeatures(t *testing.T) {
	raw := []byte(`{
	  "features": [{
	    "type": "Feature",
	    "geometry": {"type": "Point", "coordinates": [9.4046, 47.1071, 1.8]},
	    "id": "20260428_0000406",
	    "properties": {
	      "source_id": "1986386",
	      "source_catalog": "EMSC-RTS",
	      "lastupdate": "2026-04-28T21:53:17.018486Z",
	      "time": "2026-04-28T21:47:15.53939Z",
	      "flynn_region": "SWITZERLAND",
	      "lat": 47.1071,
	      "lon": 9.4046,
	      "depth": -1.8,
	      "evtype": "ke",
	      "auth": "ETHZ",
	      "mag": 1.2,
	      "magtype": "ml",
	      "unid": "20260428_0000406"
	    }
	  }]
	}`)
	var fc featureCollection
	if err := json.Unmarshal(raw, &fc); err != nil {
		t.Fatal(err)
	}
	evs := eventsFromFeatures(fc.Features, time.Date(2026, 4, 28, 22, 0, 0, 0, time.UTC))
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	ev := evs[0]
	if ev.Source != "emsc_seismic" || ev.ExtID != "20260428_0000406" {
		t.Fatalf("bad event identity: %#v", ev)
	}
	if ev.Lat != 47.1071 || ev.Lon != 9.4046 {
		t.Fatalf("bad location: %f,%f", ev.Lat, ev.Lon)
	}
	if ev.Props["flynn_region"] != "SWITZERLAND" || ev.Props["mag"] != 1.2 {
		t.Fatalf("props not preserved: %#v", ev.Props)
	}
}
