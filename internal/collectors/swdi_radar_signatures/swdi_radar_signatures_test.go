// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package swdi_radar_signatures

import (
	"testing"
	"time"

	"github.com/gordios45/collector/internal/events"
)

func TestEventsFromCSVTVS(t *testing.T) {
	buf := []byte("ZTIME,WSR_ID,CELL_ID,CELL_TYPE,RANGE,AZIMUTH,MAX_SHEAR,MXDV,LAT,LON\n2026-04-27T00:00:52Z,KVNX,K1,TVS,62,88,16,63,36.770,-96.842\n")
	evs, err := eventsFromCSV("nx3tvs", "https://example.test", buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	ev := evs[0]
	if ev.ExtID != "nx3tvs:2026-04-27T00:00:52Z:KVNX:K1" {
		t.Fatalf("bad ext id: %s", ev.ExtID)
	}
	if ev.Props["dataset"] != "nx3tvs" || ev.Props["cell_type"] != "TVS" {
		t.Fatalf("bad props: %#v", ev.Props)
	}
	if ev.Props["tvs_score"] != 2.5 || ev.Props["radar_severity_score"] != 2.5 {
		t.Fatalf("missing radar scores: %#v", ev.Props)
	}
}

func TestEventsFromCSVSummary(t *testing.T) {
	evs, err := eventsFromCSV("nx3meso", "https://example.test", []byte("summary\ncount,0\ntotalTimeInSeconds,0.0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("got %d events", len(evs))
	}
}

func TestRecentRowsKeepsLatestWithinWindow(t *testing.T) {
	base := time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
	rows := []events.Event{
		{Ts: base.Add(-30 * time.Hour), ExtID: "old"},
		{Ts: base.Add(-2 * time.Hour), ExtID: "a"},
		{Ts: base.Add(-1 * time.Hour), ExtID: "b"},
		{Ts: base, ExtID: "c"},
	}
	got := recentRows(rows, base.Add(-24*time.Hour), 2)
	if len(got) != 2 || got[0].ExtID != "b" || got[1].ExtID != "c" {
		t.Fatalf("bad rows: %#v", got)
	}
}
