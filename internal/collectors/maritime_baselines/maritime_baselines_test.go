// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package maritime_baselines

import (
	"testing"
	"time"
)

func TestParseMarineCadastreIndex(t *testing.T) {
	html := `<tr><td><a href="ais-2025-01-01.csv.zst">ais-2025-01-01.csv.zst</a></td><td class="date">2026-03-24 13:26</td><td class="size">192.8M</td></tr>
<tr><td><a href="ais-2025-01-02.csv.zst">ais-2025-01-02.csv.zst</a></td><td class="date">2026-03-24 13:26</td><td class="size">177.6M</td></tr>`
	files := parseMarineCadastreIndex("https://example.test/index.html", html)
	if len(files) != 2 {
		t.Fatalf("files=%d", len(files))
	}
	if files[0].Name != "ais-2025-01-01.csv.zst" || files[0].Size != "192.8M" {
		t.Fatalf("file=%#v", files[0])
	}
	if files[1].Date.Format("2006-01-02") != "2025-01-02" {
		t.Fatalf("date=%s", files[1].Date)
	}
}

func TestEventFromEMODnetInfo(t *testing.T) {
	raw := erddapInfo{}
	raw.Table.Rows = [][]string{
		{"attribute", "NC_GLOBAL", "institution", "String", "Flanders Marine Institute"},
		{"attribute", "NC_GLOBAL", "license", "String", "free redistribution"},
		{"attribute", "time", "actual_range", "double", "1.5778368E9, 1.6067808E9"},
		{"attribute", "vd", "units", "String", "seconds"},
	}
	ev, ok := eventFromEMODnetInfo(raw, time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("event not built")
	}
	if ev.Source != emodnetSourceID || ev.ExtID == "" {
		t.Fatalf("identity=%s/%s", ev.Source, ev.ExtID)
	}
	if ev.Props["variable_units"] != "seconds" {
		t.Fatalf("units=%v", ev.Props["variable_units"])
	}
}
