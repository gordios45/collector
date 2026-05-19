// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package iata_fuel_monitor

import (
	"testing"
	"time"
)

func TestEventFromHTMLParsesFuelMonitorSummary(t *testing.T) {
	html := []byte(`<html><body>
<h3>Fuel Price Analysis</h3>
<p>The global average jet fuel price<span> </span><span>last week fell 0.2% compared to the week before to $162.55/bbl.</span></p>
</body></html>`)
	modified := time.Date(2026, 5, 18, 14, 4, 44, 0, time.UTC)
	ev, err := eventFromHTML(html, modified, pageURL)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source != sourceID || ev.ExtID != "global_average:2026-05-18" {
		t.Fatalf("bad identity: %#v", ev)
	}
	if got := ev.Props["price_usd_per_bbl"]; got != 162.55 {
		t.Fatalf("price = %#v", got)
	}
	if got := ev.Props["week_over_week_pct"]; got != -0.2 {
		t.Fatalf("week_over_week_pct = %#v", got)
	}
	if got := ev.Props["direction"]; got != "down" {
		t.Fatalf("direction = %#v", got)
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestParseWeekChangeUp(t *testing.T) {
	change, direction, ok := parseWeekChange("The global average jet fuel price last week rose 1.4% to $170.00/bbl.")
	if !ok {
		t.Fatal("change not found")
	}
	if change != 1.4 || direction != "up" {
		t.Fatalf("change %.1f direction %q", change, direction)
	}
}
