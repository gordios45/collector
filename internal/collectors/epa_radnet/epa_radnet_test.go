// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package epa_radnet

import "testing"

func TestEventFromCSVComputesRadNetMetrics(t *testing.T) {
	csv := "Header,Date,Value,Status\n" +
		"x,04/28/2026 10:00:00,80,APPROVED\n" +
		"x,04/28/2026 11:00:00,82,APPROVED\n" +
		"x,04/28/2026 12:00:00,100,APPROVED\n"
	ev, ok := eventFromCSV(sites[9], "https://example.test/radnet.csv", []byte(csv))
	if !ok {
		t.Fatal("expected event")
	}
	if ev.Source != "epa_radnet" || ev.ExtID == "" {
		t.Fatalf("bad event identity: %#v", ev)
	}
	if ev.Props["station_id"] != "us-seattle" || ev.Props["unit"] != "nSv/h" {
		t.Fatalf("unexpected props: %#v", ev.Props)
	}
	if ev.Props["severity"] == "" {
		t.Fatalf("missing severity: %#v", ev.Props)
	}
}
