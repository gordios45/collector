// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package eia_wpsr

import "testing"

func TestEventsFromCSVParsesWPSRRows(t *testing.T) {
	spec := tableSpec{
		ID:   "table1",
		Name: "U.S. Petroleum Balance Sheet",
		URL:  "https://ir.eia.gov/wpsr/table1.csv",
	}
	buf := []byte(`"STUB_1","5/8/26","5/1/26","Difference","Percent Change","5/9/25","Difference","Percent Change"
"Crude Oil","836.971","849.882","-12.910","-1.500","841.480","-4.509","-0.500"
"Total Stocks (Including SPR)","1,620.349","1,634.013","-13.664","-0.800","1,617.795","2.554","0.200"
`)
	evs, err := eventsFromCSV(spec, buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("events len = %d, want 2", len(evs))
	}
	ev := evs[1]
	if ev.Source != sourceID || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("bad event identity: %#v", ev)
	}
	if got := ev.Props["metric"]; got != "Total Stocks (Including SPR)" {
		t.Fatalf("metric = %#v", got)
	}
	if got := ev.Props["value"]; got != 1620.349 {
		t.Fatalf("value = %#v", got)
	}
	if got := ev.Props["unit"]; got != "million_barrels" {
		t.Fatalf("unit = %#v", got)
	}
	comparisons, ok := ev.Props["comparisons"].(map[string]any)
	if !ok {
		t.Fatalf("comparisons type = %T", ev.Props["comparisons"])
	}
	if got := comparisons["value_2026_05_01"]; got != 1634.013 {
		t.Fatalf("previous week = %#v", got)
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestEventsFromCSVParsesMultiStubRows(t *testing.T) {
	spec := tableSpec{
		ID:   "table9",
		Name: "U.S. and PAD District Weekly Estimates",
		URL:  "https://ir.eia.gov/wpsr/table9.csv",
	}
	buf := []byte(`"STUB_1","STUB_2","5/8/26","5/1/26","5/9/25"
"Crude Oil Production ","(1)     Domestic Production","13,710","13,573","13,387"
"Refiner Inputs and Utilization ","Percent Utilization","91.7","90.1","90.2"
`)
	evs, err := eventsFromCSV(spec, buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("events len = %d, want 2", len(evs))
	}
	if got := evs[0].Props["category"]; got != "Crude Oil Production" {
		t.Fatalf("category = %#v", got)
	}
	if got := evs[0].Props["metric"]; got != "Domestic Production" {
		t.Fatalf("metric = %#v", got)
	}
	if got := evs[1].Props["unit"]; got != "percent" {
		t.Fatalf("unit = %#v", got)
	}
}
