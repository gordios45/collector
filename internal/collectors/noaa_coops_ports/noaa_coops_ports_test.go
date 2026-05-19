// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package noaa_coops_ports

import (
	"testing"
)

func TestApplyProductProps(t *testing.T) {
	props := map[string]any{}
	applyProductProps(props, "water_level", map[string]string{
		"v": "0.774",
		"s": "0.047",
		"f": "1,0,0,0",
		"q": "p",
	})
	if got := props["water_level_m"]; got != 0.774 {
		t.Fatalf("water_level_m=%v", got)
	}
	if got := props["water_level_quality"]; got != "p" {
		t.Fatalf("quality=%v", got)
	}

	applyProductProps(props, "wind", map[string]string{
		"s":  "7.7",
		"d":  "356.0",
		"dr": "N",
		"g":  "9.3",
	})
	if got := props["wind_speed_m_s"]; got != 7.7 {
		t.Fatalf("wind_speed_m_s=%v", got)
	}
	if got := props["wind_direction_cardinal"]; got != "N" {
		t.Fatalf("wind_direction_cardinal=%v", got)
	}
}

func TestParseStations(t *testing.T) {
	got := parseStations("123|Port One|1.5|2.5,bad,456|Port Two|-3|4")
	if len(got) != 2 {
		t.Fatalf("stations=%d", len(got))
	}
	if got[0].ID != "123" || got[0].Lat != 1.5 || got[1].Lon != 4 {
		t.Fatalf("stations=%#v", got)
	}
}
