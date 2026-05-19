// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package collectorutil

import (
	"math"
	"testing"
)

func TestSourceLocalScores(t *testing.T) {
	cases := []struct {
		name string
		got  float64
		want float64
	}{
		{"bgp", BGPOutageSeverity(0.7855410730524401, 1415), 1.375671415580479},
		{"ris", RISRoutingInstabilityScore(34, 0, 0), 1.422139751025119},
		{"cloudflare_outage", CloudflareOutageScore("outage", "", "", 0), 2},
		{"cloudflare_rank", CloudflareOutageScore("", "", "", 3), 1.8},
		{"gps", GPSJammingIntensityScore(0.1, 10), 0.75},
		{"seismic_mag", SeismicMagnitudeScore(6.5), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if math.Abs(tc.got-tc.want) > 0.0001 {
				t.Fatalf("score=%v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestAlertScores(t *testing.T) {
	props := map[string]any{
		"event":     "Flash Flood Warning",
		"severity":  "Extreme",
		"urgency":   "Immediate",
		"certainty": "Observed",
	}
	AddAlertScores(props)
	AddNWSHazardScores(props)

	if props["severity_score"] != float64(3) {
		t.Fatalf("severity_score=%v", props["severity_score"])
	}
	if math.Abs(props["official_alert_score"].(float64)-2.59) > 0.0001 {
		t.Fatalf("official_alert_score=%v", props["official_alert_score"])
	}
	if props["flash_flood_score"] != float64(3) {
		t.Fatalf("flash_flood_score=%v", props["flash_flood_score"])
	}
	if math.Abs(props["flood_score"].(float64)-2.79) > 0.0001 {
		t.Fatalf("flood_score=%v", props["flood_score"])
	}
}

func TestWeatherGeologicalAndAirspaceScores(t *testing.T) {
	t.Run("spc wind damage", func(t *testing.T) {
		props := map[string]any{"type": "wind", "magnitude": "60", "comments": "trees down"}
		AddSPCStormReportScores(props)
		if math.Abs(props["wind_damage_score"].(float64)-1.5) > 0.0001 {
			t.Fatalf("wind_damage_score=%v", props["wind_damage_score"])
		}
		if props["measured_severe_score"] != props["wind_damage_score"] {
			t.Fatalf("measured=%v wind=%v", props["measured_severe_score"], props["wind_damage_score"])
		}
	})

	t.Run("swdi hail", func(t *testing.T) {
		props := map[string]any{"dataset": "nx3hail", "cell_type": "HAIL", "max_size": 2.5}
		AddSWDIRadarScores(props)
		if math.Abs(props["hail_signature_score"].(float64)-2.2) > 0.0001 {
			t.Fatalf("hail_signature_score=%v", props["hail_signature_score"])
		}
		if props["radar_severity_score"] != props["hail_signature_score"] {
			t.Fatalf("radar severity=%v", props["radar_severity_score"])
		}
	})

	t.Run("tropical cyclone", func(t *testing.T) {
		props := map[string]any{"classification": "Hurricane", "intensity": "90 mph", "pressure": "970"}
		AddTropicalCycloneScores(props, true)
		if props["tc_intensity_score"] != 1.7 {
			t.Fatalf("tc_intensity_score=%v", props["tc_intensity_score"])
		}
		if math.Abs(props["low_pressure_score"].(float64)-0.8571428571) > 0.0001 {
			t.Fatalf("low_pressure_score=%v", props["low_pressure_score"])
		}
		if props["cone_score"] != float64(1) {
			t.Fatalf("cone_score=%v", props["cone_score"])
		}
	})

	t.Run("geological official products", func(t *testing.T) {
		emsc := map[string]any{"mag": 3.0, "depth": 1.0, "evtype": "quarry blast"}
		AddEMSCSeismicScores(emsc)
		if math.Abs(emsc["blast_like_score"].(float64)-2.9) > 0.0001 {
			t.Fatalf("blast_like_score=%v", emsc["blast_like_score"])
		}
		normal := map[string]any{"mag": 4.4, "depth": 19.0, "evtype": "ke"}
		AddEMSCSeismicScores(normal)
		if _, ok := normal["blast_like_score"]; ok {
			t.Fatalf("ordinary earthquake should not get blast_like_score: %#v", normal)
		}
		shakemap := map[string]any{"mag": 6.2, "shakemap_maxmmi_grid": 7.5, "pager_alertlevel": "orange", "tsunami": 1}
		AddUSGSShakeMapScores(shakemap)
		if shakemap["pager_alert_score"] != 2.3 || shakemap["tsunami_flag"] != 1.5 {
			t.Fatalf("shakemap scores=%#v", shakemap)
		}
		tsunami := map[string]any{"title": "Tsunami Warning", "magnitude": "7.0"}
		AddNOAATsunamiScores(tsunami)
		if tsunami["alert_score"] != float64(3) || math.Abs(tsunami["magnitude_score"].(float64)-3.6) > 0.0001 {
			t.Fatalf("tsunami scores=%#v", tsunami)
		}
	})

	t.Run("volcano and vaac", func(t *testing.T) {
		notice := map[string]any{"alert_level": "advisory", "color_code": "yellow"}
		AddVolcanoNoticeScores(notice)
		if notice["alert_score"] != 1.3 || notice["aviation_color_score"] != 1.2 {
			t.Fatalf("notice scores=%#v", notice)
		}
		vaac := map[string]any{"eruption_details": "eruption reported", "obs_va_cld": "ash observed", "upper_limit_fl": "FL250"}
		AddVAACScores(vaac)
		if math.Abs(vaac["ash_score"].(float64)-2.8) > 0.0001 {
			t.Fatalf("ash_score=%v", vaac["ash_score"])
		}
	})

	t.Run("faa status", func(t *testing.T) {
		props := map[string]any{
			"category":    "closure",
			"reason":      "weather",
			"valid_start": "2026-04-01T00:00:00Z",
			"valid_end":   "2026-04-09T00:00:00Z",
		}
		AddFAAStatusScores(props)
		if props["closure_score"] != 2.4 || props["weather_impact_score"] != 2.4 || props["standing_restriction_score"] != 1.6 {
			t.Fatalf("faa scores=%#v", props)
		}
		compact := map[string]any{
			"category": "closure",
			"reason":   "!LAS 04/141 LAS AD AP CLSD 2604281710-2607292300",
		}
		AddFAAStatusScores(compact)
		if compact["standing_restriction_score"] != 2.4 {
			t.Fatalf("compact validity standing score=%#v", compact)
		}
	})
}
