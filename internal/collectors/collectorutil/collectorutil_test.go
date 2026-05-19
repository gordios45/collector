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
