// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package rf_presence

import "testing"

func TestEventFromRow(t *testing.T) {
	row := aggregateRow{
		Role:         "receiver",
		Bucket:       "2026-04-27 22:15:00",
		Lat:          42.4,
		Lon:          -83.9,
		Spots:        1928,
		Receivers:    4,
		Transmitters: 287,
		Bands:        10,
		AvgSNR:       -15.03,
		FirstSeen:    "2026-04-27 22:16:00",
		LastSeen:     "2026-04-27 22:28:00",
	}
	ev, ok := eventFromRow(row, 30)
	if !ok {
		t.Fatal("row rejected")
	}
	if ev.Source != "rf_presence" {
		t.Fatalf("source = %q, want rf_presence", ev.Source)
	}
	if ev.Ts.Format("2006-01-02T15:04:05Z") != "2026-04-27T22:28:00Z" {
		t.Fatalf("ts = %s", ev.Ts.Format("2006-01-02T15:04:05Z"))
	}
	if ev.Props["role"] != "receiver" {
		t.Fatalf("role = %#v", ev.Props["role"])
	}
	if ev.Props["spot_density_score"].(float64) <= 2 {
		t.Fatalf("spot density score too low: %#v", ev.Props["spot_density_score"])
	}
}

func TestScoreHelpers(t *testing.T) {
	if got := spotDensityScore(0); got != 0 {
		t.Fatalf("zero density = %.2f, want 0", got)
	}
	if got := spotDensityScore(2000); got <= 2 || got > 3 {
		t.Fatalf("density score = %.2f, want high bounded", got)
	}
	if got := diversityScore(1); got <= 0 || got >= 1 {
		t.Fatalf("single diversity = %.2f, want weak positive", got)
	}
	if got := bandDiversityScore(12); got != 3 {
		t.Fatalf("band diversity = %.2f, want 3", got)
	}
}
