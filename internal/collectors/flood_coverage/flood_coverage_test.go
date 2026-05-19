// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package flood_coverage

import (
	"math"
	"strings"
	"testing"
)

func TestFloodTile(t *testing.T) {
	tests := []struct {
		name string
		lat  float64
		lon  float64
		want string
	}{
		{name: "luzon", lat: 14.8, lon: 120.7, want: "h30v07"},
		{name: "greenwich", lat: 0, lon: 0, want: "h18v09"},
		{name: "clamped north west", lat: 95, lon: -190, want: "h00v00"},
		{name: "clamped south east", lat: -95, lon: 190, want: "h35v17"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floodTile(tt.lat, tt.lon); got != tt.want {
				t.Fatalf("floodTile() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSummarizeDischarge(t *testing.T) {
	times := []string{"2026-04-29", "2026-04-30", "2026-05-01"}
	values := []float64{100, 450, 200}
	means := []float64{100, 100, 100}
	medians := []float64{80, 80, 80}
	maxima := []float64{200, 200, 200}
	m := summarizeDischarge(times, values, means, medians, maxima)
	if !m.OK {
		t.Fatal("metric not OK")
	}
	if m.Peak != 450 || m.PeakDate != "2026-04-30" {
		t.Fatalf("peak = %.1f %s", m.Peak, m.PeakDate)
	}
	if m.Ratio <= 4.0 {
		t.Fatalf("ratio = %.2f, expected strong anomaly", m.Ratio)
	}
	if m.Score <= 1.0 {
		t.Fatalf("score = %.2f, expected flood pressure", m.Score)
	}
}

func TestGeoJSONCentroid(t *testing.T) {
	g := map[string]any{
		"type": "Polygon",
		"coordinates": []any{
			[]any{
				[]any{0.0, 0.0},
				[]any{2.0, 0.0},
				[]any{2.0, 2.0},
				[]any{0.0, 2.0},
			},
		},
	}
	lat, lon, ok := geoJSONCentroid(g)
	if !ok {
		t.Fatal("centroid not OK")
	}
	if math.Abs(lat-1.0) > 0.01 || math.Abs(lon-1.0) > 0.01 {
		t.Fatalf("centroid = %.2f %.2f", lat, lon)
	}
}

func TestLANCEProductURL(t *testing.T) {
	p := lanceProduct{Collection: "61", Product: "MCDWD_L3_F1_NRT", Version: "061", FilePrefix: "MCDWD_L3_F1_NRT"}
	got := lanceProductURL(p, 2026, 119, "h30v07")
	for _, part := range []string{"allData/61/MCDWD_L3_F1_NRT/2026/119", "A2026119.h30v07.061.tif"} {
		if !strings.Contains(got, part) {
			t.Fatalf("url %q missing %q", got, part)
		}
	}
}
