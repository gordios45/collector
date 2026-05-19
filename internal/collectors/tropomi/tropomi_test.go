// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package tropomi

import (
	"strings"
	"testing"
	"time"

	"github.com/uber/h3-go/v4"
)

func TestSummarizeMetricStrongChange(t *testing.T) {
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	intervals := make([]intervalStats, 0, 8)
	for i := 0; i < 7; i++ {
		intervals = append(intervals, intervalStats{
			From:               start.AddDate(0, 0, i),
			To:                 start.AddDate(0, 0, i+1),
			Mean:               10e-6 + float64(i%2)*0.2e-6,
			SampleCount:        20,
			GeometryPixelCount: 20,
		})
	}
	current := intervalStats{
		From:               start.AddDate(0, 0, 7),
		To:                 start.AddDate(0, 0, 8),
		Mean:               25e-6,
		SampleCount:        20,
		GeometryPixelCount: 20,
	}
	intervals = append(intervals, current)

	m := summarizeMetric(gasConfigs[GasNO2], intervals, current.From, metricOptions{
		MinBaselineDays:     6,
		MinValidCount:       3,
		MinValidPct:         5,
		CurrentLookbackDays: 2,
		BaselineDays:        30,
	})
	if m.State != "strong_change" {
		t.Fatalf("state = %q, want strong_change (z %.2f)", m.State, m.AnomalyZ)
	}
	if m.AnomalyScore != 3 {
		t.Fatalf("anomaly score = %.2f, want clamped 3", m.AnomalyScore)
	}
	if m.QualityBlocked {
		t.Fatal("quality blocked = true, want false")
	}
}

func TestSummarizeMetricBaselinePending(t *testing.T) {
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	intervals := []intervalStats{
		{From: start, To: start.AddDate(0, 0, 1), Mean: 1, SampleCount: 10, GeometryPixelCount: 10},
		{From: start.AddDate(0, 0, 1), To: start.AddDate(0, 0, 2), Mean: 2, SampleCount: 10, GeometryPixelCount: 10},
	}
	m := summarizeMetric(gasConfigs[GasSO2], intervals, start.AddDate(0, 0, 1), metricOptions{
		MinBaselineDays:     3,
		MinValidCount:       3,
		MinValidPct:         5,
		CurrentLookbackDays: 2,
		BaselineDays:        30,
	})
	if m.State != "baseline_pending" {
		t.Fatalf("state = %q, want baseline_pending", m.State)
	}
	if !m.QualityBlocked {
		t.Fatal("quality blocked = false, want true")
	}
}

func TestCellGeometryProducesClosedPolygonWKT(t *testing.T) {
	cell, err := h3.LatLngToCell(h3.LatLng{Lat: 41.9, Lng: 12.5}, defaultH3Res)
	if err != nil {
		t.Fatal(err)
	}
	geom, wkt, err := cellGeometry(cell)
	if err != nil {
		t.Fatal(err)
	}
	if geom["type"] != "Polygon" {
		t.Fatalf("geojson type = %v, want Polygon", geom["type"])
	}
	if !strings.HasPrefix(wkt, "POLYGON((") || !strings.HasSuffix(wkt, "))") {
		t.Fatalf("unexpected WKT %q", wkt)
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(wkt, "POLYGON(("), "))"), ",")
	if len(parts) < 4 {
		t.Fatalf("too few WKT points: %v", parts)
	}
	if parts[0] != parts[len(parts)-1] {
		t.Fatalf("polygon is not closed: first %q last %q", parts[0], parts[len(parts)-1])
	}
}

func TestPointBoxGeometryPreservesAOICenter(t *testing.T) {
	geom, wkt, err := pointBoxGeometry(32.38627, 53.74147, 0.05)
	if err != nil {
		t.Fatal(err)
	}
	if geom["type"] != "Polygon" {
		t.Fatalf("geojson type = %v, want Polygon", geom["type"])
	}
	if !strings.Contains(wkt, "53.691470 32.336270") || !strings.Contains(wkt, "53.791470 32.436270") {
		t.Fatalf("unexpected point box WKT %q", wkt)
	}
}
