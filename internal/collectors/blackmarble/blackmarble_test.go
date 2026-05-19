// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package blackmarble

import (
	"image"
	"image/color"
	"testing"
	"time"
)

func TestSolarDarknessGuard(t *testing.T) {
	march := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC)
	equatorMidnight := localSolarMidnight(march, 0)
	if elev := solarElevationDeg(0, 0, equatorMidnight); elev > -80 {
		t.Fatalf("equator midnight solar elevation = %.2f, want dark night", elev)
	}

	june := time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC)
	arcticMidnight := localSolarMidnight(june, 20)
	if elev := solarElevationDeg(78, 20, arcticMidnight); elev <= -6 {
		t.Fatalf("arctic summer midnight solar elevation = %.2f, want night unavailable", elev)
	}
}

func TestSampleImageBrightnessAndNightExpected(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			img.Set(x, y, color.RGBA{R: 100, G: 100, B: 100, A: 255})
		}
	}
	s := sampleImage(img, time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC), 4, 4, 2, 0, 0)
	if !s.OK {
		t.Fatal("sample OK = false, want true")
	}
	if !s.NightExpected {
		t.Fatal("night expected = false, want true at equator midnight")
	}
	if s.Mean < 99 || s.Mean > 101 {
		t.Fatalf("mean brightness = %.2f, want about 100", s.Mean)
	}
}
