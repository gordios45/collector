// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package adsb_lol

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"
)

func TestAircraftEventMapsPublicADSBFields(t *testing.T) {
	lat, lon := 38.9, -77.0
	gs := 250.0
	track := 91.0
	seenPos := 5.0
	rawAlt := json.RawMessage(`10000`)
	rawRate := json.RawMessage(`1200`)
	ac := aircraft{
		Hex:       "ABC123",
		Flight:    "TEST1 ",
		Reg:       "N123AB",
		Aircraft:  "C17",
		AltBaro:   rawAlt,
		Lat:       &lat,
		Lon:       &lon,
		GS:        &gs,
		Track:     &track,
		BaroRate:  rawRate,
		Squawk:    "7700",
		Emergency: "none",
		SeenPos:   &seenPos,
		Messages:  9,
	}
	ev, ok := aircraftEvent(ac, "mil", time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("aircraft skipped")
	}
	if ev.Source != "adsb_lol" || ev.ExtID != "mil:abc123" {
		t.Fatalf("identity=%s/%s", ev.Source, ev.ExtID)
	}
	if ev.Ts != time.Date(2026, 5, 2, 11, 59, 55, 0, time.UTC) {
		t.Fatalf("ts=%s", ev.Ts)
	}
	if got := ev.Props["military"]; got != true {
		t.Fatalf("military=%v", got)
	}
	if got := ev.Props["callsign"]; got != "TEST1" {
		t.Fatalf("callsign=%v", got)
	}
	if got := ev.Props["military_activity_score"]; got != float64(1) {
		t.Fatalf("military_activity_score=%v", got)
	}
	if got := ev.Props["emergency_score"]; got != float64(2) {
		t.Fatalf("emergency_score=%v", got)
	}
	if _, ok := ev.Props["source_payload_validity"].(map[string]any); !ok {
		t.Fatalf("missing validity range")
	}
}

func TestAircraftEventAddsLowAltitudeScore(t *testing.T) {
	lat, lon := 38.9, -77.0
	gs := 120.0
	rawAlt := json.RawMessage(`2000`)
	ac := aircraft{
		Hex:     "ABC124",
		AltBaro: rawAlt,
		Lat:     &lat,
		Lon:     &lon,
		GS:      &gs,
	}
	ev, ok := aircraftEvent(ac, "aoi", time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("aircraft skipped")
	}
	got, _ := ev.Props["low_altitude_score"].(float64)
	if math.Abs(got-1.1808) > 0.0001 {
		t.Fatalf("low_altitude_score=%v", got)
	}
}

func TestAlternateADSBURLAndProviderName(t *testing.T) {
	alt := alternateADSBURL("https://api.adsb.lol/v2/mil")
	if alt != "https://api.airplanes.live/v2/mil" {
		t.Fatalf("alternate=%s", alt)
	}
	if got := providerName(alt); got != "airplanes_live" {
		t.Fatalf("provider=%s", got)
	}
	if got := alternateADSBURL("https://example.com/v2/mil"); got != "" {
		t.Fatalf("unexpected alternate=%s", got)
	}
}

func TestLiveFetchConfiguredAOI(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_NETWORK_TESTS") != "1" {
		t.Skip("set GORDIOS_LIVE_NETWORK_TESTS=1 to run against ADSB.lol")
	}
	t.Setenv("ADSB_LOL_ENDPOINTS", "dc=lat/38.9/lon/-77.0/dist/10")
	c, err := New()
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	evs, err := c.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(evs) == 0 {
		t.Fatalf("expected aircraft around configured AOI")
	}
	t.Logf("adsb_lol events=%d", len(evs))
}
