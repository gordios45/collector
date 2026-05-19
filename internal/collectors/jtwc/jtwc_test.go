// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package jtwc

import (
	"testing"
	"time"
)

// Sample taken from the ATCF B-deck spec — three lines for the same warn
// time (one per wind-radii threshold). The parser must collapse them into a
// single fix and parse coordinates/intensity correctly.
const sampleBdeck = `WP, 25, 2025102818,   , BEST,   0, 153N, 1311E, 100,  970, TY,  34, NEQ,  120,  100,   80,  100, 1003,  250,  30,  120,    0,   ,  0,    ,    ,    , ROLAND      , S,
WP, 25, 2025102818,   , BEST,   0, 153N, 1311E, 100,  970, TY,  50, NEQ,   80,   60,   50,   60, 1003,  250,  30,  120,    0,   ,  0,    ,    ,    , ROLAND      , S,
WP, 25, 2025102818,   , BEST,   0, 153N, 1311E, 100,  970, TY,  64, NEQ,   60,   40,   30,   40, 1003,  250,  30,  120,    0,   ,  0,    ,    ,    , ROLAND      , S,
WP, 25, 2025102912,   , BEST,   0, 162S, 1450W,  85,  985, TY,  34, NEQ,  100,  100,   80,  100, 1004,  250,  30,  100,    0,   ,  0,    ,    ,    , ROLAND      , S,
`

func TestParseATCF_DedupesByWarnTimeAndParsesHemispheres(t *testing.T) {
	cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	evs := parseATCF([]byte(sampleBdeck), "bwp252025.dat", cutoff)
	if len(evs) != 2 {
		t.Fatalf("expected 2 events (one per warn time), got %d", len(evs))
	}

	// Event with the larger warn time is the second sample (2025-10-29 12Z),
	// southern-hemisphere lat and western-hemisphere lon.
	var early, late = evs[0], evs[1]
	if early.Ts.After(late.Ts) {
		early, late = late, early
	}

	if want := time.Date(2025, 10, 28, 18, 0, 0, 0, time.UTC); !early.Ts.Equal(want) {
		t.Errorf("early ts: got %v want %v", early.Ts, want)
	}
	if early.Lat < 15.29 || early.Lat > 15.31 {
		t.Errorf("early lat: got %v want 15.3", early.Lat)
	}
	if early.Lon < 131.09 || early.Lon > 131.11 {
		t.Errorf("early lon: got %v want 131.1", early.Lon)
	}
	if got := early.Props["vmax_kt"]; got != 100 {
		t.Errorf("early vmax: got %v want 100", got)
	}
	if got := early.Props["mslp_mb"]; got != 970 {
		t.Errorf("early mslp: got %v want 970", got)
	}
	if got := early.Props["name"]; got != "ROLAND" {
		t.Errorf("early name: got %q want ROLAND", got)
	}
	if got := early.Source; got != "jtwc" {
		t.Errorf("source: got %q want jtwc", got)
	}

	// Southern + western hemisphere parsing.
	if late.Lat > -16.1 || late.Lat < -16.3 {
		t.Errorf("late lat: got %v want -16.2", late.Lat)
	}
	if late.Lon > -144.9 || late.Lon < -145.1 {
		t.Errorf("late lon: got %v want -145.0", late.Lon)
	}
}

func TestExtractStormIDs_NRLIndexPage(t *testing.T) {
	// Excerpt from the live science.nrlmry.navy.mil/atcf/index1.html showing
	// one active invest. The same storm ID appears in multiple href
	// attributes (gif, tcf, etc.); we must dedupe.
	html := `<a href="images/wp912026.gif"><img src="images/wp912026.thm.gif"></a>
		<a href="docs/current_storms/wp912026.tcf">tcfa message</a>
		<a href="docs/current_storms/sh272026.wrn">warning</a>
		<a href="images/al052025.gif">old</a>
		<a href="docs/current_storms/io012026.fst">forecast</a>`
	ids := extractStormIDs(html)
	want := map[string]bool{"wp912026": true, "sh272026": true, "io012026": true}
	if len(ids) != len(want) {
		t.Fatalf("got %v want %v", ids, want)
	}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("unexpected id %q (Atlantic should be filtered out)", id)
		}
	}
}

func TestParseATCF_ParsesLiveNRLBdeck(t *testing.T) {
	// Real content of bwp912026.dat pulled from
	// https://science.nrlmry.navy.mil/atcf/docs/current_storms/bwp912026.dat
	// at 2026-04-27T12:08Z. Three BEST-track fixes for the wp912026 invest
	// system (TC formation alert candidate; storm name "INVEST", type "DB"
	// = disturbance).
	const live = `WP, 91, 2026042618,   , BEST,   0,  35N, 1612E,  15, 1009, DB,   0,    ,    0,    0,    0,    0,    0,    0,   0,   0,   0,   W,   0,    ,   0,   0,     INVEST,  ,
WP, 91, 2026042700,   , BEST,   0,  36N, 1610E,  15, 1009, DB,   0,    ,    0,    0,    0,    0,    0,    0,   0,   0,   0,   W,   0,    ,   0,   0,     INVEST,  ,
WP, 91, 2026042706,   , BEST,   0,  37N, 1609E,  15, 1005, DB,   0,    ,    0,    0,    0,    0,    0,    0,   0,   0,   0,   W,   0,    ,   0,   0,     INVEST,  ,
`
	cutoff := time.Date(2026, 4, 25, 0, 0, 0, 0, time.UTC) // before all fixes
	evs := parseATCF([]byte(live), "bwp912026.dat", cutoff)
	if len(evs) != 3 {
		t.Fatalf("expected 3 events, got %d", len(evs))
	}
	for _, e := range evs {
		if e.Source != "jtwc" {
			t.Errorf("source: got %q", e.Source)
		}
		if e.Props["basin"] != "WP" {
			t.Errorf("basin: got %v", e.Props["basin"])
		}
		if e.Props["name"] != "INVEST" {
			t.Errorf("name: got %v want INVEST", e.Props["name"])
		}
		if e.Props["storm_type"] != "DB" {
			t.Errorf("storm_type: got %v want DB", e.Props["storm_type"])
		}
		if e.Lat < 3.4 || e.Lat > 3.8 {
			t.Errorf("lat out of expected band: %v", e.Lat)
		}
		if e.Lon < 160.8 || e.Lon > 161.3 {
			t.Errorf("lon out of expected band: %v", e.Lon)
		}
	}
}

func TestParseATCFLatLon_HemisphereSign(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
		ok   bool
		fn   func(string) (float64, bool)
	}{
		{"153N", 15.3, true, parseATCFLat},
		{"100S", -10.0, true, parseATCFLat},
		{"00N", 0.0, true, parseATCFLat},
		{"1311E", 131.1, true, parseATCFLon},
		{"1450W", -145.0, true, parseATCFLon},
		{"abc", 0, false, parseATCFLat},
	}
	for _, c := range cases {
		got, ok := c.fn(c.raw)
		if ok != c.ok {
			t.Errorf("%s: ok=%v want %v", c.raw, ok, c.ok)
		}
		if ok && (got < c.want-0.001 || got > c.want+0.001) {
			t.Errorf("%s: got %v want %v", c.raw, got, c.want)
		}
	}
}
