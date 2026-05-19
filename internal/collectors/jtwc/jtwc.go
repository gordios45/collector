// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// JTWC — Joint Typhoon Warning Center active tropical cyclones.
//
// Fills the hemispheric gap left by `nhc` (Atlantic + East/Central Pacific
// only): JTWC is the US warning authority for the NW Pacific, North Indian,
// South Indian, and South Pacific basins.
//
// Source — NRL Monterey's ATCF support page at science.nrlmry.navy.mil.
// JTWC's primary tree at metoc.navy.mil is fronted by CloudFront and
// blocks data products from hyperscaler IP ranges; NRL Monterey carries
// the same b-deck (best-track) files at a host that's reachable from
// commodity egress. NHC's public ATCF FTP no longer mirrors JTWC basins.
//
// The NRL host serves individual files at known paths but rejects bulk
// directory listings (WAF returns "Request Rejected" for autoindex
// access), so the discovery flow is:
//
//  1. GET /atcf/index1.html — the curated landing page that lists every
//     currently-tracked storm by ID (e.g. wp912026, sh272026). NRL
//     maintains it as the "what's active right now" view.
//  2. For each storm ID matching a JTWC basin, GET
//     /atcf/docs/current_storms/b<id>.dat — the B-deck file.
//  3. Parse each as ATCF best-track and emit one event per unique fix.
//
// ATCF B-deck format is comma-separated, fixed columns. We parse only
// BEST-track analysis lines (TECH=BEST, TAU=0). Multiple lines share the
// same warn time when the storm has 34/50/64-kt wind radii reported; we
// dedupe by warn time keeping the row with the highest VMAX (the three
// thresholds agree on the fix anyway). Pre-storm "INVEST" systems
// (cyclone numbers 80–99) are included — for an MI fusion stack they
// signal "something is brewing" before naming.
//
// Both URLs are overridable via JTWC_INDEX_URL and JTWC_BTK_BASE_URL for
// operators who maintain an internal mirror or want to point at the
// metoc.navy.mil tree from a NIPRNet-friendly egress.
//
// Refs:
//
//	ATCF spec — https://science.nrlmry.navy.mil/atcf/docs/database/new/abdeck.html
//	NRL index — https://science.nrlmry.navy.mil/atcf/index1.html
package jtwc

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	defaultIndexURL  = "https://science.nrlmry.navy.mil/atcf/index1.html"
	defaultBdeckBase = "https://science.nrlmry.navy.mil/atcf/docs/current_storms/"
)

// JTWC AOR basin codes — NW Pacific, North Indian, South Hemisphere,
// Central Pacific. We deliberately exclude AL/EP since those are NHC's
// territory and already collected by the `nhc` source.
var jtwcBasins = map[string]bool{"wp": true, "io": true, "sh": true, "cp": true}

// Only emit fixes from the last fixWindow; older rows in an active b-deck
// are historical and would just be re-upserted on every poll for nothing.
const fixWindow = 7 * 24 * time.Hour

type Collector struct {
	indexURL string
	bdeckURL string
}

func New() (*Collector, error) {
	idx := strings.TrimSpace(os.Getenv("JTWC_INDEX_URL"))
	if idx == "" {
		idx = defaultIndexURL
	}
	base := strings.TrimSpace(os.Getenv("JTWC_BTK_BASE_URL"))
	if base == "" {
		base = defaultBdeckBase
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return &Collector{indexURL: idx, bdeckURL: base}, nil
}

func (c *Collector) ID() string               { return "jtwc" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	body, err := httpx.GetBytes(ctx, c.indexURL, nil)
	if err != nil {
		return nil, fmt.Errorf("index: %w", err)
	}
	stormIDs := extractStormIDs(string(body))

	out := make([]events.Event, 0, 64)
	cutoff := time.Now().UTC().Add(-fixWindow)
	for _, id := range stormIDs {
		fn := "b" + id + ".dat"
		raw, err := httpx.GetBytes(ctx, c.bdeckURL+fn, nil)
		if err != nil {
			continue // best-effort per storm
		}
		out = append(out, parseATCF(raw, fn, cutoff)...)
	}
	return out, nil
}

// stormIDRe matches ATCF storm IDs as they appear in NRL's index1.html:
// `wp912026`, `sh272026`, `io012026`, `cp032025`. Two-letter basin code,
// two-digit cyclone number (01–49 named, 80–99 invest), four-digit year.
var stormIDRe = regexp.MustCompile(`(?i)\b(wp|io|sh|cp)(\d{2})(\d{4})\b`)

// extractStormIDs pulls unique JTWC-basin storm IDs from the curated
// landing page. The page lists each active system once with multiple
// product links (b-deck, gif, tcf, wrn) so we de-dupe.
func extractStormIDs(html string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range stormIDRe.FindAllStringSubmatch(html, -1) {
		basin := strings.ToLower(m[1])
		if !jtwcBasins[basin] {
			continue
		}
		id := basin + m[2] + m[3]
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

type atcfFix struct {
	ts       time.Time
	lat, lon float64
	vmax     int
	mslp     int
	sty      string
	name     string
	basin    string
	cy       string
	year     int
}

// parseATCF returns one event per unique BEST-track fix in the b-deck.
// Lines older than cutoff are skipped.
func parseATCF(buf []byte, filename string, cutoff time.Time) []events.Event {
	fixes := map[time.Time]atcfFix{}
	sc := bufio.NewScanner(strings.NewReader(string(buf)))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		cols := splitATCF(line)
		if len(cols) < 11 {
			continue
		}
		basin := strings.ToUpper(cols[0])
		cy := cols[1]
		ts, err := time.Parse("2006010215", cols[2])
		if err != nil {
			continue
		}
		ts = ts.UTC()
		if ts.Before(cutoff) {
			continue
		}
		if cols[4] != "BEST" {
			continue
		}
		if tau, _ := strconv.Atoi(cols[5]); tau != 0 {
			continue
		}
		lat, ok := parseATCFLat(cols[6])
		if !ok {
			continue
		}
		lon, ok := parseATCFLon(cols[7])
		if !ok {
			continue
		}
		vmax, _ := strconv.Atoi(cols[8])
		mslp, _ := strconv.Atoi(cols[9])
		sty := cols[10]
		name := ""
		// Storm name lives at column 28 (index 27) in the ATCF spec.
		if len(cols) > 27 {
			name = strings.Trim(cols[27], " *")
		}
		f := atcfFix{
			ts: ts, lat: lat, lon: lon,
			vmax: vmax, mslp: mslp, sty: sty,
			name: name, basin: basin, cy: cy, year: ts.Year(),
		}
		if cur, ok := fixes[ts]; !ok || vmax > cur.vmax {
			fixes[ts] = f
		}
	}

	out := make([]events.Event, 0, len(fixes))
	for _, f := range fixes {
		sid := fmt.Sprintf("%s%s%d", f.basin, f.cy, f.year)
		props := map[string]any{
			"id":         sid,
			"name":       f.name,
			"basin":      f.basin,
			"cy":         f.cy,
			"season":     f.year,
			"vmax_kt":    f.vmax,
			"mslp_mb":    f.mslp,
			"storm_type": f.sty,
			"warn_time":  f.ts.Format(time.RFC3339),
			"tech":       "BEST",
			"file":       filename,
		}
		out = append(out, events.Event{
			Ts:     f.ts,
			Source: "jtwc",
			ExtID:  sid + ":" + f.ts.UTC().Format("2006010215"),
			Lat:    f.lat,
			Lon:    f.lon,
			Props:  props,
		})
	}
	return out
}

// splitATCF splits the comma-delimited line and trims whitespace per field.
func splitATCF(line string) []string {
	parts := strings.Split(line, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// parseATCFLat parses "153N" → 15.3, "100S" → -10.0. Resolution 0.1°.
func parseATCFLat(s string) (float64, bool) {
	if len(s) < 2 {
		return 0, false
	}
	hem := s[len(s)-1]
	n, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, false
	}
	v := float64(n) / 10.0
	switch hem {
	case 'N', 'n':
		return v, true
	case 'S', 's':
		return -v, true
	}
	return 0, false
}

func parseATCFLon(s string) (float64, bool) {
	if len(s) < 2 {
		return 0, false
	}
	hem := s[len(s)-1]
	n, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, false
	}
	v := float64(n) / 10.0
	switch hem {
	case 'E', 'e':
		return v, true
	case 'W', 'w':
		return -v, true
	}
	return 0, false
}
