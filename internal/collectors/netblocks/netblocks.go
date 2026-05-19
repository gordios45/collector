// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package netblocks ingests NetBlocks RSS reports as interpreted network
// disruption context. It complements raw IODA/BGP/OONI telemetry and is not a
// substitute for independent sensor corroboration.
package netblocks

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const feedURL = "https://netblocks.org/feed"

var htmlTagRE = regexp.MustCompile(`<[^>]+>`)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "netblocks_rss" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, feedURL, map[string]string{
		"Accept": "application/rss+xml,application/xml,text/xml,*/*",
	})
	if err != nil {
		return nil, err
	}
	items, err := parseRSS(buf)
	if err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(items))
	for _, it := range items {
		if ev, ok := eventFromItem(it); ok {
			out = append(out, ev)
		}
	}
	return dedupe(out), nil
}

type rssEnvelope struct {
	Items []rssItem `xml:"channel>item"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	GUID        string `xml:"guid"`
	Creator     string `xml:"creator"`
}

func parseRSS(buf []byte) ([]rssItem, error) {
	var env rssEnvelope
	if err := xml.Unmarshal(buf, &env); err != nil {
		return nil, err
	}
	return env.Items, nil
}

func eventFromItem(it rssItem) (events.Event, bool) {
	title := strings.TrimSpace(it.Title)
	desc := strip(it.Description)
	text := normalizeSpace(title + " " + desc)
	country, lat, lon, ok := matchCountry(text)
	if !ok {
		return events.Event{}, false
	}
	ts := parseTime(it.PubDate)
	outageType, score := outageClassification(text)
	props := map[string]any{
		"title":                       title,
		"description":                 desc,
		"link":                        strings.TrimSpace(it.Link),
		"country":                     country,
		"outage_type":                 outageType,
		"severity_score":              score,
		"interpreted_report":          true,
		"raw_sensor_source":           "NetBlocks interpreted report; corroborate with IODA/BGP/OONI where possible",
		"source_api_endpoint":         feedURL,
		"source_payload_validity":     validity(ts, 96*time.Hour, "netblocks_report_publication_time"),
		"deterministic_parser":        "netblocks_rss_rules_v1",
		"independence_group_hint":     "netblocks_interpreted_report",
		"same_source_self_confirming": false,
	}
	return events.Event{
		Ts:     ts,
		Source: "netblocks_rss",
		ExtID:  stableID(firstNonEmpty(it.GUID, it.Link, title)),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func outageClassification(text string) (string, float64) {
	t := strings.ToLower(text)
	score := 0.8
	typ := "network_disruption_report"
	switch {
	case containsAny(t, "internet shutdown", "network shutdown", "total shutdown"):
		typ = "internet_shutdown"
		score = 3.0
	case containsAny(t, "internet blackout", "telecoms blackout", "network blackout"):
		typ = "internet_blackout"
		score = 3.0
	case containsAny(t, "social media", "platform restriction", "blocked"):
		typ = "platform_restriction"
		score = 2.2
	case containsAny(t, "mobile internet", "mobile data"):
		typ = "mobile_data_disruption"
		score = 2.0
	case containsAny(t, "internet disrupted", "network disrupted", "connectivity disrupted", "outage"):
		typ = "internet_disruption"
		score = 2.4
	}
	if containsAny(t, "nationwide", "countrywide", "across the country") {
		score += 0.5
	}
	if containsAny(t, "election", "protest", "demonstration", "conflict", "military") {
		score += 0.35
	}
	return typ, clamp(score, 0, 3.5)
}

type countryPoint struct {
	Name     string
	Lat, Lon float64
	Aliases  []string
}

var countries = []countryPoint{
	{Name: "Afghanistan", Lat: 33, Lon: 67},
	{Name: "Algeria", Lat: 28, Lon: 3},
	{Name: "Bangladesh", Lat: 24, Lon: 90},
	{Name: "Belarus", Lat: 53, Lon: 28},
	{Name: "Brazil", Lat: -14, Lon: -52},
	{Name: "Burkina Faso", Lat: 12, Lon: -2},
	{Name: "Cameroon", Lat: 6, Lon: 12},
	{Name: "Chad", Lat: 15, Lon: 19},
	{Name: "China", Lat: 35, Lon: 104},
	{Name: "Colombia", Lat: 4, Lon: -72},
	{Name: "Cuba", Lat: 22, Lon: -80},
	{Name: "DRC", Lat: -3, Lon: 24, Aliases: []string{"democratic republic of congo", "congo kinshasa"}},
	{Name: "Egypt", Lat: 27, Lon: 30},
	{Name: "Ethiopia", Lat: 9, Lon: 40},
	{Name: "Gabon", Lat: -1, Lon: 11},
	{Name: "Gambia", Lat: 13, Lon: -16},
	{Name: "Guinea", Lat: 10, Lon: -10},
	{Name: "India", Lat: 21, Lon: 79},
	{Name: "Indonesia", Lat: -5, Lon: 120},
	{Name: "Iran", Lat: 32, Lon: 53},
	{Name: "Iraq", Lat: 33, Lon: 44},
	{Name: "Kazakhstan", Lat: 48, Lon: 67},
	{Name: "Kenya", Lat: 0, Lon: 38},
	{Name: "Libya", Lat: 27, Lon: 17},
	{Name: "Mali", Lat: 17, Lon: -4},
	{Name: "Myanmar", Lat: 22, Lon: 96},
	{Name: "Nepal", Lat: 28, Lon: 84},
	{Name: "Niger", Lat: 16, Lon: 8},
	{Name: "Nigeria", Lat: 10, Lon: 8},
	{Name: "Pakistan", Lat: 30, Lon: 69},
	{Name: "Russia", Lat: 60, Lon: 100},
	{Name: "Senegal", Lat: 14, Lon: -14},
	{Name: "Somalia", Lat: 6, Lon: 46},
	{Name: "Sri Lanka", Lat: 8, Lon: 81},
	{Name: "Sudan", Lat: 16, Lon: 30},
	{Name: "Syria", Lat: 35, Lon: 38},
	{Name: "Tanzania", Lat: -6, Lon: 35},
	{Name: "Turkey", Lat: 39, Lon: 35, Aliases: []string{"turkiye"}},
	{Name: "Uganda", Lat: 1, Lon: 32},
	{Name: "Ukraine", Lat: 49, Lon: 32},
	{Name: "Venezuela", Lat: 7, Lon: -67},
	{Name: "Yemen", Lat: 16, Lon: 48},
	{Name: "Zimbabwe", Lat: -19, Lon: 29},
}

func matchCountry(text string) (string, float64, float64, bool) {
	bestIdx := len(text) + 1
	var best countryPoint
	for _, c := range countries {
		names := append([]string{c.Name}, c.Aliases...)
		for _, name := range names {
			if idx := boundedIndex(text, name); idx >= 0 && idx < bestIdx {
				bestIdx = idx
				best = c
			}
		}
	}
	if bestIdx > len(text) {
		return "", 0, 0, false
	}
	return best.Name, best.Lat, best.Lon, true
}

func boundedIndex(text, term string) int {
	t := strings.ToLower(text)
	needle := strings.ToLower(strings.TrimSpace(term))
	if needle == "" {
		return -1
	}
	offset := 0
	for {
		idx := strings.Index(t[offset:], needle)
		if idx < 0 {
			return -1
		}
		start := offset + idx
		end := start + len(needle)
		if isBoundary(t, start-1) && isBoundary(t, end) {
			return start
		}
		offset = end
		if offset >= len(t) {
			return -1
		}
	}
}

func isBoundary(s string, idx int) bool {
	if idx < 0 || idx >= len(s) {
		return true
	}
	ch := s[idx]
	return !(ch >= 'a' && ch <= 'z') && !(ch >= '0' && ch <= '9')
}

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(htmlTagRE.ReplaceAllString(s, " "))
}

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func containsAny(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, term) {
			return true
		}
	}
	return false
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(strings.ToLower(s))))
	return "netblocks:" + hex.EncodeToString(h[:])
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.ExtID == "" || e.Source == "" || !e.HasPoint() {
			continue
		}
		k := e.Source + "|" + e.ExtID
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, e)
	}
	return out
}

func _compileGuard() {
	_ = fmt.Sprintf
}
