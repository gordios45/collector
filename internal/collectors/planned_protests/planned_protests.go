// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package planned_protests ingests no-key public protest calendars.
package planned_protests

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "planned_protests"

const (
	dcActionICS = "https://calendar.google.com/calendar/ical/dfk2eele63bcudvsqg8hdoponc%40group.calendar.google.com/public/basic.ics"
	phillyURL   = "https://phillyprotest.com/"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchDCAction(ctx)...)
	out = append(out, c.fetchPhillyProtest(ctx)...)
	return dedupe(out), nil
}

func (c *Collector) fetchDCAction(ctx context.Context) []events.Event {
	body, err := httpx.GetBytes(ctx, dcActionICS, map[string]string{"Accept": "text/calendar,*/*"})
	if err != nil {
		return nil
	}
	items := parseICS(body)
	out := make([]events.Event, 0, len(items))
	for _, ve := range items {
		start := parseICSTime(firstNonEmpty(ve["DTSTART"], ve["DTSTART;VALUE=DATE"]))
		if !eventWindow(start) {
			continue
		}
		end := parseICSTime(firstNonEmpty(ve["DTEND"], ve["DTEND;VALUE=DATE"]))
		title := firstNonEmpty(ve["SUMMARY"], "Planned demonstration")
		desc := firstNonEmpty(ve["DESCRIPTION"], title)
		loc := firstNonEmpty(ve["LOCATION"], "Washington, DC")
		lat, lon := 38.9072, -77.0369
		props := plannedProps("DC Action", dcActionICS, "https://calendar.google.com/", title, desc, loc, start, end)
		id := firstNonEmpty(ve["UID"], title+start.Format(time.RFC3339))
		out = append(out, event(start, "dc_action:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchPhillyProtest(ctx context.Context) []events.Event {
	body, err := httpx.GetBytes(ctx, phillyURL, map[string]string{"Accept": "text/html,*/*"})
	if err != nil {
		return nil
	}
	maps := extractJSONEvents(body)
	out := make([]events.Event, 0, len(maps))
	for _, m := range maps {
		start := parseTimeString(textAt(m, "startDate"))
		if !eventWindow(start) {
			continue
		}
		end := parseTimeString(textAt(m, "endDate"))
		title := firstNonEmpty(textAt(m, "name"), "Planned protest")
		desc := strip(textAt(m, "description"))
		location := locationText(m)
		lat, lon, ok := centroidForLocation(location)
		if !ok {
			continue
		}
		props := plannedProps("Philly Protest", phillyURL, textAt(m, "url"), title, desc, location, start, end)
		id := firstNonEmpty(textAt(m, "@id"), textAt(m, "url"), title+start.Format(time.RFC3339))
		out = append(out, event(start, "philly_protest:"+id, lat, lon, props))
	}
	return out
}

func plannedProps(provider, endpoint, publicURL, title, desc, location string, start, end time.Time) map[string]any {
	if publicURL == "" {
		publicURL = endpoint
	}
	score := plannedProtestScore(start, desc+" "+title)
	return map[string]any{
		"source_provider":         provider,
		"source_api_endpoint":     endpoint,
		"source_public_url":       publicURL,
		"source_provider_kind":    "public_planned_demonstration_calendar",
		"title":                   "Planned protest: " + strings.TrimSpace(title),
		"description":             strings.TrimSpace(desc),
		"location":                strings.TrimSpace(location),
		"start_time":              timeString(start),
		"end_time":                timeString(end),
		"planned_protest_score":   score,
		"labels":                  []string{"planned_demonstration", "protest"},
		"source_payload_validity": plannedValidity(start, end),
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func plannedProtestScore(start time.Time, text string) float64 {
	score := 0.8
	if !start.IsZero() {
		until := time.Until(start)
		if until >= 0 && until <= 72*time.Hour {
			score += 0.7
		}
		if until < 0 && until >= -6*time.Hour {
			score += 0.6
		}
	}
	if containsAny(strings.ToLower(text), "march", "rally", "demonstration", "walkout", "strike") {
		score += 0.3
	}
	return propx.ClampFloat(score, 0, 2.5)
}

func parseICS(body []byte) []map[string]string {
	body = bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n"))
	rawLines := strings.Split(string(body), "\n")
	lines := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		line = strings.TrimRight(line, "\r")
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') && len(lines) > 0 {
			lines[len(lines)-1] += strings.TrimSpace(line)
			continue
		}
		lines = append(lines, line)
	}
	var out []map[string]string
	var cur map[string]string
	for _, line := range lines {
		switch strings.TrimSpace(line) {
		case "BEGIN:VEVENT":
			cur = map[string]string{}
			continue
		case "END:VEVENT":
			if len(cur) > 0 {
				out = append(out, cur)
			}
			cur = nil
			continue
		}
		if cur == nil {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = icsUnescape(value)
		cur[key] = value
		if base, _, ok := strings.Cut(key, ";"); ok {
			cur[base] = value
		}
	}
	return out
}

func icsUnescape(s string) string {
	s = strings.ReplaceAll(s, `\n`, "\n")
	s = strings.ReplaceAll(s, `\N`, "\n")
	s = strings.ReplaceAll(s, `\,`, ",")
	s = strings.ReplaceAll(s, `\;`, ";")
	s = strings.ReplaceAll(s, `\\`, `\`)
	return strings.TrimSpace(s)
}

func extractJSONEvents(body []byte) []map[string]any {
	var out []map[string]any
	for _, m := range scriptRE.FindAllSubmatch(body, -1) {
		raw := bytes.TrimSpace([]byte(html.UnescapeString(string(m[1]))))
		if !bytes.Contains(raw, []byte("startDate")) || !bytes.Contains(raw, []byte("Event")) {
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			continue
		}
		out = append(out, findEventMaps(v)...)
	}
	return out
}

var scriptRE = regexp.MustCompile(`(?is)<script[^>]*>(.*?)</script>`)

func findEventMaps(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		var out []map[string]any
		for _, item := range x {
			out = append(out, findEventMaps(item)...)
		}
		return out
	case map[string]any:
		if isEventMap(x) {
			return []map[string]any{x}
		}
		var out []map[string]any
		for _, value := range x {
			out = append(out, findEventMaps(value)...)
		}
		return out
	default:
		return nil
	}
}

func isEventMap(m map[string]any) bool {
	typ := strings.ToLower(fmt.Sprint(m["@type"]))
	return strings.Contains(typ, "event") && strings.TrimSpace(textAt(m, "startDate")) != ""
}

func locationText(m map[string]any) string {
	loc, _ := m["location"].(map[string]any)
	addr, _ := loc["address"].(map[string]any)
	parts := []string{
		textAt(loc, "name"),
		textAt(addr, "streetAddress"),
		textAt(addr, "addressLocality"),
		textAt(addr, "addressRegion"),
		textAt(addr, "postalCode"),
		textAt(addr, "addressCountry"),
	}
	return strings.Join(nonEmpty(parts...), ", ")
}

func centroidForLocation(location string) (float64, float64, bool) {
	l := strings.ToLower(location)
	switch {
	case strings.Contains(l, "washington") || strings.Contains(l, "district of columbia") || strings.Contains(l, " dc"):
		return 38.9072, -77.0369, true
	case strings.Contains(l, "philadelphia") || strings.Contains(l, " phila") || strings.Contains(l, " pa"):
		return 39.9526, -75.1652, true
	default:
		return 0, 0, false
	}
}

func parseICSTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		"20060102T150405Z", "20060102T150405", "20060102",
		time.RFC3339Nano, time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func parseTimeString(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05-07:00", "2006-01-02T15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func eventWindow(start time.Time) bool {
	if start.IsZero() {
		return false
	}
	now := time.Now().UTC()
	return start.After(now.Add(-30*24*time.Hour)) && start.Before(now.Add(45*24*time.Hour))
}

func plannedValidity(start, end time.Time) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(6 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Add(-24 * time.Hour).Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": "public_calendar_event_window",
	}
}

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(tagRE.ReplaceAllString(s, " "))
}

var tagRE = regexp.MustCompile(`<[^>]+>`)

func textAt(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return strings.TrimSpace(s)
	}
	if v, ok := m[key]; ok && v != nil {
		return strings.TrimSpace(fmt.Sprint(v))
	}
	return ""
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func nonEmpty(vals ...string) []string {
	out := []string{}
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			out = append(out, strings.TrimSpace(v))
		}
	}
	return out
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.Source == "" || e.ExtID == "" || !e.HasPoint() {
			continue
		}
		key := e.Source + ":" + e.ExtID
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, e)
	}
	return out
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(h[:])
}
