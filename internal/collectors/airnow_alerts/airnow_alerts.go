// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package airnow_alerts ingests the public AIRNow/EnviroFlash CAP aggregate.
package airnow_alerts

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"html"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID = "airnow_alerts"
	endpoint = "http://feeds.enviroflash.info/cap/aggregate.xml"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, endpoint, map[string]string{"Accept": "application/atom+xml,application/rss+xml,application/xml,text/xml,*/*"})
	if err != nil {
		return nil, err
	}
	var env feedEnvelope
	if err := xml.Unmarshal(buf, &env); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(env.Items)+len(env.Entries))
	for _, it := range append(env.Items, env.Entries...) {
		if e, ok := eventFromItem(it); ok {
			out = append(out, e)
		}
	}
	return dedupe(out), nil
}

type feedEnvelope struct {
	Items   []feedItem `xml:"channel>item"`
	Entries []feedItem `xml:"entry"`
}

type feedItem struct {
	ID          string   `xml:"id"`
	GUID        string   `xml:"guid"`
	Title       string   `xml:"title"`
	Link        feedLink `xml:"link"`
	Updated     string   `xml:"updated"`
	Published   string   `xml:"published"`
	PubDate     string   `xml:"pubDate"`
	Summary     string   `xml:"summary"`
	Content     string   `xml:"content"`
	Headline    string   `xml:"headline"`
	Description string   `xml:"description"`
	Event       string   `xml:"event"`
	Instruction string   `xml:"instruction"`
	Severity    string   `xml:"severity"`
	Urgency     string   `xml:"urgency"`
	Certainty   string   `xml:"certainty"`
	Effective   string   `xml:"effective"`
	Expires     string   `xml:"expires"`
	AreaDesc    string   `xml:"areaDesc"`
	Circle      string   `xml:"circle"`
	Polygon     string   `xml:"polygon"`
	Info        struct {
		Headline    string `xml:"headline"`
		Description string `xml:"description"`
		Event       string `xml:"event"`
		Instruction string `xml:"instruction"`
		Severity    string `xml:"severity"`
		Urgency     string `xml:"urgency"`
		Certainty   string `xml:"certainty"`
		Effective   string `xml:"effective"`
		Expires     string `xml:"expires"`
		Area        struct {
			AreaDesc string `xml:"areaDesc"`
			Circle   string `xml:"circle"`
			Polygon  string `xml:"polygon"`
		} `xml:"area"`
	} `xml:"info"`
}

type feedLink struct {
	Href string `xml:"href,attr"`
	Text string `xml:",chardata"`
}

func eventFromItem(it feedItem) (events.Event, bool) {
	circle := firstNonEmpty(it.Circle, it.Info.Area.Circle)
	polygon := firstNonEmpty(it.Polygon, it.Info.Area.Polygon)
	lat, lon, geom, ok := geometry(circle, polygon)
	if !ok {
		return events.Event{}, false
	}
	title := firstNonEmpty(it.Headline, it.Info.Headline, it.Title, it.Event, it.Info.Event, "Air quality alert")
	desc := strip(firstNonEmpty(it.Description, it.Info.Description, it.Event, it.Info.Event, it.Summary, it.Content))
	instruction := strip(firstNonEmpty(it.Instruction, it.Info.Instruction))
	severity := firstNonEmpty(it.Severity, it.Info.Severity)
	effective := parseTimeString(firstNonEmpty(it.Effective, it.Info.Effective, it.Published, it.Updated, it.PubDate))
	expires := parseTimeString(firstNonEmpty(it.Expires, it.Info.Expires))
	if effective.IsZero() {
		effective = time.Now().UTC()
	}
	if expires.IsZero() || expires.Before(effective) {
		expires = effective.Add(24 * time.Hour)
	}
	aqi := maxAQI(desc + " " + title)
	score := airQualityScore(aqi, severity, desc+" "+title)
	linkText := strings.TrimSpace(it.Link.Text)
	linkHref := strings.TrimSpace(it.Link.Href)
	id := firstNonEmpty(queryID(it.ID), queryID(linkText), queryID(linkHref), it.GUID, it.ID, linkText, stableID(title+fmt.Sprint(lat, lon)))
	props := map[string]any{
		"source_provider":           "AIRNow Program, US Environmental Protection Agency",
		"source_api_endpoint":       endpoint,
		"source_public_url":         firstNonEmpty(linkHref, linkText, "https://www.airnow.gov/"),
		"source_provider_kind":      "official_air_quality_alert",
		"title":                     strings.TrimSpace(title),
		"description":               strings.TrimSpace(desc),
		"instruction":               strings.TrimSpace(instruction),
		"event":                     firstNonEmpty(it.Event, it.Info.Event),
		"severity":                  severity,
		"urgency":                   firstNonEmpty(it.Urgency, it.Info.Urgency),
		"certainty":                 firstNonEmpty(it.Certainty, it.Info.Certainty),
		"area_desc":                 firstNonEmpty(it.AreaDesc, it.Info.Area.AreaDesc),
		"aqi":                       aqi,
		"air_quality_score":         score,
		"labels":                    alertLabels(title + " " + desc),
		"source_payload_validity":   validity(effective, expires),
		"source_payload_expires_at": expires.Format(time.RFC3339),
	}
	return events.Event{Ts: effective, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Geom: geom, Props: props}, true
}

func geometry(circle, polygon string) (float64, float64, string, bool) {
	if lat, lon, ok := parseCircle(circle); ok {
		return lat, lon, fmt.Sprintf("POINT(%f %f)", lon, lat), true
	}
	if lat, lon, wkt, ok := parsePolygon(polygon); ok {
		return lat, lon, wkt, true
	}
	return 0, 0, "", false
}

func parseCircle(raw string) (float64, float64, bool) {
	first := strings.Fields(strings.TrimSpace(raw))
	if len(first) == 0 {
		return 0, 0, false
	}
	parts := strings.Split(strings.ReplaceAll(first[0], " ", ""), ",")
	if len(parts) != 2 {
		parts = strings.Fields(strings.ReplaceAll(first[0], ",", " "))
	}
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	return lat, lon, err1 == nil && err2 == nil && validLatLon(lat, lon)
}

func parsePolygon(raw string) (float64, float64, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, "", false
	}
	var pts []string
	var sumLat, sumLon float64
	for _, pair := range strings.Fields(raw) {
		xy := strings.Split(strings.TrimSpace(pair), ",")
		if len(xy) != 2 {
			continue
		}
		lat, err1 := strconv.ParseFloat(xy[0], 64)
		lon, err2 := strconv.ParseFloat(xy[1], 64)
		if err1 != nil || err2 != nil || !validLatLon(lat, lon) {
			continue
		}
		sumLat += lat
		sumLon += lon
		pts = append(pts, fmt.Sprintf("%f %f", lon, lat))
	}
	if len(pts) < 3 {
		return 0, 0, "", false
	}
	if pts[0] != pts[len(pts)-1] {
		pts = append(pts, pts[0])
	}
	return sumLat / float64(len(pts)-1), sumLon / float64(len(pts)-1), "POLYGON((" + strings.Join(pts, ",") + "))", true
}

func alertLabels(text string) []string {
	labels := []string{"air_quality_alert", "air_pollution"}
	l := strings.ToLower(text)
	if containsAny(l, "smoke", "wildfire", "particle", "pm2.5", "pm 2.5", "particulate") {
		labels = append(labels, "smoke_or_air_release")
	}
	if strings.Contains(l, "ozone") {
		labels = append(labels, "ozone")
	}
	return unique(labels)
}

func airQualityScore(aqi int, severity, text string) float64 {
	score := 0.6
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "extreme":
		score = 3.3
	case "severe":
		score = 2.5
	case "moderate":
		score = 1.5
	case "minor":
		score = 0.8
	}
	switch {
	case aqi >= 301:
		score = math.Max(score, 3.5)
	case aqi >= 201:
		score = math.Max(score, 2.8)
	case aqi >= 151:
		score = math.Max(score, 2.0)
	case aqi >= 101:
		score = math.Max(score, 1.2)
	}
	if containsAny(strings.ToLower(text), "unhealthy", "hazardous", "very unhealthy") {
		score += 0.2
	}
	return propx.ClampFloat(score, 0, 4)
}

var aqiRE = regexp.MustCompile(`(?i)(?:AQI[^0-9]{0,20}([0-9]{2,3})|([0-9]{2,3})\s*AQI)`)

func maxAQI(text string) int {
	best := 0
	for _, m := range aqiRE.FindAllStringSubmatch(text, -1) {
		for _, part := range m[1:] {
			if part == "" {
				continue
			}
			n, err := strconv.Atoi(part)
			if err == nil && n > best {
				best = n
			}
		}
	}
	return best
}

func queryID(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return u.Query().Get("id")
}

func parseTimeString(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"2006-01-02T15:04:05-07:00", "2006-01-02T15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": "airnow_cap_effective_expires",
	}
}

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(tagRE.ReplaceAllString(s, " "))
}

var tagRE = regexp.MustCompile(`<[^>]+>`)

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func unique(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
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
