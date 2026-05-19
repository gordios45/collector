// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package road_traffic_flow ingests road flow/congestion signals from public
// Open511 feeds and configured DATEX II measured-data feeds.
package road_traffic_flow

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const sourceID = "road_traffic_flow"

type Collector struct {
	open511 []feedSpec
	datex2  []feedSpec
}

type feedSpec struct {
	Label string
	URL   string
	Lat   float64
	Lon   float64
}

func New() (*Collector, error) {
	open511 := parseFeeds(os.Getenv("OPEN511_FEEDS"))
	if len(open511) == 0 {
		open511 = []feedSpec{{
			Label: "DriveBC Open511",
			URL:   "https://api.open511.gov.bc.ca/events?format=json&limit=200",
			Lat:   53.7,
			Lon:   -125.0,
		}}
	}
	return &Collector{
		open511: open511,
		datex2:  parseFeeds(os.Getenv("DATEX2_FEEDS")),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 2 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, f := range c.open511 {
		evs, err := fetchOpen511(ctx, f)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	for _, f := range c.datex2 {
		evs, err := fetchDATEX2(ctx, f)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type open511Response struct {
	Events []open511Event `json:"events"`
}

type open511Event struct {
	ID          string           `json:"id"`
	URL         string           `json:"url"`
	Headline    string           `json:"headline"`
	Status      string           `json:"status"`
	Created     string           `json:"created"`
	Updated     string           `json:"updated"`
	Description string           `json:"description"`
	EventType   string           `json:"event_type"`
	Severity    string           `json:"severity"`
	Geography   open511Geometry  `json:"geography"`
	Roads       []map[string]any `json:"roads"`
	Areas       []map[string]any `json:"areas"`
}

type open511Geometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

func fetchOpen511(ctx context.Context, feed feedSpec) ([]events.Event, error) {
	var raw open511Response
	if err := httpx.GetJSON(ctx, feed.URL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Events))
	for _, row := range raw.Events {
		ev, ok := eventFromOpen511(feed, row)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromOpen511(feed feedSpec, row open511Event) (events.Event, bool) {
	ts := parseTime(firstNonEmpty(row.Updated, row.Created))
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	lat, lon, ok := pointFromOpen511(row.Geography)
	if !ok {
		lat, lon = feed.Lat, feed.Lon
	}
	text := strings.Join([]string{row.Headline, row.Description, row.EventType, row.Severity}, " ")
	props := map[string]any{
		"source_provider":         feed.Label,
		"source_url":              row.URL,
		"feed_url":                feed.URL,
		"event_id":                row.ID,
		"headline":                row.Headline,
		"description":             row.Description,
		"status":                  row.Status,
		"event_type":              row.EventType,
		"severity":                row.Severity,
		"roads":                   row.Roads,
		"areas":                   row.Areas,
		"traffic_flow_signal":     flowSignal(text),
		"congestion_score":        congestionScore(text),
		"source_payload_validity": validity(ts, 30*time.Minute, "open511_event_updated_time"),
	}
	id := firstNonEmpty(row.ID, stableID(feed.URL+row.Headline+row.Description))
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  "open511:" + id,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func pointFromOpen511(g open511Geometry) (float64, float64, bool) {
	if strings.EqualFold(g.Type, "Point") {
		var coords []float64
		if err := json.Unmarshal(g.Coordinates, &coords); err == nil && len(coords) >= 2 {
			return coords[1], coords[0], validLatLon(coords[1], coords[0])
		}
	}
	if strings.EqualFold(g.Type, "LineString") {
		var coords [][]float64
		if err := json.Unmarshal(g.Coordinates, &coords); err == nil && len(coords) > 0 {
			var lat, lon float64
			var n int
			for _, c := range coords {
				if len(c) >= 2 && validLatLon(c[1], c[0]) {
					lat += c[1]
					lon += c[0]
					n++
				}
			}
			if n > 0 {
				return lat / float64(n), lon / float64(n), true
			}
		}
	}
	return 0, 0, false
}

func fetchDATEX2(ctx context.Context, feed feedSpec) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, feed.URL, map[string]string{"Accept": "application/xml,text/xml,*/*"})
	if err != nil {
		return nil, err
	}
	return eventsFromDATEX2(feed, buf), nil
}

type datexObservation struct {
	SiteID      string
	TS          time.Time
	Lat         float64
	Lon         float64
	SpeedKPH    float64
	FlowPerHour float64
}

func eventsFromDATEX2(feed feedSpec, buf []byte) []events.Event {
	obs := parseDATEX2(buf)
	out := make([]events.Event, 0, len(obs))
	now := time.Now().UTC()
	for _, o := range obs {
		ts := o.TS
		if ts.IsZero() {
			ts = now
		}
		lat, lon := o.Lat, o.Lon
		if !validLatLon(lat, lon) {
			lat, lon = feed.Lat, feed.Lon
		}
		props := map[string]any{
			"source_provider":         feed.Label,
			"feed_url":                feed.URL,
			"measurement_site_id":     o.SiteID,
			"average_speed_kph":       round(o.SpeedKPH, 1),
			"vehicle_flow_per_hour":   round(o.FlowPerHour, 1),
			"congestion_score":        datexCongestionScore(o.SpeedKPH, o.FlowPerHour),
			"traffic_flow_signal":     "datex2_measured_data",
			"source_payload_validity": validity(ts, 15*time.Minute, "datex2_measurement_time"),
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  "datex2:" + firstNonEmpty(o.SiteID, stableID(feed.URL+ts.Format(time.RFC3339))),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out
}

func parseDATEX2(buf []byte) []datexObservation {
	dec := xml.NewDecoder(bytes.NewReader(buf))
	var stack []string
	var cur datexObservation
	var inSite bool
	var out []datexObservation
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			name := t.Name.Local
			stack = append(stack, name)
			if name == "siteMeasurements" {
				inSite = true
				cur = datexObservation{}
			}
			if inSite && name == "measurementSiteReference" {
				for _, a := range t.Attr {
					if a.Name.Local == "id" || a.Name.Local == "targetClass" {
						if cur.SiteID == "" && strings.TrimSpace(a.Value) != "" && a.Name.Local == "id" {
							cur.SiteID = strings.TrimSpace(a.Value)
						}
					}
				}
			}
		case xml.CharData:
			if !inSite {
				continue
			}
			text := strings.TrimSpace(string(t))
			if text == "" {
				continue
			}
			top := stack[len(stack)-1]
			switch {
			case top == "measurementTimeDefault":
				cur.TS = parseTime(text)
			case top == "latitude":
				cur.Lat = parseFloat(text)
			case top == "longitude":
				cur.Lon = parseFloat(text)
			case top == "speed" && containsStack(stack, "averageVehicleSpeed"):
				cur.SpeedKPH = parseFloat(text)
			case top == "vehicleFlowRate" && containsStack(stack, "vehicleFlow"):
				cur.FlowPerHour = parseFloat(text)
			}
		case xml.EndElement:
			name := t.Name.Local
			if name == "siteMeasurements" {
				if cur.SiteID != "" || cur.SpeedKPH > 0 || cur.FlowPerHour > 0 {
					out = append(out, cur)
				}
				inSite = false
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	return out
}

func flowSignal(text string) string {
	text = strings.ToLower(text)
	switch {
	case strings.Contains(text, "closed"), strings.Contains(text, "closure"):
		return "closed_or_blocked"
	case strings.Contains(text, "single lane"), strings.Contains(text, "alternating"):
		return "lane_restriction"
	case strings.Contains(text, "delay"), strings.Contains(text, "congestion"), strings.Contains(text, "slow"):
		return "delay_or_congestion"
	default:
		return "road_event"
	}
}

func congestionScore(text string) float64 {
	text = strings.ToLower(text)
	score := 0.5
	if strings.Contains(text, "minor") {
		score = math.Max(score, 1)
	}
	if strings.Contains(text, "delay") || strings.Contains(text, "single lane") {
		score = math.Max(score, 2)
	}
	if strings.Contains(text, "major") || strings.Contains(text, "closed") || strings.Contains(text, "closure") {
		score = math.Max(score, 4)
	}
	return score
}

func datexCongestionScore(speedKPH, flow float64) float64 {
	score := 0.0
	if speedKPH > 0 && speedKPH < 25 {
		score += 2.5
	} else if speedKPH > 0 && speedKPH < 45 {
		score += 1.5
	}
	if flow > 1500 {
		score += 1
	}
	return math.Min(4, score)
}

func parseFeeds(raw string) []feedSpec {
	out := []feedSpec{}
	for _, part := range strings.Split(raw, ",") {
		fields := strings.Split(part, "|")
		if len(fields) < 2 {
			continue
		}
		f := feedSpec{Label: strings.TrimSpace(fields[0]), URL: strings.TrimSpace(fields[1])}
		if len(fields) >= 4 {
			f.Lat = parseFloat(fields[2])
			f.Lon = parseFloat(fields[3])
		}
		if f.Label != "" && f.URL != "" {
			out = append(out, f)
		}
	}
	return out
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	layouts := []string{time.RFC3339, "2006-01-02T15:04:05Z07:00", "2006-01-02T15:04:05"}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func containsStack(stack []string, want string) bool {
	for _, s := range stack {
		if s == want {
			return true
		}
	}
	return false
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func stableID(s string) string {
	h := uint32(2166136261)
	for _, b := range []byte(strings.ToLower(s)) {
		h ^= uint32(b)
		h *= 16777619
	}
	return fmt.Sprintf("%08x", h)
}

func parseFloat(raw string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return v
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}
