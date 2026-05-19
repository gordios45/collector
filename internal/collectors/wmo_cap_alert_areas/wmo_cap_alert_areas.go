// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// WMO Alert Hub CAP XML alert areas.
//
// The summary feed can contain thousands of active alerts, so this collector
// deliberately fan-outs only the newest severe/extreme CAP XML documents.
// Polygon and circle geometries are preserved when the CAP message carries
// them; otherwise we keep a member-centroid fallback with geometry_source set.
package wmo_cap_alert_areas

import (
	"context"
	"encoding/xml"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	alertsURL    = "https://severeweather.wmo.int/json/wmo_all.json"
	membersURL   = "https://severeweather.wmo.int/json/wmo_member.json?20240904"
	capBaseURL   = "https://severeweather.wmo.int/v2/cap-alerts/"
	maxCAPFetch  = 120
	maxAreaRows  = 500
	circlePoints = 32
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "wmo_cap_alert_areas" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type alertsResponse struct {
	LastUpdated string      `json:"lastUpdated"`
	Items       []alertItem `json:"items"`
}

type alertItem struct {
	ID        string `json:"id"`
	Event     string `json:"event"`
	Headline  string `json:"headline"`
	Sent      string `json:"sent"`
	Expires   string `json:"expires"`
	AreaDesc  string `json:"areaDesc"`
	MID       string `json:"mid"`
	Region    string `json:"ra"`
	Severity  int    `json:"s"`
	Urgency   int    `json:"u"`
	Certainty int    `json:"c"`
	URL       string `json:"url"`
	Effective string `json:"effective"`
}

type memberRegion struct {
	Members []member `json:"members"`
}

type member struct {
	MID  string  `json:"mid"`
	Name string  `json:"name"`
	Dept string  `json:"dept"`
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
	Code string  `json:"code"`
	Reg  int     `json:"reg"`
}

type capAlert struct {
	Identifier string    `xml:"identifier"`
	Sender     string    `xml:"sender"`
	Sent       string    `xml:"sent"`
	Status     string    `xml:"status"`
	MsgType    string    `xml:"msgType"`
	Scope      string    `xml:"scope"`
	Infos      []capInfo `xml:"info"`
}

type capInfo struct {
	Language    string    `xml:"language"`
	Category    []string  `xml:"category"`
	Event       string    `xml:"event"`
	Urgency     string    `xml:"urgency"`
	Severity    string    `xml:"severity"`
	Certainty   string    `xml:"certainty"`
	Effective   string    `xml:"effective"`
	Onset       string    `xml:"onset"`
	Expires     string    `xml:"expires"`
	SenderName  string    `xml:"senderName"`
	Headline    string    `xml:"headline"`
	Description string    `xml:"description"`
	Instruction string    `xml:"instruction"`
	Areas       []capArea `xml:"area"`
}

type capArea struct {
	AreaDesc string   `xml:"areaDesc"`
	Polygon  []string `xml:"polygon"`
	Circle   []string `xml:"circle"`
	Geocode  []struct {
		ValueName string `xml:"valueName"`
		Value     string `xml:"value"`
	} `xml:"geocode"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	members, err := fetchMembers(ctx)
	if err != nil {
		return nil, err
	}
	var raw alertsResponse
	if err := httpx.GetJSON(ctx, alertsURL, nil, &raw); err != nil {
		return nil, err
	}
	out := []events.Event{}
	fetched := 0
	for _, item := range raw.Items {
		if fetched >= maxCAPFetch || len(out) >= maxAreaRows {
			break
		}
		if item.URL == "" || item.Severity > 2 || item.Severity <= 0 {
			continue
		}
		fetched++
		capURL := normalizeCAPURL(item.URL)
		buf, err := httpx.GetBytes(ctx, capURL, map[string]string{"Accept": "application/xml,text/xml"})
		if err != nil {
			continue
		}
		evs, err := eventsFromCAP(buf, item, members[item.MID], capURL, raw.LastUpdated)
		if err != nil {
			continue
		}
		out = append(out, evs...)
	}
	return out, nil
}

func fetchMembers(ctx context.Context) (map[string]member, error) {
	var raw []memberRegion
	if err := httpx.GetJSON(ctx, membersURL, nil, &raw); err != nil {
		return nil, err
	}
	out := map[string]member{}
	for _, region := range raw {
		for _, m := range region.Members {
			if m.MID != "" {
				out[m.MID] = m
			}
		}
	}
	return out, nil
}

func eventsFromCAP(buf []byte, item alertItem, m member, capURL, lastUpdated string) ([]events.Event, error) {
	var alert capAlert
	if err := xml.Unmarshal(buf, &alert); err != nil {
		return nil, err
	}
	info := chooseInfo(alert.Infos)
	if len(info.Areas) == 0 {
		info.Areas = []capArea{{AreaDesc: item.AreaDesc}}
	}
	ts := firstTime(alert.Sent, info.Effective, item.Sent, item.Effective, lastUpdated)
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	identifier := firstNonEmpty(alert.Identifier, item.ID)
	out := []events.Event{}
	for i, area := range info.Areas {
		geoms := geometriesForArea(area)
		if len(geoms) == 0 && (m.Lat != 0 || m.Lng != 0) {
			geoms = []areaGeom{{
				WKT:            fmt.Sprintf("POINT(%.6f %.6f)", m.Lng, m.Lat),
				Lat:            m.Lat,
				Lon:            m.Lng,
				GeometrySource: "member_centroid",
			}}
		}
		if len(geoms) == 0 {
			continue
		}
		for j, geom := range geoms {
			props := map[string]any{
				"identifier":          identifier,
				"id":                  item.ID,
				"event":               firstNonEmpty(info.Event, item.Event),
				"headline":            firstNonEmpty(info.Headline, item.Headline),
				"sent":                firstNonEmpty(alert.Sent, item.Sent),
				"effective":           firstNonEmpty(info.Effective, item.Effective),
				"onset":               info.Onset,
				"expires":             firstNonEmpty(info.Expires, item.Expires),
				"areaDesc":            firstNonEmpty(area.AreaDesc, item.AreaDesc),
				"severity":            firstNonEmpty(info.Severity, severityLabel(item.Severity)),
				"urgency":             firstNonEmpty(info.Urgency, urgencyLabel(item.Urgency)),
				"certainty":           firstNonEmpty(info.Certainty, certaintyLabel(item.Certainty)),
				"category":            strings.Join(info.Category, ","),
				"sender":              alert.Sender,
				"senderName":          info.SenderName,
				"status":              alert.Status,
				"msgType":             alert.MsgType,
				"scope":               alert.Scope,
				"member_id":           item.MID,
				"member_country":      m.Name,
				"member_code":         m.Code,
				"member_department":   m.Dept,
				"wmo_region":          firstNonEmpty(item.Region, strconv.Itoa(m.Reg)),
				"cap_url":             capURL,
				"geometry_source":     geom.GeometrySource,
				"source_api_endpoint": alertsURL,
				"lastUpdated":         lastUpdated,
			}
			out = append(out, events.Event{
				Ts:     ts,
				Source: "wmo_cap_alert_areas",
				ExtID:  fmt.Sprintf("%s:%d:%d", identifier, i, j),
				Lat:    geom.Lat,
				Lon:    geom.Lon,
				Geom:   geom.WKT,
				Props:  props,
			})
		}
	}
	return out, nil
}

type areaGeom struct {
	WKT            string
	Lat            float64
	Lon            float64
	GeometrySource string
}

func geometriesForArea(area capArea) []areaGeom {
	out := []areaGeom{}
	for _, raw := range area.Polygon {
		ring := parseCAPPolygon(raw)
		if wkt := polygonWKT(ring); wkt != "" {
			lon, lat := centroid(ring)
			out = append(out, areaGeom{WKT: wkt, Lat: lat, Lon: lon, GeometrySource: "cap_polygon"})
		}
	}
	for _, raw := range area.Circle {
		ring := parseCAPCircle(raw)
		if wkt := polygonWKT(ring); wkt != "" {
			lon, lat := centroid(ring)
			out = append(out, areaGeom{WKT: wkt, Lat: lat, Lon: lon, GeometrySource: "cap_circle"})
		}
	}
	return out
}

func parseCAPPolygon(raw string) [][2]float64 {
	out := [][2]float64{}
	for _, token := range strings.Fields(raw) {
		parts := strings.Split(token, ",")
		if len(parts) < 2 {
			continue
		}
		lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err1 == nil && err2 == nil {
			out = append(out, [2]float64{lon, lat})
		}
	}
	return out
}

func parseCAPCircle(raw string) [][2]float64 {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) < 2 {
		return nil
	}
	center := strings.Split(fields[0], ",")
	if len(center) < 2 {
		return nil
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(center[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(center[1]), 64)
	radiusKM, err3 := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
	if err1 != nil || err2 != nil || err3 != nil || radiusKM <= 0 {
		return nil
	}
	ring := make([][2]float64, 0, circlePoints)
	latDeg := radiusKM / 111.32
	lonScale := math.Cos(lat * math.Pi / 180)
	if math.Abs(lonScale) < 0.05 {
		lonScale = 0.05
	}
	lonDeg := radiusKM / (111.32 * lonScale)
	for i := 0; i < circlePoints; i++ {
		a := (float64(i) / float64(circlePoints)) * 2 * math.Pi
		ring = append(ring, [2]float64{lon + math.Cos(a)*lonDeg, lat + math.Sin(a)*latDeg})
	}
	return ring
}

func polygonWKT(ring [][2]float64) string {
	if len(ring) < 3 {
		return ""
	}
	pts := make([]string, 0, len(ring)+1)
	for _, p := range ring {
		pts = append(pts, fmt.Sprintf("%.6f %.6f", p[0], p[1]))
	}
	if ring[0] != ring[len(ring)-1] {
		pts = append(pts, fmt.Sprintf("%.6f %.6f", ring[0][0], ring[0][1]))
	}
	return "POLYGON((" + strings.Join(pts, ",") + "))"
}

func centroid(ring [][2]float64) (lon, lat float64) {
	if len(ring) == 0 {
		return 0, 0
	}
	for _, p := range ring {
		lon += p[0]
		lat += p[1]
	}
	n := float64(len(ring))
	return lon / n, lat / n
}

func chooseInfo(infos []capInfo) capInfo {
	if len(infos) == 0 {
		return capInfo{}
	}
	for _, info := range infos {
		lang := strings.ToLower(info.Language)
		if strings.HasPrefix(lang, "en") || lang == "" {
			return info
		}
	}
	return infos[0]
}

func normalizeCAPURL(s string) string {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return s
	}
	return capBaseURL + strings.TrimLeft(s, "/")
}

func firstTime(vals ...string) time.Time {
	for _, v := range vals {
		if t := parseTime(v); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339Nano, time.RFC3339} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func severityLabel(v int) string {
	switch v {
	case 1:
		return "Extreme"
	case 2:
		return "Severe"
	case 3:
		return "Moderate"
	case 4:
		return "Minor"
	}
	return strconv.Itoa(v)
}

func urgencyLabel(v int) string {
	switch v {
	case 1:
		return "Immediate"
	case 2:
		return "Expected"
	case 3:
		return "Future"
	case 4:
		return "Past"
	}
	return strconv.Itoa(v)
}

func certaintyLabel(v int) string {
	switch v {
	case 1:
		return "Observed"
	case 2:
		return "Likely"
	case 3:
		return "Possible"
	case 4:
		return "Unlikely"
	}
	return strconv.Itoa(v)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
