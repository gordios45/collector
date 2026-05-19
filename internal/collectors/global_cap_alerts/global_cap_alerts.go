// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package global_cap_alerts ingests the public Alert-Hub/Esri global CAP
// feature layer. It complements WMO Alert Hub by carrying active CAP alerts
// from broader civil authorities, including transport and infrastructure
// categories, without requiring an API key.
package global_cap_alerts

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID = "global_cap_alerts"
	rssURL   = "http://cap-alerts.s3.us-east-1.amazonaws.com/unfiltered/rss.xml"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

type featureCollection struct {
	Features []feature `json:"features"`
}

type feature struct {
	Geometry   geoJSONGeometry `json:"geometry"`
	Properties map[string]any  `json:"properties"`
}

type geoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := getBytes(ctx, rssURL, "application/rss+xml,application/xml,text/xml,*/*")
	if err != nil {
		return nil, err
	}
	var raw alertHubRSS
	if err := xml.Unmarshal(buf, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, minInt(len(raw.Items), 40))
	for i, item := range raw.Items {
		if i >= 40 {
			break
		}
		if ev, ok := fetchCAPEvent(ctx, item); ok {
			out = append(out, ev)
		}
	}
	return dedupe(out), nil
}

type alertHubRSS struct {
	Items []alertHubItem `xml:"channel>item"`
}

type alertHubItem struct {
	Title            string `xml:"title"`
	Link             string `xml:"link"`
	Description      string `xml:"description"`
	PubDate          string `xml:"pubDate"`
	Updated          string `xml:"http://www.w3.org/2005/Atom updated"`
	SourceFeed       string `xml:"http://alert-hub.org/cap-extensions sourceFeed"`
	AlertID          string `xml:"http://alert-hub.org/cap-extensions alertId"`
	ISOPubDate       string `xml:"http://alert-hub.org/cap-extensions isoPubDate"`
	PreservationCopy string `xml:"http://alert-hub.org/cap-extensions preservationCopy"`
}

type capAlert struct {
	Identifier string    `xml:"identifier"`
	Sender     string    `xml:"sender"`
	Sent       string    `xml:"sent"`
	Status     string    `xml:"status"`
	MsgType    string    `xml:"msgType"`
	Scope      string    `xml:"scope"`
	Info       []capInfo `xml:"info"`
}

type capInfo struct {
	Language    string    `xml:"language"`
	Category    []string  `xml:"category"`
	Event       string    `xml:"event"`
	Urgency     string    `xml:"urgency"`
	Severity    string    `xml:"severity"`
	Certainty   string    `xml:"certainty"`
	Headline    string    `xml:"headline"`
	Description string    `xml:"description"`
	Instruction string    `xml:"instruction"`
	Web         string    `xml:"web"`
	Effective   string    `xml:"effective"`
	Onset       string    `xml:"onset"`
	Expires     string    `xml:"expires"`
	Area        []capArea `xml:"area"`
}

type capArea struct {
	AreaDesc string   `xml:"areaDesc"`
	Polygon  []string `xml:"polygon"`
	Circle   []string `xml:"circle"`
}

func fetchCAPEvent(ctx context.Context, item alertHubItem) (events.Event, bool) {
	link := firstNonEmpty(item.PreservationCopy, item.Link)
	if link == "" {
		return events.Event{}, false
	}
	link = strings.Replace(link, "https://cap-alerts.s3.us-east-1.amazonaws.com/", "http://cap-alerts.s3.amazonaws.com/", 1)
	link = strings.Replace(link, "https://cap-alerts.s3.amazonaws.com/", "http://cap-alerts.s3.amazonaws.com/", 1)
	link = strings.Replace(link, "http://cap-alerts.s3.amazonaws.com/", "http://cap-alerts.s3.us-east-1.amazonaws.com/", 1)
	buf, err := getBytes(ctx, link, "application/cap+xml,application/xml,text/xml,*/*")
	if err != nil {
		return events.Event{}, false
	}
	var alert capAlert
	if err := xml.Unmarshal(buf, &alert); err != nil {
		return events.Event{}, false
	}
	return eventFromCAP(item, alert)
}

var capHTTPClient = &http.Client{
	Timeout: 45 * time.Second,
	Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "tcp4", addr)
		},
		ForceAttemptHTTP2:   false,
		TLSHandshakeTimeout: 15 * time.Second,
	},
}

func getBytes(ctx context.Context, rawURL, accept string) ([]byte, error) {
	if strings.Contains(rawURL, "cap-alerts.s3") {
		return curlBytes(ctx, rawURL)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	resp, err := capHTTPClient.Do(req)
	if err != nil {
		return curlBytes(ctx, rawURL)
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s -> %d: %s", rawURL, resp.StatusCode, string(buf[:minInt(len(buf), 400)]))
	}
	return buf, nil
}

func curlBytes(ctx context.Context, rawURL string) ([]byte, error) {
	if _, err := exec.LookPath("curl"); err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "30", "-A", "gordios/0.1", rawURL).Output()
}

func eventFromCAP(item alertHubItem, alert capAlert) (events.Event, bool) {
	info := chooseInfo(alert.Info)
	if info.Event == "" && item.Title != "" {
		info.Event = item.Title
	}
	lat, lon, ok := capInfoCentroid(info)
	if !ok {
		return events.Event{}, false
	}
	p := map[string]any{
		"identifier":  firstNonEmpty(alert.Identifier, item.AlertID),
		"sender":      alert.Sender,
		"sent":        firstNonEmpty(alert.Sent, item.ISOPubDate, item.PubDate),
		"event":       firstNonEmpty(info.Event, item.Title),
		"headline":    firstNonEmpty(info.Headline, item.Title),
		"description": firstNonEmpty(info.Description, item.Description),
		"instruction": info.Instruction,
		"areaDesc":    firstAreaDesc(info),
		"severity":    info.Severity,
		"urgency":     info.Urgency,
		"certainty":   info.Certainty,
		"status":      alert.Status,
		"msgType":     alert.MsgType,
		"category":    strings.Join(info.Category, ","),
		"web":         info.Web,
		"expires":     info.Expires,
		"onset":       info.Onset,
		"effective":   info.Effective,
	}
	id := firstNonEmpty(alert.Identifier, item.AlertID, stableID(firstNonEmpty(item.Link, item.Title)))
	ts := parseTimeAny(p["sent"], p["effective"], p["onset"])
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	expires := parseTimeAny(p["expires"])
	props := map[string]any{
		"source_provider":         "Alert-Hub global CAP RSS",
		"source_api_endpoint":     rssURL,
		"source_public_url":       firstNonEmpty(item.Link, info.Web, rssURL),
		"source_provider_kind":    "aggregated_official_cap_alert_feed",
		"identifier":              id,
		"sender":                  alert.Sender,
		"sent":                    formatTime(ts),
		"event":                   p["event"],
		"headline":                p["headline"],
		"description":             p["description"],
		"instruction":             p["instruction"],
		"areaDesc":                p["areaDesc"],
		"severity":                p["severity"],
		"urgency":                 p["urgency"],
		"certainty":               p["certainty"],
		"status":                  alert.Status,
		"msgType":                 alert.MsgType,
		"category":                p["category"],
		"web":                     p["web"],
		"source_feed":             item.SourceFeed,
		"alert_hub_updated":       item.Updated,
		"labels":                  labelsFor(p),
		"civil_alert_score":       civilAlertScore(p),
		"source_payload_validity": validity(ts, expires, "global_cap_active_alert_window"),
	}
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  "global_cap:" + stableID(id),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func eventFromFeature(feat feature) (events.Event, bool) {
	if len(feat.Properties) == 0 {
		return events.Event{}, false
	}
	lat, lon, ok := geoJSONCentroid(feat.Geometry)
	if !ok {
		return events.Event{}, false
	}
	p := feat.Properties
	id := firstNonEmpty(textAny(p["identifier"]), stableID(fmt.Sprint(p)))
	ts := parseTimeAny(p["sent"], p["effective"], p["onset"])
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	expires := parseTimeAny(p["expires"])
	props := map[string]any{
		"source_provider":         "Alert-Hub / Esri CAP Alerts Feed",
		"source_api_endpoint":     rssURL,
		"source_public_url":       "https://www.arcgis.com/home/item.html?id=6e00ddc7558f48f98996502fb367473f",
		"source_provider_kind":    "aggregated_official_cap_alert_feed",
		"identifier":              id,
		"sender":                  textAny(p["sender"]),
		"sent":                    formatTime(ts),
		"event":                   textAny(p["event"]),
		"headline":                textAny(p["headline"]),
		"description":             textAny(p["description"]),
		"instruction":             textAny(p["instruction"]),
		"areaDesc":                textAny(p["areaDesc"]),
		"severity":                textAny(p["severity"]),
		"urgency":                 textAny(p["urgency"]),
		"certainty":               textAny(p["certainty"]),
		"status":                  textAny(p["status"]),
		"msgType":                 textAny(p["msgType"]),
		"category":                textAny(p["category"]),
		"web":                     textAny(p["web"]),
		"labels":                  labelsFor(p),
		"civil_alert_score":       civilAlertScore(p),
		"source_payload_validity": validity(ts, expires, "global_cap_active_alert_window"),
	}
	copyProps(props, p)
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  "global_cap:" + stableID(id+fmt.Sprintf(":%.4f:%.4f", lat, lon)),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func chooseInfo(infos []capInfo) capInfo {
	if len(infos) == 0 {
		return capInfo{}
	}
	for _, info := range infos {
		if strings.HasPrefix(strings.ToLower(info.Language), "en") {
			return info
		}
	}
	return infos[0]
}

func capInfoCentroid(info capInfo) (float64, float64, bool) {
	for _, area := range info.Area {
		for _, polygon := range area.Polygon {
			if lat, lon, ok := polygonCentroid(polygon); ok {
				return lat, lon, true
			}
		}
		for _, circle := range area.Circle {
			if lat, lon, ok := circlePoint(circle); ok {
				return lat, lon, true
			}
		}
	}
	return 0, 0, false
}

func polygonCentroid(raw string) (float64, float64, bool) {
	var latSum, lonSum float64
	var count int
	for _, pair := range strings.Fields(raw) {
		lat, lon, ok := capPair(pair)
		if !ok {
			continue
		}
		latSum += lat
		lonSum += lon
		count++
	}
	if count == 0 {
		return 0, 0, false
	}
	lat, lon := latSum/float64(count), lonSum/float64(count)
	return lat, lon, validLatLon(lat, lon)
}

func circlePoint(raw string) (float64, float64, bool) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return 0, 0, false
	}
	return capPair(fields[0])
}

func capPair(raw string) (float64, float64, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	return lat, lon, err1 == nil && err2 == nil && validLatLon(lat, lon)
}

func firstAreaDesc(info capInfo) string {
	for _, area := range info.Area {
		if strings.TrimSpace(area.AreaDesc) != "" {
			return strings.TrimSpace(area.AreaDesc)
		}
	}
	return ""
}

func labelsFor(p map[string]any) []string {
	text := strings.ToLower(strings.Join([]string{
		textAny(p["event"]), textAny(p["headline"]), textAny(p["description"]), textAny(p["category"]),
	}, " "))
	out := []string{"cap_alert"}
	if containsAny(text, "transport", "road", "rail", "traffic", "airport", "closure", "accident") {
		out = append(out, "transport_disruption")
	}
	if containsAny(text, "power", "electric", "outage", "infrastructure", "water supply", "telecom") {
		out = append(out, "infrastructure_disruption")
	}
	if containsAny(text, "flood", "rainfall", "rain", "inundation") {
		out = append(out, "flood")
	}
	if containsAny(text, "wildfire", "forest fire", "brush fire", "fire danger") {
		out = append(out, "wildfire")
	}
	if containsAny(text, "earthquake", "tsunami", "volcano", "volcanic") {
		out = append(out, "geological_hazard")
	}
	if containsAny(text, "storm", "thunderstorm", "cyclone", "hurricane", "typhoon", "wind", "snow", "blizzard") {
		out = append(out, "severe_weather")
	}
	return unique(out)
}

func civilAlertScore(p map[string]any) float64 {
	score := 0.5
	switch strings.ToLower(textAny(p["severity"])) {
	case "extreme":
		score += 2.2
	case "severe":
		score += 1.5
	case "moderate":
		score += 0.8
	case "minor":
		score += 0.2
	}
	if strings.EqualFold(textAny(p["urgency"]), "Immediate") {
		score += 0.4
	}
	if strings.EqualFold(textAny(p["certainty"]), "Observed") {
		score += 0.3
	}
	return propx.ClampFloat(score, 0, 3)
}

func geoJSONCentroid(g geoJSONGeometry) (float64, float64, bool) {
	var coords any
	if err := json.Unmarshal(g.Coordinates, &coords); err != nil {
		return 0, 0, false
	}
	var latSum, lonSum float64
	var count int
	var walk func(any)
	walk = func(v any) {
		arr, ok := v.([]any)
		if !ok {
			return
		}
		if len(arr) >= 2 {
			lon, lonOK := floatAny(arr[0])
			lat, latOK := floatAny(arr[1])
			if lonOK && latOK && validLatLon(lat, lon) {
				latSum += lat
				lonSum += lon
				count++
				return
			}
		}
		for _, child := range arr {
			walk(child)
		}
	}
	walk(coords)
	if count == 0 {
		return 0, 0, false
	}
	lat, lon := latSum/float64(count), lonSum/float64(count)
	return lat, lon, validLatLon(lat, lon)
}

func parseTimeAny(vals ...any) time.Time {
	for _, v := range vals {
		if t := parseTime(textAny(v)); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") {
		return time.Time{}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 100000000000 {
			return time.UnixMilli(n).UTC()
		}
		if n > 1000000000 {
			return time.Unix(n, 0).UTC()
		}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time, basis string) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(6 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func copyProps(dst map[string]any, src map[string]any) {
	for k, v := range src {
		if _, exists := dst[k]; exists {
			dst["raw_"+k] = v
		} else {
			dst[k] = v
		}
	}
}

func floatAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func textAny(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	case json.Number:
		return x.String()
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func unique(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
