// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package direct_weather_alerts ingests direct no-key national alert feeds
// that are not fully covered by the existing WMO/MeteoAlarm collectors or
// that provide better geometry/detail from the issuing service.
package direct_weather_alerts

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
)

const (
	sourceID   = "direct_weather_alerts"
	canadaURL  = "https://api.weather.gc.ca/collections/weather-alerts/items?f=json&limit=500"
	norwayURL  = "https://api.met.no/weatherapi/metalerts/2.0/current.json"
	jmaMapURL  = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
	jmaPublic  = "https://www.jma.go.jp/bosai/warning/"
	sourceKind = "direct_national_official_weather_alert_feed"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, fetchJMA(ctx)...)
	out = append(out, fetchNorway(ctx)...)
	out = append(out, fetchCanada(ctx)...)
	return dedupe(out), nil
}

type featureCollection struct {
	Features []feature `json:"features"`
}

type feature struct {
	ID         any             `json:"id"`
	Geometry   geoJSONGeometry `json:"geometry"`
	Properties map[string]any  `json:"properties"`
}

type geoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

func fetchCanada(ctx context.Context) []events.Event {
	var raw featureCollection
	if err := getJSON(ctx, canadaURL, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, ok := geoJSONCentroid(f.Geometry)
		if !ok {
			continue
		}
		p := f.Properties
		id := firstNonEmpty(textAny(p["id"]), textAny(f.ID), stableID(fmt.Sprint(p)))
		ts := parseTimeAny(p["publication_datetime"], p["effective_datetime"], p["onset_datetime"])
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		expires := parseTimeAny(p["expiration_datetime"], p["event_end_datetime"])
		eventName := firstNonEmpty(textAny(p["alert_name_en"]), textAny(p["alert_short_name_en"]), textAny(p["alert_code"]))
		props := baseProps("Environment and Climate Change Canada GeoMet", canadaURL, "https://weather.gc.ca/warnings/index_e.html", eventName, textAny(p["alert_text_en"]))
		copyProps(props, p)
		props["identifier"] = id
		props["event"] = eventName
		props["headline"] = firstNonEmpty(textAny(p["alert_name_en"]), textAny(p["feature_name_en"]))
		props["areaDesc"] = firstNonEmpty(textAny(p["feature_name_en"]), textAny(p["alert_name_en"]))
		props["severity"] = severityFromAlertType(textAny(p["alert_type"]))
		props["urgency"] = "Expected"
		props["certainty"] = "Likely"
		props["country"] = "Canada"
		props["country_code"] = "CA"
		props["source_payload_validity"] = validity(ts, expires, "canada_geomet_alert_validity")
		out = append(out, event(ts, "canada:"+id, lat, lon, props))
	}
	return out
}

func fetchNorway(ctx context.Context) []events.Event {
	var raw featureCollection
	if err := getJSON(ctx, norwayURL, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, ok := geoJSONCentroid(f.Geometry)
		if !ok {
			continue
		}
		p := f.Properties
		id := firstNonEmpty(textAny(p["id"]), textAny(f.ID), stableID(fmt.Sprint(p)))
		ts := parseTimeAny(p["sent"], p["onset"], p["eventEndingTime"])
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		expires := parseTimeAny(p["expires"], p["eventEndingTime"])
		eventName := firstNonEmpty(textAny(p["event"]), textAny(p["eventAwarenessName"]), textAny(p["awareness_type"]))
		props := baseProps("Norwegian Meteorological Institute MetAlerts", norwayURL, "https://api.met.no/weatherapi/metalerts/2.0/documentation", eventName, textAny(p["description"]))
		copyProps(props, p)
		props["identifier"] = id
		props["event"] = eventName
		props["headline"] = textAny(p["title"])
		props["areaDesc"] = textAny(p["area"])
		props["severity"] = firstNonEmpty(textAny(p["severity"]), severityFromAwareness(textAny(p["awareness_level"])))
		props["urgency"] = firstNonEmpty(textAny(p["urgency"]), "Expected")
		props["certainty"] = firstNonEmpty(textAny(p["certainty"]), "Likely")
		props["country"] = "Norway"
		props["country_code"] = "NO"
		props["source_payload_validity"] = validity(ts, expires, "norway_metalerts_validity")
		out = append(out, event(ts, "norway:"+id, lat, lon, props))
	}
	return out
}

type jmaWarningSet []struct {
	ReportDatetime string `json:"reportDatetime"`
	AreaTypes      []struct {
		Areas []struct {
			Code     string `json:"code"`
			Warnings []struct {
				Code   string `json:"code"`
				Status string `json:"status"`
			} `json:"warnings"`
		} `json:"areas"`
	} `json:"areaTypes"`
}

func fetchJMA(ctx context.Context) []events.Event {
	var raw jmaWarningSet
	if err := getJSON(ctx, jmaMapURL, &raw); err != nil {
		return nil
	}
	out := []events.Event{}
	seen := map[string]bool{}
	for _, report := range raw {
		ts := parseTimeAny(report.ReportDatetime)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		for _, at := range report.AreaTypes {
			for _, area := range at.Areas {
				pref := jmaPref(area.Code)
				if pref.Name == "" || !validLatLon(pref.Lat, pref.Lon) {
					continue
				}
				for _, warning := range area.Warnings {
					if !jmaActiveStatus(warning.Status) || warning.Code == "" {
						continue
					}
					name := jmaWarningName(warning.Code)
					key := pref.Code + ":" + warning.Code
					if seen[key] {
						continue
					}
					seen[key] = true
					severity := jmaSeverity(warning.Code)
					props := baseProps("Japan Meteorological Agency warnings", jmaMapURL, jmaPublic, name, "")
					props["identifier"] = key
					props["event"] = name
					props["headline"] = strings.TrimSpace(pref.Name + " " + name)
					props["areaDesc"] = pref.Name
					props["severity"] = severity
					props["urgency"] = "Expected"
					props["certainty"] = "Likely"
					props["status"] = warning.Status
					props["warning_code"] = warning.Code
					props["area_code"] = area.Code
					props["country"] = "Japan"
					props["country_code"] = "JP"
					props["source_payload_validity"] = validity(ts, ts.Add(12*time.Hour), "jma_warning_validity")
					out = append(out, event(ts, "jma:"+key+":"+report.ReportDatetime, pref.Lat, pref.Lon, props))
				}
			}
		}
	}
	return out
}

func getJSON(ctx context.Context, rawURL string, out any) error {
	buf, err := exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "30", "-A", "gordios/0.1", rawURL).Output()
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func baseProps(provider, endpoint, publicURL, eventName, desc string) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"source_public_url":    publicURL,
		"source_provider_kind": sourceKind,
		"title":                strings.TrimSpace(eventName),
		"description":          strings.TrimSpace(desc),
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	collectorutil.AddAlertScores(props)
	collectorutil.AddWMOHazardScores(props)
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func severityFromAlertType(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "warning":
		return "Severe"
	case "watch":
		return "Moderate"
	case "advisory", "statement":
		return "Minor"
	default:
		return firstNonEmpty(s, "Moderate")
	}
}

func severityFromAwareness(s string) string {
	l := strings.ToLower(s)
	switch {
	case strings.Contains(l, "red") || strings.Contains(l, "extreme"):
		return "Extreme"
	case strings.Contains(l, "orange") || strings.Contains(l, "severe"):
		return "Severe"
	case strings.Contains(l, "yellow") || strings.Contains(l, "moderate"):
		return "Moderate"
	default:
		return ""
	}
}

type prefPoint struct {
	Code string
	Name string
	Lat  float64
	Lon  float64
}

func jmaPref(areaCode string) prefPoint {
	if len(areaCode) < 2 {
		return prefPoint{}
	}
	if p, ok := jmaPrefCentroids[areaCode[:2]]; ok {
		return p
	}
	return prefPoint{}
}

func jmaActiveStatus(status string) bool {
	status = strings.TrimSpace(status)
	return status != "" && status != "解除" && status != "発表警報・注意報はなし"
}

func jmaWarningName(code string) string {
	if s, ok := jmaWarningNames[code]; ok {
		return s
	}
	return "JMA weather warning " + code
}

func jmaSeverity(code string) string {
	switch code {
	case "02", "03", "04", "05", "06", "07", "08":
		return "Severe"
	case "50", "51", "52", "53", "54", "55", "56", "57":
		return "Extreme"
	default:
		return "Moderate"
	}
}

var jmaWarningNames = map[string]string{
	"02": "snowstorm warning", "03": "heavy rain warning", "04": "flood warning",
	"05": "storm warning", "06": "heavy snow warning", "07": "high wave warning",
	"08": "storm surge warning", "10": "heavy rain advisory", "12": "heavy snow advisory",
	"13": "snowstorm advisory", "14": "thunderstorm advisory", "15": "strong wind advisory",
	"16": "high wave advisory", "17": "snowmelt advisory", "18": "flood advisory",
	"19": "storm surge advisory", "20": "dense fog advisory", "21": "dry air advisory",
	"22": "avalanche advisory", "23": "ice accretion advisory", "24": "frost advisory",
	"25": "low temperature advisory", "26": "snow accretion advisory",
}

var jmaPrefCentroids = map[string]prefPoint{
	"01": {"01", "Hokkaido", 43.0642, 141.3469}, "02": {"02", "Aomori", 40.8244, 140.7400},
	"03": {"03", "Iwate", 39.7036, 141.1527}, "04": {"04", "Miyagi", 38.2688, 140.8721},
	"05": {"05", "Akita", 39.7186, 140.1024}, "06": {"06", "Yamagata", 38.2404, 140.3633},
	"07": {"07", "Fukushima", 37.7503, 140.4676}, "08": {"08", "Ibaraki", 36.3418, 140.4468},
	"09": {"09", "Tochigi", 36.5657, 139.8836}, "10": {"10", "Gunma", 36.3907, 139.0604},
	"11": {"11", "Saitama", 35.8574, 139.6489}, "12": {"12", "Chiba", 35.6051, 140.1233},
	"13": {"13", "Tokyo", 35.6895, 139.6917}, "14": {"14", "Kanagawa", 35.4478, 139.6425},
	"15": {"15", "Niigata", 37.9024, 139.0232}, "16": {"16", "Toyama", 36.6953, 137.2113},
	"17": {"17", "Ishikawa", 36.5947, 136.6256}, "18": {"18", "Fukui", 36.0652, 136.2216},
	"19": {"19", "Yamanashi", 35.6642, 138.5684}, "20": {"20", "Nagano", 36.6513, 138.1810},
	"21": {"21", "Gifu", 35.3912, 136.7223}, "22": {"22", "Shizuoka", 34.9769, 138.3831},
	"23": {"23", "Aichi", 35.1802, 136.9066}, "24": {"24", "Mie", 34.7303, 136.5086},
	"25": {"25", "Shiga", 35.0045, 135.8686}, "26": {"26", "Kyoto", 35.0211, 135.7556},
	"27": {"27", "Osaka", 34.6863, 135.5200}, "28": {"28", "Hyogo", 34.6913, 135.1830},
	"29": {"29", "Nara", 34.6851, 135.8048}, "30": {"30", "Wakayama", 34.2260, 135.1675},
	"31": {"31", "Tottori", 35.5039, 134.2377}, "32": {"32", "Shimane", 35.4723, 133.0505},
	"33": {"33", "Okayama", 34.6618, 133.9350}, "34": {"34", "Hiroshima", 34.3963, 132.4594},
	"35": {"35", "Yamaguchi", 34.1859, 131.4714}, "36": {"36", "Tokushima", 34.0658, 134.5593},
	"37": {"37", "Kagawa", 34.3401, 134.0434}, "38": {"38", "Ehime", 33.8416, 132.7661},
	"39": {"39", "Kochi", 33.5597, 133.5311}, "40": {"40", "Fukuoka", 33.6064, 130.4181},
	"41": {"41", "Saga", 33.2494, 130.2988}, "42": {"42", "Nagasaki", 32.7448, 129.8737},
	"43": {"43", "Kumamoto", 32.7898, 130.7417}, "44": {"44", "Oita", 33.2382, 131.6126},
	"45": {"45", "Miyazaki", 31.9111, 131.4239}, "46": {"46", "Kagoshima", 31.5602, 130.5581},
	"47": {"47", "Okinawa", 26.2124, 127.6809},
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
	if n, err := fmt.Sscanf(s, "%d", new(int64)); err == nil && n == 1 {
		var i int64
		_, _ = fmt.Sscanf(s, "%d", &i)
		if i > 100000000000 {
			return time.UnixMilli(i).UTC()
		}
		if i > 1000000000 {
			return time.Unix(i, 0).UTC()
		}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05-07:00", "2006-01-02 15:04"} {
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
		end = start.Add(12 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
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
		var f float64
		if _, err := fmt.Sscanf(strings.TrimSpace(x), "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
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

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
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
