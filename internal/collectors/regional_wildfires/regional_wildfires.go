// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package regional_wildfires ingests no-key official regional wildfire feeds.
package regional_wildfires

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "regional_wildfires"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchCalFire(ctx)...)
	out = append(out, c.fetchNSW(ctx)...)
	out = append(out, c.fetchGeoJSON(ctx, feedSpec{
		Provider:  "Victoria Emergency",
		URL:       "http://emergency.vic.gov.au/public/osom-geojson.json",
		PublicURL: "https://emergency.vic.gov.au/",
	})...)
	out = append(out, c.fetchSouthAustralia(ctx)...)
	out = append(out, c.fetchRSS(ctx, rssFeed{Provider: "Emergency WA", URL: "https://www.emergency.wa.gov.au/data/incident_FCAD.rss", PublicURL: "https://www.emergency.wa.gov.au/#map/incident/"})...)
	out = append(out, c.fetchRSS(ctx, rssFeed{Provider: "InciWeb", URL: "https://inciweb.nwcg.gov/feeds/rss/incidents/", PublicURL: "https://inciweb.wildfire.gov/"})...)
	out = append(out, c.fetchRSS(ctx, rssFeed{Provider: "Queensland Fire Department", URL: "https://www.qfes.qld.gov.au/data/alerts/bushfireAlert.xml", PublicURL: "https://www.fire.qld.gov.au/Current-Incidents"})...)
	return dedupe(out), nil
}

func (c *Collector) fetchCalFire(ctx context.Context) []events.Event {
	const endpoint = "https://www.fire.ca.gov/umbraco/api/IncidentApi/List?inactive=true"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		var wrapped struct {
			Incidents []map[string]any `json:"Incidents"`
			Data      []map[string]any `json:"data"`
		}
		if err2 := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &wrapped); err2 != nil {
			return nil
		}
		rows = append(wrapped.Incidents, wrapped.Data...)
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["Latitude"])
		lon, lonOK := floatAny(row["Longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(textAny(row["UniqueId"]), textAny(row["Id"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["Updated"], row["Started"], row["Created"])
		props := baseProps("CAL FIRE", endpoint, "https://www.fire.ca.gov/incidents/", id, firstNonEmpty(textAny(row["Name"]), textAny(row["Title"])))
		copyProps(props, row)
		props["county"] = textAny(row["County"])
		props["state"] = "CA"
		props["area_acres"], _ = floatAny(row["AcresBurned"])
		props["percent_contained"], _ = floatAny(row["PercentContained"])
		props["wildfire_context_score"] = wildfireScore(props)
		out = append(out, event(ts, "calfire:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchNSW(ctx context.Context) []events.Event {
	const endpoint = "http://www.rfs.nsw.gov.au/feeds/majorIncidents.json"
	return c.fetchGeoJSON(ctx, feedSpec{Provider: "NSW Rural Fire Service", URL: endpoint, PublicURL: "https://www.rfs.nsw.gov.au/fire-information/fires-near-me"})
}

func (c *Collector) fetchSouthAustralia(ctx context.Context) []events.Event {
	const endpoint = "https://s3-ap-southeast-2.amazonaws.com/data.eso.sa.gov.au/prod/cfs/criimson/cfs_current_incidents.json"
	var raw any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	rows := extractMaps(raw)
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, lon, ok := pointFromMap(row)
		if !ok {
			continue
		}
		title := firstNonEmpty(textAny(row["title"]), textAny(row["name"]), textAny(row["incidentName"]), textAny(row["incident_type"]))
		if title == "" || !wildfireText(title+" "+textAny(row["type"])+" "+textAny(row["description"])) {
			continue
		}
		id := firstNonEmpty(textAny(row["id"]), textAny(row["incidentNumber"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["created"], row["updated"], row["datetime"], row["published"])
		props := baseProps("South Australian Country Fire Service", endpoint, "https://www.cfs.sa.gov.au/site/warnings_and_incidents.jsp", id, title)
		copyProps(props, row)
		props["country"] = "Australia"
		props["wildfire_context_score"] = wildfireScore(props)
		out = append(out, event(ts, "south_au_cfs:"+id, lat, lon, props))
	}
	return out
}

type feedSpec struct {
	Provider  string
	URL       string
	PublicURL string
}

func (c *Collector) fetchGeoJSON(ctx context.Context, feed feedSpec) []events.Event {
	var raw struct {
		Features []struct {
			ID         any             `json:"id"`
			Properties map[string]any  `json:"properties"`
			Geometry   json.RawMessage `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, feed.URL, map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		lat, lon, ok := pointFromGeoJSON(feat.Geometry)
		if !ok {
			continue
		}
		row := feat.Properties
		title := firstNonEmpty(textAny(row["title"]), textAny(row["name"]), textAny(row["Name"]), textAny(row["incident_name"]))
		desc := strip(firstNonEmpty(textAny(row["description"]), textAny(row["Description"])))
		if !wildfireText(title + " " + desc + " " + textAny(row["type"])) {
			continue
		}
		id := firstNonEmpty(textAny(row["guid"]), textAny(row["id"]), textAny(feat.ID), stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		ts := parseTimeAny(row["pubDate"], row["updated"], row["created"], row["start"])
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, id, title)
		copyProps(props, row)
		props["description"] = desc
		props["wildfire_context_score"] = wildfireScore(props)
		out = append(out, event(ts, stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

type rssFeed struct {
	Provider  string
	URL       string
	PublicURL string
}

func (c *Collector) fetchRSS(ctx context.Context, feed rssFeed) []events.Event {
	buf, err := httpx.GetBytes(ctx, feed.URL, map[string]string{"Accept": "application/rss+xml,application/xml,text/xml,*/*"})
	if err != nil {
		return nil
	}
	var env feedEnvelope
	if err := xml.Unmarshal(buf, &env); err != nil {
		return nil
	}
	out := []events.Event{}
	for _, it := range env.Items {
		title, desc := strings.TrimSpace(it.Title), strip(it.Description)
		if !wildfireText(title + " " + desc) {
			continue
		}
		lat, lon, ok := parsePoint(firstNonEmpty(it.GeoRSSPoint, it.Point))
		if !ok {
			continue
		}
		ts := parseTimeString(firstNonEmpty(it.PubDate, it.Updated))
		id := firstNonEmpty(it.GUID, it.Link, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, id, title)
		props["description"] = desc
		props["link"] = strings.TrimSpace(it.Link)
		props["wildfire_context_score"] = wildfireScore(props)
		out = append(out, event(ts, "rss:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	for _, it := range env.Entries {
		title, desc := strings.TrimSpace(it.Title), strip(firstNonEmpty(it.Content, it.Summary))
		if !wildfireText(title + " " + desc) {
			continue
		}
		lat, lon, ok := parsePoint(firstNonEmpty(it.GeoRSSPoint, it.Point))
		if !ok {
			continue
		}
		ts := parseTimeString(firstNonEmpty(it.Updated, it.Published))
		id := firstNonEmpty(it.ID, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, id, title)
		props["description"] = desc
		props["category"] = it.Category.Term
		props["wildfire_context_score"] = wildfireScore(props)
		out = append(out, event(ts, "atom:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

type feedEnvelope struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	Updated     string `xml:"updated"`
	GUID        string `xml:"guid"`
	GeoRSSPoint string `xml:"http://www.georss.org/georss point"`
	Point       string `xml:"point"`
}

type atomEntry struct {
	Title       string `xml:"title"`
	ID          string `xml:"id"`
	Content     string `xml:"content"`
	Summary     string `xml:"summary"`
	Published   string `xml:"published"`
	Updated     string `xml:"updated"`
	GeoRSSPoint string `xml:"http://www.georss.org/georss point"`
	Point       string `xml:"point"`
	Category    struct {
		Term string `xml:"term,attr"`
	} `xml:"category"`
}

func baseProps(provider, endpoint, publicURL, id, title string) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"source_public_url":    publicURL,
		"incident_id":          id,
		"title":                strings.TrimSpace(title),
		"labels":               []string{"wildfire", "fire"},
		"source_provider_kind": "official_regional_wildfire_feed",
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	addRegionalWildfireSubtypeScores(props)
	props["source_payload_validity"] = map[string]any{
		"valid_start":    ts.Format(time.RFC3339),
		"valid_end":      ts.Add(48 * time.Hour).Format(time.RFC3339),
		"validity_basis": "official_wildfire_incident_publication_time",
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func wildfireScore(p map[string]any) float64 {
	score := 1.4
	acres, _ := floatAny(p["area_acres"])
	if acres == 0 {
		acres, _ = floatAny(p["AcresBurned"])
	}
	if acres > 0 {
		score += propx.ClampFloat(math.Log1p(acres)/8.0, 0, 1.2)
	}
	contained, ok := floatAny(p["percent_contained"])
	if !ok {
		contained, _ = floatAny(p["PercentContained"])
	}
	if contained > 0 && contained < 30 {
		score += 0.4
	}
	severity := strings.ToLower(textAny(p["category"]) + " " + textAny(p["status"]) + " " + textAny(p["description"]))
	if strings.Contains(severity, "watch and act") || strings.Contains(severity, "emergency warning") {
		score += 0.5
	}
	return propx.ClampFloat(score, 0, 3)
}

func addRegionalWildfireSubtypeScores(props map[string]any) {
	score, _ := floatAny(props["wildfire_context_score"])
	acres, _ := floatAny(props["area_acres"])
	if acres == 0 {
		acres, _ = floatAny(props["AcresBurned"])
	}
	if acres == 0 {
		acres, _ = floatAny(props["acres"])
	}
	contained, ok := floatAny(props["percent_contained"])
	if !ok {
		contained, _ = floatAny(props["PercentContained"])
	}
	text := strings.ToLower(strings.Join([]string{
		textAny(props["title"]),
		textAny(props["raw_title"]),
		textAny(props["description"]),
		textAny(props["category"]),
		textAny(props["type"]),
		textAny(props["incident_type"]),
		textAny(props["status"]),
		textAny(props["labels"]),
	}, " "))
	if plannedFireText(text) {
		props["planned_burn_score"] = propx.ClampFloat(math.Max(score, 1.4), 0, 3)
	}
	if uncontrolledFireText(text) {
		props["uncontrolled_fire_score"] = propx.ClampFloat(math.Max(score, 1.0), 0, 3)
	}
	if acres >= 1000 {
		props["large_fire_score"] = propx.ClampFloat(math.Log10(acres)/2.0, 0, 3)
	}
	if contained > 0 && contained < 30 {
		props["low_containment_score"] = 1.0
	}
}

func plannedFireText(text string) bool {
	return containsAny(text,
		"planned burn",
		"planned hazard reduction",
		"hazard reduction",
		"prescribed burn",
		"prescribed fire",
		"controlled burn",
		"control burn",
		"fuel reduction",
		"cultural burn",
		"pile burn",
		"backburn",
		"back burning",
		"burn off",
		"burnoff",
	)
}

func uncontrolledFireText(text string) bool {
	return containsAny(text,
		"out of control",
		"not under control",
		"emergency warning",
		"watch and act",
		"evacuate",
		"evacuation",
		"threatening",
		"rapid rate of spread",
	)
}

func wildfireText(s string) bool {
	l := strings.ToLower(s)
	return containsAny(l, "wildfire", "bush fire", "bushfire", "grass fire", "forest fire", "brush fire", "vegetation fire", "fire alert", "fire -")
}

func extractMaps(v any) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, item := range x {
			out = append(out, extractMaps(item)...)
		}
		return out
	case map[string]any:
		for _, key := range []string{"features", "incidents", "data", "results"} {
			if child, ok := x[key]; ok {
				rows := extractMaps(child)
				if len(rows) > 0 {
					return rows
				}
			}
		}
		return []map[string]any{x}
	default:
		return nil
	}
}

func pointFromMap(m map[string]any) (float64, float64, bool) {
	for _, ks := range [][2]string{{"lat", "lon"}, {"lat", "lng"}, {"latitude", "longitude"}, {"Latitude", "Longitude"}} {
		lat, latOK := floatAny(m[ks[0]])
		lon, lonOK := floatAny(m[ks[1]])
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	return 0, 0, false
}

func pointFromGeoJSON(raw json.RawMessage) (float64, float64, bool) {
	var g struct {
		Type        string            `json:"type"`
		Coordinates json.RawMessage   `json:"coordinates"`
		Geometries  []json.RawMessage `json:"geometries"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return 0, 0, false
	}
	if strings.EqualFold(g.Type, "Point") {
		var c []float64
		if err := json.Unmarshal(g.Coordinates, &c); err == nil && len(c) >= 2 && validLatLon(c[1], c[0]) {
			return c[1], c[0], true
		}
	}
	if strings.EqualFold(g.Type, "GeometryCollection") {
		for _, geom := range g.Geometries {
			if lat, lon, ok := pointFromGeoJSON(geom); ok {
				return lat, lon, true
			}
		}
	}
	return 0, 0, false
}

func parsePoint(s string) (float64, float64, bool) {
	parts := strings.Fields(strings.TrimSpace(strings.ReplaceAll(s, ",", " ")))
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(parts[0], 64)
	lon, err2 := strconv.ParseFloat(parts[1], 64)
	if err1 == nil && err2 == nil && validLatLon(lat, lon) {
		return lat, lon, true
	}
	return 0, 0, false
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

func parseTimeAny(vals ...any) time.Time {
	for _, v := range vals {
		if t := parseTimeString(textAny(v)); !t.IsZero() {
			return t
		}
	}
	return time.Now().UTC()
}

func parseTimeString(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"02/01/2006 3:04:05 PM", "02/01/2006 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(tagRE.ReplaceAllString(s, " "))
}

var tagRE = regexp.MustCompile(`<[^>]+>`)

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
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

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
