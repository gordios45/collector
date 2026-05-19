// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package public_safety ingests no-key municipal fire/police/CAD feeds.
//
// This collector deliberately starts with official direct JSON/Socrata endpoints
// that expose coordinates without decrypting web-map payloads or relying on
// social APIs.
package public_safety

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "public_safety_incidents"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchSanDiegoFire(ctx)...)
	out = append(out, c.fetchRSS(ctx, rssFeed{
		Provider:  "ACT Emergency Services",
		URL:       "http://www.esa.act.gov.au/feeds/currentincidents.xml",
		PublicURL: "https://esa.act.gov.au/feeds/currentincidents.xml",
	})...)
	out = append(out, c.fetchSocrata(ctx, socrataFeed{
		Provider: "Baton Rouge Fire Department",
		URL:      "https://data.brla.gov/resource/dakq-4sda.json",
		Params: map[string]string{
			"$limit": "500",
			"$order": "disp_date DESC, disp_time DESC",
		},
		PublicURL: "https://data.brla.gov/Public-Safety/Baton-Rouge-Fire-Incidents/dakq-4sda/data",
		KindKey:   "inci_descript",
		TimeKeys:  []string{"disp_date", "disp_time"},
		IDKey:     "inci_no",
	})...)
	out = append(out, c.fetchSocrata(ctx, socrataFeed{
		Provider:  "Cincinnati Police Department",
		URL:       "https://data.cincinnati-oh.gov/resource/gexm-h6bt.json",
		Params:    map[string]string{"$limit": "500", "$order": "create_time_incident DESC"},
		PublicURL: "https://data.cincinnati-oh.gov/Safety/PDI-Police-Data-Initiative-Police-Calls-for-Servic/gexm-h6bt/data",
		KindKey:   "incident_type_desc",
		TimeKeys:  []string{"create_time_incident"},
		IDKey:     "event_number",
		LatKey:    "latitude_x",
		LonKey:    "longitude_x",
	})...)
	out = append(out, c.fetchSocrata(ctx, socrataFeed{
		Provider:  "Montgomery County Police Department",
		URL:       "https://data.montgomerycountymd.gov/resource/98cc-bc7d.json",
		Params:    map[string]string{"$limit": "500", "$order": "start_time DESC"},
		PublicURL: "https://data.montgomerycountymd.gov/resource/98cc-bc7d",
		KindKey:   "initial_type",
		TimeKeys:  []string{"start_time"},
		IDKey:     "incident_id",
		LatKey:    "latitude",
		LonKey:    "longitude",
	})...)
	out = append(out, c.fetchSocrata(ctx, socrataFeed{
		Provider:  "San Francisco Fire Department",
		URL:       "https://data.sfgov.org/resource/nuek-vuh3.json",
		Params:    map[string]string{"$limit": "500", "$order": "received_dttm DESC"},
		PublicURL: "https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3/data",
		KindKey:   "call_type",
		TimeKeys:  []string{"received_dttm"},
		IDKey:     "incident_number",
	})...)
	out = append(out, c.fetchTallahassee(ctx)...)
	out = append(out, c.fetchRSS(ctx, rssFeed{
		Provider:  "Monroe County NY 911",
		URL:       "https://www.monroecounty.gov/911/rss.php",
		PublicURL: "https://www.monroecounty.gov/safety",
	})...)
	out = append(out, c.fetchRSS(ctx, rssFeed{
		Provider:        "Los Angeles Fire Department",
		URL:             "https://www.lafd.org/alerts-rss.xml",
		PublicURL:       "https://www.lafd.org/alerts",
		ResolveMapLinks: true,
	})...)
	out = append(out, c.fetchChandler(ctx)...)
	return dedupe(out), nil
}

func (c *Collector) fetchSanDiegoFire(ctx context.Context) []events.Event {
	const endpoint = "https://webapps.sandiego.gov/SDFireDispatch/api/v1/Incidents"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, lon, ok := pointFromMap(row)
		if !ok {
			continue
		}
		callType := textAny(row["CallType"])
		if isLowSignalPublicSafety(callType) {
			continue
		}
		id := firstNonEmpty(textAny(row["MasterIncidentNumber"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["ResponseDate"], row["DispatchDate"])
		labels := incidentLabels(callType)
		props := baseProps("San Diego Fire Dispatch", endpoint, "https://webapps.sandiego.gov/sdfiredispatch/", id, callType, labels)
		copyProps(props, row)
		props["address"] = strings.TrimSpace(strings.Join(nonEmpty(textAny(row["Address"]), textAny(row["CrossStreet"]), textAny(row["City"])), ", "))
		props["incident_score"] = incidentScore(callType, labels)
		out = append(out, event(ts, "sandiego_fire:"+id, lat, lon, props))
	}
	return out
}

type socrataFeed struct {
	Provider  string
	URL       string
	Params    map[string]string
	PublicURL string
	KindKey   string
	TimeKeys  []string
	IDKey     string
	LatKey    string
	LonKey    string
}

func (c *Collector) fetchSocrata(ctx context.Context, feed socrataFeed) []events.Event {
	endpoint := withParams(feed.URL, feed.Params)
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, lon, ok := pointFromSocrata(row, feed.LatKey, feed.LonKey)
		if !ok {
			continue
		}
		kind := textAny(row[feed.KindKey])
		if isLowSignalPublicSafety(kind) {
			continue
		}
		id := firstNonEmpty(textAny(row[feed.IDKey]), stableID(feed.Provider+fmt.Sprint(row)))
		ts := socrataTime(row, feed.TimeKeys)
		labels := incidentLabels(kind)
		props := baseProps(feed.Provider, endpoint, feed.PublicURL, id, kind, labels)
		copyProps(props, row)
		props["incident_score"] = incidentScore(kind, labels)
		props["address"] = addressFromRow(row)
		out = append(out, event(ts, stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchChandler(ctx context.Context) []events.Event {
	const endpoint = "https://data.chandlerpd.com/wp-json/open-data/v1/calls-for-service-2020?orderby=call_received_date_time&order=desc"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["call_latitude"])
		lon, lonOK := floatAny(row["call_longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		kind := textAny(row["call_reported_as"])
		if isLowSignalPublicSafety(kind) {
			continue
		}
		id := firstNonEmpty(textAny(row["id"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["call_received_date_time"])
		labels := incidentLabels(kind)
		props := baseProps("Chandler Police Department", endpoint, endpoint, id, kind, labels)
		copyProps(props, row)
		props["address"] = strings.TrimSpace(strings.Join(nonEmpty(textAny(row["call_place_name"]), textAny(row["call_address"]), textAny(row["call_city"]), "AZ"), ", "))
		props["incident_score"] = incidentScore(kind, labels)
		out = append(out, event(ts, "chandler:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchTallahassee(ctx context.Context) []events.Event {
	const endpoint = "http://talgov.com/gis/handler/topsactiveincidents.ashx"
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &resp); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(resp.Data))
	for _, row := range resp.Data {
		lon, lonOK := floatAny(row["x"])
		lat, latOK := floatAny(row["y"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		kind := textAny(row["eventdesc"])
		if isLowSignalPublicSafety(kind) {
			continue
		}
		id := firstNonEmpty(textAny(row["eventinc"]), textAny(row["eventnum"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["eventdate"])
		labels := incidentLabels(kind)
		props := baseProps("Tallahassee Police Department", endpoint, "https://www.talgov.com/gis/tops/", id, kind, labels)
		copyProps(props, row)
		props["address"] = textAny(row["eventaddress"])
		props["description"] = textAny(row["eventheadline"])
		props["incident_score"] = incidentScore(kind, labels)
		out = append(out, event(ts, "tallahassee:"+id, lat, lon, props))
	}
	return out
}

type rssFeed struct {
	Provider        string
	URL             string
	PublicURL       string
	ResolveMapLinks bool
}

type rssDocument struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	GUID        string   `xml:"guid"`
	Description string   `xml:"description"`
	PubDate     string   `xml:"pubDate"`
	Categories  []string `xml:"category"`
	GeoLat      string   `xml:"lat"`
	GeoLong     string   `xml:"long"`
	GeoRSSPoint string   `xml:"http://www.georss.org/georss point"`
	Point       string   `xml:"point"`
}

func (c *Collector) fetchRSS(ctx context.Context, feed rssFeed) []events.Event {
	buf, err := httpx.GetBytes(ctx, feed.URL, map[string]string{"Accept": "application/rss+xml, application/xml, text/xml"})
	if err != nil {
		return nil
	}
	var doc rssDocument
	if err := xml.Unmarshal(buf, &doc); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(doc.Channel.Items))
	for _, item := range doc.Channel.Items {
		kind, address, desc := rssIncidentDetails(item)
		if isLowSignalPublicSafety(kind) {
			continue
		}
		lat, latOK := floatAny(item.GeoLat)
		lon, lonOK := floatAny(item.GeoLong)
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			lat, lon, latOK = pointFromSpacePair(firstNonEmpty(item.GeoRSSPoint, item.Point))
			lonOK = latOK
		}
		if (!latOK || !lonOK || !validLatLon(lat, lon)) && feed.ResolveMapLinks {
			lat, lon, latOK = resolveMapLinkPoint(ctx, desc)
			lonOK = latOK
		}
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(rssID(item), stableID(feed.Provider+item.Title+item.PubDate))
		ts := parseTimeString(cleanRSSString(item.PubDate))
		labels := incidentLabels(kind)
		publicURL := firstNonEmpty(absoluteFeedURL(feed.PublicURL, item.Link), feed.PublicURL)
		props := baseProps(feed.Provider, feed.URL, publicURL, id, kind, labels)
		props["address"] = address
		props["description"] = desc
		props["rss_guid"] = cleanRSSString(item.GUID)
		props["incident_score"] = incidentScore(kind, labels)
		out = append(out, event(ts, stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

func baseProps(provider, endpoint, publicURL, id, kind string, labels []string) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"source_public_url":    publicURL,
		"incident_id":          id,
		"incident_type":        kind,
		"labels":               labels,
		"title":                titleFor(kind, labels),
		"source_provider_kind": "official_local_public_safety_feed",
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	addPublicSafetySubtypeScores(props)
	props["source_payload_validity"] = map[string]any{
		"valid_start":    ts.Format(time.RFC3339),
		"valid_end":      ts.Add(4 * time.Hour).Format(time.RFC3339),
		"validity_basis": "public_safety_dispatch_time",
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func incidentLabels(kind string) []string {
	l := strings.ToLower(kind)
	out := []string{"public_safety_incident"}
	switch {
	case containsAny(l, "structure", "building fire", "commercial fire", "residential fire", "highrise"):
		out = append(out, "structure_fire")
	case containsAny(l, "fire", "burn", "vehicle fire", "rubbish"):
		out = append(out, "fire")
	}
	if containsAny(l, "gas leak", "natural gas", "carbon monoxide", "co alarm") {
		out = append(out, "gas_leak")
	}
	if containsAny(l, "chemical", "hazmat", "spill") {
		out = append(out, "chemical_spill", "hazmat")
	}
	if containsAny(l, "accident", "crash", "collision", "vehicle vs", "mva", "mvc", "extrication") {
		out = append(out, "road_accident")
	}
	if containsAny(l, "shots", "shooting", "gun") {
		out = append(out, "shooting")
	}
	if containsAny(l, "assault", "fight", "battery", "stabbing") {
		out = append(out, "assault")
	}
	if containsAny(l, "power line down", "wires down") {
		out = append(out, "power_outage")
	}
	return unique(out)
}

func incidentScore(kind string, labels []string) float64 {
	score := 0.6
	for _, l := range labels {
		switch l {
		case "structure_fire", "chemical_spill", "hazmat", "gas_leak":
			score = math.Max(score, 2.2)
		case "road_accident", "shooting", "assault", "fire":
			score = math.Max(score, 1.4)
		case "power_outage":
			score = math.Max(score, 1.0)
		}
	}
	text := strings.ToLower(kind)
	if containsAny(text, "highrise", "hospital", "extrication", "injuries", "major") {
		score += 0.4
	}
	return propx.ClampFloat(score, 0, 3)
}

func addPublicSafetySubtypeScores(props map[string]any) {
	score, _ := floatAny(props["incident_score"])
	labels := labelSet(props["labels"])
	text := strings.ToLower(strings.Join([]string{
		textAny(props["title"]),
		textAny(props["description"]),
		textAny(props["incident_type"]),
		textAny(props["address"]),
	}, " "))
	if labels["structure_fire"] {
		props["structure_fire_report_score"] = propx.ClampFloat(math.Max(score, 1.5), 0, 3)
	}
	if labels["wildfire"] || labels["brush_fire"] || labels["vegetation_fire"] ||
		containsAny(text, "brush fire", "vegetation fire", "wildland fire", "wildfire", "forest fire", "grass fire") {
		props["vegetation_fire_report_score"] = propx.ClampFloat(math.Max(score, 1.2), 0, 3)
	}
	if labels["hazmat"] || labels["chemical_spill"] {
		props["hazmat_report_score"] = propx.ClampFloat(math.Max(score, 1.4), 0, 3)
	}
	if labels["road_accident"] {
		props["road_accident_report_score"] = propx.ClampFloat(math.Max(score, 0.8), 0, 3)
	}
	if labels["gas_leak"] {
		props["gas_leak_report_score"] = propx.ClampFloat(math.Max(score, 1.2), 0, 3)
	}
	if labels["power_outage"] || containsAny(text, "wire down", "wires down", "power line") {
		props["power_line_report_score"] = propx.ClampFloat(math.Max(score, 0.8), 0, 3)
	}
	if labels["shooting"] || labels["assault"] {
		props["violence_report_score"] = propx.ClampFloat(math.Max(score, 0.8), 0, 3)
	}
}

func labelSet(v any) map[string]bool {
	out := map[string]bool{}
	switch labels := v.(type) {
	case []string:
		for _, label := range labels {
			out[strings.ToLower(strings.TrimSpace(label))] = true
		}
	case []any:
		for _, item := range labels {
			out[strings.ToLower(strings.TrimSpace(fmt.Sprint(item)))] = true
		}
	case string:
		for _, label := range strings.Split(labels, ",") {
			out[strings.ToLower(strings.Trim(strings.TrimSpace(label), "[]\"'"))] = true
		}
	}
	delete(out, "")
	return out
}

func isLowSignalPublicSafety(kind string) bool {
	l := strings.ToLower(strings.TrimSpace(kind))
	if l == "" {
		return true
	}
	for _, block := range []string{
		"medical", "medical alert", "move up", "public assist", "welfare check",
		"traffic stop", "found property", "fraud", "hot spot", "alarm",
	} {
		if strings.Contains(l, block) {
			return true
		}
	}
	return false
}

func pointFromSocrata(row map[string]any, latKey, lonKey string) (float64, float64, bool) {
	if latKey != "" && lonKey != "" {
		lat, latOK := floatAny(row[latKey])
		lon, lonOK := floatAny(row[lonKey])
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	return pointFromMap(row)
}

func pointFromMap(row map[string]any) (float64, float64, bool) {
	for _, ks := range [][2]string{
		{"lat", "lon"}, {"lat", "lng"}, {"latitude", "longitude"}, {"Latitude", "Longitude"},
		{"call_latitude", "call_longitude"}, {"latitude_x", "longitude_x"},
		{"ycoord", "xcoord"},
	} {
		lat, latOK := floatAny(row[ks[0]])
		lon, lonOK := floatAny(row[ks[1]])
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	if coords, ok := row["coordinates"].([]any); ok && len(coords) >= 2 {
		lon, lonOK := floatAny(coords[0])
		lat, latOK := floatAny(coords[1])
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	if geo, ok := row["geolocation"].(map[string]any); ok {
		return pointFromMap(geo)
	}
	if loc, ok := row["location"].(map[string]any); ok {
		return pointFromMap(loc)
	}
	if loc, ok := row["case_location"].(map[string]any); ok {
		return pointFromMap(loc)
	}
	return 0, 0, false
}

func socrataTime(row map[string]any, keys []string) time.Time {
	if len(keys) == 2 {
		date := strings.TrimSpace(textAny(row[keys[0]]))
		tod := strings.TrimSpace(textAny(row[keys[1]]))
		if date != "" && tod != "" {
			if t := parseTimeString(strings.Split(date, "T")[0] + "T" + tod); !t.IsZero() {
				return t
			}
		}
	}
	vals := make([]any, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, row[k])
	}
	return parseTimeAny(vals...)
}

func addressFromRow(row map[string]any) string {
	if geo, ok := row["geolocation"].(map[string]any); ok {
		if raw := textAny(geo["human_address"]); raw != "" {
			var addr map[string]any
			if err := json.Unmarshal([]byte(raw), &addr); err == nil {
				return strings.TrimSpace(strings.Join(nonEmpty(textAny(addr["address"]), textAny(addr["city"]), textAny(addr["state"]), textAny(addr["zip"])), ", "))
			}
		}
	}
	for _, key := range []string{"address_x", "address", "location", "formattedstreet"} {
		if v := textAny(row[key]); v != "" {
			return v
		}
	}
	return ""
}

func titleFor(kind string, labels []string) string {
	label := kind
	if len(labels) > 1 {
		label = strings.ReplaceAll(labels[1], "_", " ")
	}
	return "Public safety report: " + strings.TrimSpace(label)
}

func withParams(base string, params map[string]string) string {
	if len(params) == 0 {
		return base
	}
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String()
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
		time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05",
		"01/02/2006 15:04:05", "01/02/2006 03:04:05 PM", "2006-01-02",
		"2006-01-02 15:04 MST", "2006-01-02 15:04:05 MST",
		time.RFC1123Z, time.RFC1123, "Mon, 2 Jan 2006 15:04:05 MST", "Mon, 2 Jan 2006 15:04:05 -0700",
		"Monday, January 2, 2006 - 15:04", "Monday, Jan 2, 2006 - 15:04",
		"Jan 2 2006 3:04PM", "Jan  2 2006  3:04PM", "Jan 2 2006  3:04PM",
		"1/2/2006 15:04", "1/02/2006 15:04", "01/02/2006 15:04",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
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

func rssIncidentDetails(item rssItem) (string, string, string) {
	title := cleanRSSString(item.Title)
	desc := cleanRSSString(item.Description)
	kind := title
	address := ""
	if before, after, ok := strings.Cut(title, " at "); ok {
		kind = before
		address = after
	}
	if len(item.Categories) > 0 {
		cat := cleanRSSString(item.Categories[0])
		if cat != "" {
			kind = cat
		}
	}
	if strings.Contains(desc, ";") && strings.Contains(desc, "INC#") {
		parts := splitClean(desc, ";")
		if len(parts) > 0 && parts[0] != "" {
			kind = parts[0]
		}
		if len(parts) > 3 && parts[3] != "" {
			address = parts[3]
		}
	}
	return kind, address, desc
}

func rssID(item rssItem) string {
	for _, s := range []string{item.Description, item.Title, item.GUID, item.Link} {
		clean := cleanRSSString(s)
		for _, re := range []*regexp.Regexp{
			regexp.MustCompile(`(?i)\bID:\s*([A-Z0-9-]+)`),
			regexp.MustCompile(`(?i)\bINC#\s*([A-Z0-9-]+)`),
			regexp.MustCompile(`(?i)\bIncident:\s*([A-Z0-9-]+)`),
		} {
			if m := re.FindStringSubmatch(clean); len(m) > 1 {
				return m[1]
			}
		}
	}
	return firstNonEmpty(cleanRSSString(item.GUID), cleanRSSString(item.Link))
}

func cleanRSSString(s string) string {
	s = html.UnescapeString(s)
	s = regexp.MustCompile(`(?s)<[^>]+>`).ReplaceAllString(s, " ")
	s = strings.ReplaceAll(s, "\u00a0", " ")
	return strings.Join(strings.Fields(s), " ")
}

func splitClean(s, sep string) []string {
	raw := strings.Split(s, sep)
	out := make([]string, 0, len(raw))
	for _, part := range raw {
		out = append(out, strings.TrimSpace(part))
	}
	return out
}

func absoluteFeedURL(base, href string) string {
	href = cleanRSSString(href)
	if href == "" || href == "view" {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	if u.IsAbs() {
		return u.String()
	}
	b, err := url.Parse(base)
	if err != nil {
		return ""
	}
	return b.ResolveReference(u).String()
}

func resolveMapLinkPoint(ctx context.Context, text string) (float64, float64, bool) {
	mapURL := regexp.MustCompile(`https://bit\.ly/[A-Za-z0-9]+`).FindString(html.UnescapeString(text))
	if mapURL == "" {
		return 0, 0, false
	}
	client := &http.Client{
		Timeout: 8 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, mapURL, nil)
	if err != nil {
		return 0, 0, false
	}
	req.Header.Set("User-Agent", "gordios/0.1")
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, false
	}
	defer resp.Body.Close()
	return pointFromURL(resp.Header.Get("Location"))
}

func pointFromURL(raw string) (float64, float64, bool) {
	u, err := url.Parse(raw)
	if err == nil {
		if q := u.Query().Get("query"); q != "" {
			if lat, lon, ok := pointFromPair(q); ok {
				return lat, lon, true
			}
		}
	}
	return pointFromPair(raw)
}

func pointFromPair(s string) (float64, float64, bool) {
	m := regexp.MustCompile(`([+-]?\d+(?:\.\d+)?),\s*([+-]?\d+(?:\.\d+)?)`).FindStringSubmatch(s)
	if len(m) < 3 {
		return 0, 0, false
	}
	lat, latOK := floatAny(m[1])
	lon, lonOK := floatAny(m[2])
	if !latOK || !lonOK || !validLatLon(lat, lon) {
		return 0, 0, false
	}
	return lat, lon, true
}

func pointFromSpacePair(s string) (float64, float64, bool) {
	parts := strings.Fields(strings.TrimSpace(strings.ReplaceAll(s, ",", " ")))
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, latOK := floatAny(parts[0])
	lon, lonOK := floatAny(parts[1])
	if !latOK || !lonOK || !validLatLon(lat, lon) {
		return 0, 0, false
	}
	return lat, lon, true
}
