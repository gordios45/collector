// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package road_incidents ingests no-key public state DOT/511 incident feeds.
//
// These are official road-event reports: crashes, closures, debris, winter
// hazards, and planned disruptions. The collector intentionally avoids
// registration-key 511 APIs and only uses public JSON/RSS/ArcGIS/GeoJSON
// endpoints.
package road_incidents

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"math"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "road_incidents"

type Collector struct {
	mu             sync.Mutex
	indotCached    []events.Event
	indotAt        time.Time
	autobahnCached []events.Event
	autobahnAt     time.Time
}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchNCDOT(ctx)...)
	out = append(out, c.fetchDelaware(ctx)...)
	out = append(out, c.fetchArcGIS(ctx, arcGISFeed{
		Provider:  "Iowa DOT 511",
		URL:       "https://services.arcgis.com/8lRhdTsQyJpO52F1/arcgis/rest/services/511_IA_Road_Conditions_View/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://511ia.org/",
	})...)
	out = append(out, c.fetchGeoJSON(ctx, geoJSONFeed{
		Provider:  "Arkansas DOT iDrive",
		URL:       "https://layers.idrivearkansas.com/closures_points.geojson",
		PublicURL: "https://idrivearkansas.com/",
	})...)
	out = append(out, c.fetchQLDTraffic(ctx)...)
	out = append(out, c.fetchOregonTripCheck(ctx)...)
	out = append(out, c.fetchMassDOT(ctx)...)
	out = append(out, c.fetchINDOT(ctx)...)
	out = append(out, c.fetchAutobahn(ctx)...)
	out = append(out, c.fetchDigitraffic(ctx)...)
	out = append(out, c.fetchNDW(ctx)...)
	out = append(out, c.fetchBisonFute(ctx)...)
	for _, f := range rssFeeds {
		out = append(out, c.fetchRSS(ctx, f)...)
	}
	return dedupe(out), nil
}

type rssFeed struct {
	Provider  string
	URL       string
	PublicURL string
	State     string
	Country   string
	Lat       float64
	Lon       float64
}

var rssFeeds = []rssFeed{
	{Provider: "Maryland DOT CHART", URL: "https://chart.maryland.gov/rss/ProduceRSS.aspx?Type=TIandRC&filter=TI", PublicURL: "https://chart.maryland.gov/incidents/index.php", State: "MD", Lat: 39.05, Lon: -76.64},
	{Provider: "New Jersey 511", URL: "http://511nj.org/RSS511Service/RSS511Service.svc/rest/rss/RSSActiveIncidents", PublicURL: "https://511nj.org/", State: "NJ", Lat: 40.14, Lon: -74.73},
	{Provider: "Virginia 511", URL: "http://www.511virginia.org/mobile/data/RSS/StatewideIncident.xml", PublicURL: "https://www.511virginia.org/", State: "VA", Lat: 37.52, Lon: -78.85},
	{Provider: "WSDOT Highway Alerts", URL: "http://www.wsdot.com/traffic/api/HighwayAlerts/rss.aspx", PublicURL: "https://www.wsdot.com/traffic/trafficalerts/default.aspx", State: "WA", Lat: 47.40, Lon: -120.70},
	{Provider: "National Highways England unplanned events", URL: "https://m.highwaysengland.co.uk/feeds/rss/UnplannedEvents.xml", PublicURL: "https://m.highwaysengland.co.uk/", State: "England", Country: "United Kingdom", Lat: 52.35, Lon: -1.50},
	{Provider: "National Highways England current and future events", URL: "https://m.highwaysengland.co.uk/feeds/rss/CurrentAndFutureEvents.xml", PublicURL: "https://m.highwaysengland.co.uk/", State: "England", Country: "United Kingdom", Lat: 52.35, Lon: -1.50},
}

type arcGISFeed struct {
	Provider  string
	URL       string
	PublicURL string
}

type geoJSONFeed struct {
	Provider  string
	URL       string
	PublicURL string
}

func (c *Collector) fetchNCDOT(ctx context.Context) []events.Event {
	const endpoint = "https://tims.ncdot.gov/tims/api/incidents/"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["Latitude"])
		lon, lonOK := floatAny(row["Longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		incidentType := textAny(row["IncidentType"])
		condition := textAny(row["Condition"])
		title := strings.TrimSpace(strings.Join(nonEmpty(incidentType, condition, textAny(row["Road"]), textAny(row["Location"])), " - "))
		ts := parseTimeAny(row["LastUpdate"], row["Start"])
		id := firstNonEmpty(textAny(row["Id"]), stableID(title+fmt.Sprint(lat, lon)))
		props := baseProps("NCDOT TIMS", endpoint, "https://tims.ncdot.gov/tims/", title, textAny(row["Reason"]), incidentType)
		copyProps(props, row)
		props["road"] = textAny(row["Road"])
		props["location"] = textAny(row["Location"])
		props["county"] = textAny(row["CountyName"])
		props["city"] = textAny(row["City"])
		props["condition"] = condition
		props["state"] = "NC"
		props["labels"] = roadLabels(incidentType + " " + condition)
		props["road_disruption_score"] = roadScore(incidentType, condition, textAny(row["Reason"]))
		props["source_payload_validity"] = validity(ts, parseTimeAny(row["End"]))
		out = append(out, event(ts, "ncdot:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchDelaware(ctx context.Context) []events.Event {
	feeds := []struct {
		provider string
		url      string
		path     string
	}{
		{"Delaware DOT advisories", "https://tmc.deldot.gov/json/advisory.json", "advisories"},
		{"Delaware DOT Waze alerts", "https://tmc.deldot.gov/json/wazealert.json", "alerts"},
	}
	var out []events.Event
	for _, f := range feeds {
		var raw map[string]any
		if err := httpx.GetJSON(ctx, f.url, map[string]string{"Accept": "application/json"}, &raw); err != nil {
			continue
		}
		for _, row := range mapsAt(raw, f.path) {
			lat, lon, ok := pointFromMap(row)
			if !ok {
				continue
			}
			title := firstNonEmpty(textAny(row["title"]), textAny(row["headline"]), textAny(row["event"]), textAny(row["type"]))
			body := firstNonEmpty(textAny(row["description"]), textAny(row["message"]), textAny(row["street"]))
			typ := firstNonEmpty(textAny(row["type"]), textAny(row["subtype"]), title)
			ts := parseTimeAny(row["updated"], row["last_update"], row["start_time"], row["pubDate"])
			id := firstNonEmpty(textAny(row["id"]), textAny(row["uuid"]), stableID(f.provider+title+fmt.Sprint(lat, lon)))
			props := baseProps(f.provider, f.url, "http://deldot.gov/Traffic/travel_advisory/", title, body, typ)
			copyProps(props, row)
			props["state"] = "DE"
			props["labels"] = roadLabels(title + " " + body)
			props["road_disruption_score"] = roadScore(title, typ, body)
			out = append(out, event(ts, "deldot:"+id, lat, lon, props))
		}
	}
	return out
}

type massDOTEvents struct {
	Events []massDOTEvent `xml:"Events>Event"`
}

type massDOTEvent struct {
	EventID                 string `xml:"EventId"`
	EventCreatedDate        string `xml:"EventCreatedDate"`
	EventStartDate          string `xml:"EventStartDate"`
	EventEndDate            string `xml:"EventEndDate"`
	LastUpdate              string `xml:"LastUpdate"`
	EventStatus             string `xml:"EventStatus"`
	EventCategory           string `xml:"EventCategory"`
	EventType               string `xml:"EventType"`
	EventSubType            string `xml:"EventSubType"`
	RoadwayName             string `xml:"RoadwayName"`
	Direction               string `xml:"Direction"`
	PrimaryLatitude         string `xml:"PrimaryLatitude"`
	PrimaryLongitude        string `xml:"PrimaryLongitude"`
	LocationDescription     string `xml:"LocationDescription"`
	LaneBlockageDescription string `xml:"LaneBlockageDescription"`
	RecurrenceDescription   string `xml:"RecurrenceDescription"`
}

func (c *Collector) fetchMassDOT(ctx context.Context) []events.Event {
	const endpoint = "http://events.massdot.evbg.net/"
	buf, err := httpx.GetBytes(ctx, endpoint, map[string]string{"Accept": "application/xml,text/xml,*/*"})
	if err != nil {
		return nil
	}
	var raw massDOTEvents
	if err := xml.Unmarshal(buf, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Events))
	for _, row := range raw.Events {
		lat, latOK := floatAny(row.PrimaryLatitude)
		lon, lonOK := floatAny(row.PrimaryLongitude)
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		typ := firstNonEmpty(row.EventSubType, row.EventType, row.EventCategory, "road incident")
		title := strings.TrimSpace(strings.Join(nonEmpty(typ, row.RoadwayName, row.Direction), " - "))
		body := strings.TrimSpace(strings.Join(nonEmpty(row.LocationDescription, row.LaneBlockageDescription, row.RecurrenceDescription), " "))
		ts := parseTimeString(firstNonEmpty(row.LastUpdate, row.EventCreatedDate, row.EventStartDate))
		end := parseTimeString(row.EventEndDate)
		id := firstNonEmpty(row.EventID, stableID(title+fmt.Sprint(lat, lon)))
		props := baseProps("MassDOT 511", endpoint, "https://mass511.com/", title, body, typ)
		props["road"] = row.RoadwayName
		props["direction"] = row.Direction
		props["location"] = row.LocationDescription
		props["state"] = "MA"
		props["status"] = row.EventStatus
		props["labels"] = roadLabels(title + " " + body + " " + typ)
		props["road_disruption_score"] = roadScore(title, typ, body)
		props["source_payload_validity"] = validity(ts, end)
		out = append(out, event(ts, "massdot:"+id, lat, lon, props))
	}
	return out
}

type indotUpdate struct {
	Header struct {
		MessageTime indotTimestamp `xml:"message-time-stamp"`
		Expiry      indotTimestamp `xml:"message-expiry-time"`
	} `xml:"message-header"`
	EventRef struct {
		ID string `xml:"event-id"`
	} `xml:"event-reference"`
	Details struct {
		Detail struct {
			Descriptions struct {
				Descriptions []indotDescription `xml:"description"`
			} `xml:"descriptions"`
			Locations struct {
				Locations []indotLocation `xml:"location"`
			} `xml:"locations"`
		} `xml:"detail"`
	} `xml:"details"`
}

type indotTimestamp struct {
	Date      string `xml:"date"`
	Time      string `xml:"time"`
	UTCOffset string `xml:"utc-offset"`
}

type indotDescription struct {
	Text  string `xml:",chardata"`
	Inner string `xml:",innerxml"`
}

type indotLocation struct {
	OnLink struct {
		Primary struct {
			Geo indotGeo `xml:"geo-location"`
		} `xml:"primary-location"`
	} `xml:"location-on-link"`
}

type indotGeo struct {
	Lat string `xml:"latitude"`
	Lon string `xml:"longitude"`
}

func (c *Collector) fetchINDOT(ctx context.Context) []events.Event {
	c.mu.Lock()
	if time.Since(c.indotAt) < 30*time.Minute && len(c.indotCached) > 0 {
		out := append([]events.Event(nil), c.indotCached...)
		c.mu.Unlock()
		return out
	}
	c.mu.Unlock()

	const endpoint = "https://inhub.carsprogram.org/data/feu-g.xml"
	buf, err := getBytesWithTimeout(ctx, endpoint, "application/xml,text/xml,*/*", 75*time.Second)
	if err != nil {
		return nil
	}
	dec := xml.NewDecoder(bytes.NewReader(buf))
	out := []events.Event{}
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "full-event-update" {
			continue
		}
		var row indotUpdate
		if err := dec.DecodeElement(&row, &start); err != nil {
			continue
		}
		if e, ok := indotEvent(endpoint, row); ok {
			out = append(out, e)
		}
	}
	out = dedupe(out)
	c.mu.Lock()
	c.indotCached = append([]events.Event(nil), out...)
	c.indotAt = time.Now()
	c.mu.Unlock()
	return out
}

func indotEvent(endpoint string, row indotUpdate) (events.Event, bool) {
	lat, lon, ok := indotPoint(row)
	if !ok {
		return events.Event{}, false
	}
	desc := indotDescriptionText(row.Details.Detail.Descriptions.Descriptions)
	if desc == "" {
		return events.Event{}, false
	}
	ts := parseINDOTTime(row.Header.MessageTime)
	end := parseINDOTTime(row.Header.Expiry)
	id := firstNonEmpty(row.EventRef.ID, stableID(desc+fmt.Sprint(lat, lon)))
	typ := firstWord(desc)
	title := firstNonEmpty(shortRoadTitle(desc), typ, "Indiana road incident")
	props := baseProps("INDOT 511", endpoint, "https://511in.org/", title, desc, typ)
	props["state"] = "IN"
	props["labels"] = roadLabels(desc)
	props["road_disruption_score"] = roadScore(desc)
	props["source_payload_validity"] = validity(ts, end)
	return event(ts, "indot:"+id, lat, lon, props), true
}

func indotDescriptionText(descs []indotDescription) string {
	best := ""
	for _, d := range descs {
		clean := strip(firstNonEmpty(d.Text, d.Inner))
		if len(clean) > len(best) {
			best = clean
		}
	}
	return strings.TrimSpace(best)
}

func indotPoint(row indotUpdate) (float64, float64, bool) {
	for _, loc := range row.Details.Detail.Locations.Locations {
		lat, latOK := microDegree(loc.OnLink.Primary.Geo.Lat)
		lon, lonOK := microDegree(loc.OnLink.Primary.Geo.Lon)
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	return 0, 0, false
}

func microDegree(s string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return v / 1_000_000, true
}

func parseINDOTTime(ts indotTimestamp) time.Time {
	return parseTimeString(strings.TrimSpace(ts.Date + " " + ts.Time + " " + ts.UTCOffset))
}

func shortRoadTitle(desc string) string {
	desc = strings.TrimSpace(strings.TrimPrefix(desc, "Audio Text:"))
	desc = strings.Join(strings.Fields(desc), " ")
	if i := strings.Index(desc, "."); i > 0 && i < 160 {
		return desc[:i]
	}
	if len(desc) > 160 {
		return desc[:160]
	}
	return desc
}

func getBytesWithTimeout(ctx context.Context, rawURL, accept string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "gordios/0.1")
	req.Header.Set("Accept", accept)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s -> %d", rawURL, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *Collector) fetchArcGIS(ctx context.Context, feed arcGISFeed) []events.Event {
	var raw struct {
		Features []struct {
			Attributes map[string]any `json:"attributes"`
			Geometry   map[string]any `json:"geometry"`
		} `json:"features"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := httpx.GetJSON(ctx, feed.URL, map[string]string{"Accept": "application/json"}, &raw); err != nil || raw.Error.Message != "" {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		lat, lon, ok := pointFromGeometry(feat.Geometry)
		if !ok {
			continue
		}
		row := feat.Attributes
		title := firstNonEmpty(textAny(row["title"]), textAny(row["EVENTTYPE"]), textAny(row["EventType"]), textAny(row["DESCRIPTION"]), textAny(row["Description"]), textAny(row["ROADNAME"]))
		body := firstNonEmpty(textAny(row["description"]), textAny(row["DESCRIPTION"]), textAny(row["COMMENTS"]), textAny(row["Comments"]), textAny(row["DETAILS"]))
		typ := firstNonEmpty(textAny(row["EVENTTYPE"]), textAny(row["EventType"]), textAny(row["TYPE"]), title)
		ts := parseTimeAny(row["LastUpdated"], row["lastUpdated"], row["StartTime"], row["STARTTIME"], row["created_date"])
		id := firstNonEmpty(textAny(row["OBJECTID"]), textAny(row["objectid"]), textAny(row["ID"]), stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, title, body, typ)
		copyProps(props, row)
		props["labels"] = roadLabels(title + " " + body + " " + typ)
		props["road_disruption_score"] = roadScore(title, typ, body)
		out = append(out, event(ts, "arcgis:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchGeoJSON(ctx context.Context, feed geoJSONFeed) []events.Event {
	var raw struct {
		Features []struct {
			ID         any            `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string          `json:"type"`
				Coordinates json.RawMessage `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, feed.URL, map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		lat, lon, ok := geoJSONCentroid(feat.Geometry.Type, feat.Geometry.Coordinates)
		if !ok {
			continue
		}
		row := feat.Properties
		title := firstNonEmpty(textAny(row["title"]), textAny(row["name"]), textAny(row["description"]), textAny(row["route"]))
		body := firstNonEmpty(textAny(row["description"]), textAny(row["details"]), textAny(row["comment"]))
		typ := firstNonEmpty(textAny(row["type"]), textAny(row["event_type"]), title)
		ts := parseTimeAny(row["updated"], row["start"], row["start_time"], row["last_update"])
		id := firstNonEmpty(textAny(feat.ID), textAny(row["id"]), stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, title, body, typ)
		copyProps(props, row)
		props["labels"] = roadLabels(title + " " + body + " " + typ)
		props["road_disruption_score"] = roadScore(title, typ, body)
		out = append(out, event(ts, "geojson:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchQLDTraffic(ctx context.Context) []events.Event {
	const endpoint = "https://data.qldtraffic.qld.gov.au/events_v2.geojson"
	var raw struct {
		Features []struct {
			ID         any            `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string          `json:"type"`
				Coordinates json.RawMessage `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		lat, lon, ok := geoJSONCentroid(feat.Geometry.Type, feat.Geometry.Coordinates)
		if !ok {
			continue
		}
		row := feat.Properties
		road, locality := qldRoadSummary(row)
		impact := qldImpactText(row)
		typ := firstNonEmpty(textAny(row["event_type"]), textAny(row["event_subtype"]), "road incident")
		title := strings.TrimSpace(strings.Join(nonEmpty(typ, textAny(row["event_subtype"]), road, locality), " - "))
		body := strings.TrimSpace(strings.Join(nonEmpty(textAny(row["description"]), textAny(row["advice"]), impact), " "))
		ts := parseTimeAny(row["last_updated"], nestedAny(row, "duration", "start"))
		id := firstNonEmpty(textAny(row["id"]), textAny(feat.ID), stableID(title+fmt.Sprint(lat, lon)))
		props := baseProps("Queensland Traffic", endpoint, "https://qldtraffic.qld.gov.au/", title, body, typ)
		copyProps(props, row)
		props["road"] = road
		props["location"] = locality
		props["state"] = "Queensland"
		props["country"] = "Australia"
		props["condition"] = impact
		props["labels"] = roadLabels(title + " " + body + " " + impact)
		props["road_disruption_score"] = roadScore(title, typ, body, impact)
		props["source_payload_validity"] = validity(ts, parseTimeAny(nestedAny(row, "duration", "end")))
		out = append(out, event(ts, "qld_traffic:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchOregonTripCheck(ctx context.Context) []events.Event {
	const endpoint = "https://www.tripcheck.com/Scripts/map/data/INCD.js?"
	var raw struct {
		Features []struct {
			Attributes map[string]any `json:"attributes"`
			Geometry   map[string]any `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json,text/javascript,*/*"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, feat := range raw.Features {
		row := feat.Attributes
		lat, lon, ok := pointFromOregon(row, feat.Geometry)
		if !ok {
			continue
		}
		title := firstNonEmpty(textAny(row["eventSubTypeName"]), textAny(row["eventTypeName"]), textAny(row["odotCategoryDescript"]))
		body := strings.TrimSpace(strings.Join(nonEmpty(textAny(row["comments"]), textAny(row["odotSeverityDescript"]), textAny(row["lanesAffected"])), " "))
		typ := firstNonEmpty(textAny(row["odotCategoryDescript"]), textAny(row["eventTypeName"]), title)
		ts := parseTimeAny(row["startTime"], row["lastUpdated"])
		id := firstNonEmpty(textAny(row["incidentId"]), stableID(fmt.Sprint(row)))
		props := baseProps("Oregon TripCheck", endpoint, "https://www.tripcheck.com/", title, body, typ)
		copyProps(props, row)
		props["road"] = textAny(row["route"])
		props["location"] = firstNonEmpty(textAny(row["beginMarker"]), textAny(row["locationName"]))
		props["state"] = "OR"
		props["condition"] = textAny(row["odotSeverityDescript"])
		props["labels"] = roadLabels(title + " " + body + " " + typ)
		props["road_disruption_score"] = roadScore(title, typ, body)
		out = append(out, event(ts, "oregon_tripcheck:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchAutobahn(ctx context.Context) []events.Event {
	c.mu.Lock()
	if time.Since(c.autobahnAt) < 10*time.Minute && len(c.autobahnCached) > 0 {
		out := append([]events.Event(nil), c.autobahnCached...)
		c.mu.Unlock()
		return out
	}
	c.mu.Unlock()

	const roadsURL = "https://verkehr.autobahn.de/o/autobahn/"
	var roads struct {
		Roads []string `json:"roads"`
	}
	if err := curlJSON(ctx, roadsURL, &roads); err != nil {
		return nil
	}
	services := []struct {
		Path string
		Key  string
		Type string
	}{
		{"warning", "warning", "warning"},
		{"closure", "closure", "closure"},
		{"roadworks", "roadworks", "roadworks"},
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	out := []events.Event{}
	for _, road := range roads.Roads {
		road = strings.TrimSpace(road)
		if road == "" {
			continue
		}
		for _, svc := range services {
			wg.Add(1)
			go func(road string, svc struct {
				Path string
				Key  string
				Type string
			}) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					return
				}
				rows := fetchAutobahnService(ctx, road, svc.Path, svc.Key, svc.Type)
				if len(rows) == 0 {
					return
				}
				mu.Lock()
				out = append(out, rows...)
				mu.Unlock()
			}(road, svc)
		}
	}
	wg.Wait()
	out = dedupe(out)
	c.mu.Lock()
	c.autobahnCached = append([]events.Event(nil), out...)
	c.autobahnAt = time.Now()
	c.mu.Unlock()
	return out
}

type autobahnItem struct {
	Identifier          string `json:"identifier"`
	Icon                string `json:"icon"`
	IsBlocked           string `json:"isBlocked"`
	Future              bool   `json:"future"`
	Extent              string `json:"extent"`
	Point               string `json:"point"`
	StartTimestamp      string `json:"startTimestamp"`
	DisplayType         string `json:"display_type"`
	Subtitle            string `json:"subtitle"`
	Title               string `json:"title"`
	AbnormalTrafficType string `json:"abnormalTrafficType"`
	AverageSpeed        string `json:"averageSpeed"`
	DelayTimeValue      string `json:"delayTimeValue"`
	Source              string `json:"source"`
	Coordinate          struct {
		Lat  float64 `json:"lat"`
		Long float64 `json:"long"`
	} `json:"coordinate"`
	Description []string `json:"description"`
	Geometry    struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	} `json:"geometry"`
}

func fetchAutobahnService(ctx context.Context, road, path, key, typ string) []events.Event {
	endpoint := fmt.Sprintf("https://verkehr.autobahn.de/o/autobahn/%s/services/%s", url.PathEscape(road), path)
	raw := map[string][]autobahnItem{}
	if err := curlJSON(ctx, endpoint, &raw); err != nil {
		return nil
	}
	rows := raw[key]
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, lon := row.Coordinate.Lat, row.Coordinate.Long
		if !validLatLon(lat, lon) {
			lat, lon, _ = parsePoint(strings.ReplaceAll(row.Point, ",", " "))
		}
		if !validLatLon(lat, lon) {
			lat, lon, _ = geoJSONCentroid(row.Geometry.Type, row.Geometry.Coordinates)
		}
		if !validLatLon(lat, lon) {
			continue
		}
		ts := parseTimeString(row.StartTimestamp)
		title := strings.TrimSpace(strings.Join(nonEmpty(road, row.Title, row.Subtitle), " - "))
		desc := strings.TrimSpace(strings.Join(row.Description, " "))
		incidentType := firstNonEmpty(row.AbnormalTrafficType, row.DisplayType, typ)
		props := baseProps("Autobahn GmbH public API", endpoint, "https://verkehr.autobahn.de/", title, desc, incidentType)
		props["country"] = "Germany"
		props["road"] = road
		props["status"] = row.DisplayType
		props["is_blocked"] = row.IsBlocked
		props["future"] = row.Future
		props["delay_minutes"] = row.DelayTimeValue
		props["average_speed_kmh"] = row.AverageSpeed
		props["provider_source"] = row.Source
		props["incident_id"] = row.Identifier
		props["labels"] = roadLabels(title + " " + incidentType + " " + desc)
		props["road_disruption_score"] = roadScore(title, typ, incidentType, desc, row.IsBlocked)
		props["source_payload_validity"] = validity(ts, ts.Add(12*time.Hour))
		out = append(out, event(ts, "autobahn:"+path+":"+firstNonEmpty(row.Identifier, stableID(title+fmt.Sprint(lat, lon))), lat, lon, props))
	}
	return out
}

func curlJSON(ctx context.Context, rawURL string, out any) error {
	buf, err := exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "20", "-A", "gordios/0.1", rawURL).Output()
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func (c *Collector) fetchDigitraffic(ctx context.Context) []events.Event {
	feeds := []struct {
		URL  string
		Type string
	}{
		{"https://tie.digitraffic.fi/api/traffic-message/v2/traffic-announcements", "traffic announcement"},
		{"https://tie.digitraffic.fi/api/traffic-message/v2/roadworks", "road work"},
	}
	var out []events.Event
	for _, feed := range feeds {
		var raw struct {
			Features []struct {
				Properties map[string]any `json:"properties"`
				Geometry   struct {
					Type        string          `json:"type"`
					Coordinates json.RawMessage `json:"coordinates"`
				} `json:"geometry"`
			} `json:"features"`
		}
		if err := curlJSONDigitraffic(ctx, feed.URL, &raw); err != nil {
			continue
		}
		for _, feat := range raw.Features {
			lat, lon, ok := geoJSONCentroid(feat.Geometry.Type, feat.Geometry.Coordinates)
			if !ok {
				continue
			}
			row := feat.Properties
			title, body, road, location := digitrafficSummary(row)
			typ := firstNonEmpty(textAny(row["situationType"]), feed.Type)
			ts := parseTimeAny(row["versionTime"], row["releaseTime"], nestedAny(row, "timeAndDuration", "startTime"))
			end := parseTimeAny(nestedAny(row, "timeAndDuration", "endTime"))
			id := firstNonEmpty(textAny(row["situationId"]), stableID(title+fmt.Sprint(lat, lon)))
			props := baseProps("Fintraffic Digitraffic road traffic messages", feed.URL, "https://liikennetilanne.fintraffic.fi/", title, body, typ)
			copyProps(props, row)
			props["country"] = "Finland"
			props["road"] = road
			props["location"] = location
			props["labels"] = roadLabels(title + " " + body + " " + typ)
			props["road_disruption_score"] = roadScore(title, typ, body)
			props["source_payload_validity"] = validity(ts, end)
			out = append(out, event(ts, "digitraffic:"+id, lat, lon, props))
		}
	}
	return out
}

func curlJSONDigitraffic(ctx context.Context, rawURL string, out any) error {
	buf, err := exec.CommandContext(ctx, "curl", "--compressed", "-fsS", "-L", "--max-time", "30", "-H", "Digitraffic-User: gordios/0.1 collector", "-H", "Accept: application/geo+json,application/json", rawURL).Output()
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func digitrafficSummary(row map[string]any) (title, body, road, location string) {
	anns, _ := row["announcements"].([]any)
	var ann map[string]any
	for _, item := range anns {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if ann == nil || strings.EqualFold(textAny(m["language"]), "en") {
			ann = m
		}
		if strings.EqualFold(textAny(m["language"]), "en") {
			break
		}
	}
	if ann != nil {
		title = textAny(ann["title"])
		location = textAny(nestedAny(ann, "location", "description"))
		body = strings.TrimSpace(strings.Join(nonEmpty(location, textAny(ann["additionalInformation"])), " "))
		road = firstNonEmpty(
			textAny(nestedAny(ann, "locationDetails", "roadAddressLocation", "primaryPoint", "roadName")),
			textAny(nestedAny(ann, "locationDetails", "roadAddressLocation", "primaryPoint", "roadAddress", "road")),
		)
	}
	if title == "" {
		title = firstNonEmpty(textAny(row["situationType"]), "Fintraffic road incident")
	}
	return title, body, road, location
}

func (c *Collector) fetchNDW(ctx context.Context) []events.Event {
	feeds := []struct {
		Provider string
		URL      string
	}{
		{"NDW safety-related traffic messages", "https://opendata.ndw.nu/veiligheidsgerelateerde_berichten_srti.xml.gz"},
		{"NDW temporary traffic measures closures", "https://opendata.ndw.nu/tijdelijke_verkeersmaatregelen_afsluitingen.xml.gz"},
	}
	var out []events.Event
	for _, f := range feeds {
		buf, err := curlBytes(ctx, f.URL)
		if err != nil {
			continue
		}
		out = append(out, parseDATEXEvents(f.Provider, f.URL, "https://www.ndw.nu/", "Netherlands", "ndw", buf)...)
	}
	return out
}

func (c *Collector) fetchBisonFute(ctx context.Context) []events.Event {
	const dirURL = "http://tipi.bison-fute.gouv.fr/bison-fute-ouvert/publicationsDIR/Evenementiel-DIR/grt/RRN/?C=M;O=D"
	buf, err := curlBytes(ctx, dirURL)
	if err != nil {
		return nil
	}
	linkRe := regexp.MustCompile(`href="([0-9]+\.xml)"`)
	seen := map[string]bool{}
	out := []events.Event{}
	for _, m := range linkRe.FindAllStringSubmatch(string(buf), -1) {
		if seen[m[1]] {
			continue
		}
		seen[m[1]] = true
		endpoint := "http://tipi.bison-fute.gouv.fr/bison-fute-ouvert/publicationsDIR/Evenementiel-DIR/grt/RRN/" + m[1]
		body, err := curlBytes(ctx, endpoint)
		if err != nil {
			continue
		}
		out = append(out, parseDATEXEvents("Bison Fute DATEX", endpoint, "https://www.bison-fute.gouv.fr/", "France", "bison_fute", body)...)
		if len(out) >= 80 || len(seen) >= 40 {
			break
		}
	}
	return out
}

func curlBytes(ctx context.Context, rawURL string) ([]byte, error) {
	return exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "30", "-A", "gordios/0.1", rawURL).Output()
}

var (
	situationRe    = regexp.MustCompile(`(?s)<(?:[A-Za-z0-9_]+:)?situation\b[^>]*\bid="([^"]+)"[^>]*>(.*?)</(?:[A-Za-z0-9_]+:)?situation>`)
	xsiTypeRe      = regexp.MustCompile(`xsi:type="(?:[A-Za-z0-9_]+:)?([^"]+)"`)
	xmlTagStripRe  = regexp.MustCompile(`<[^>]+>`)
	xmlSpaceFoldRe = regexp.MustCompile(`\s+`)
)

func parseDATEXEvents(provider, endpoint, publicURL, country, prefix string, buf []byte) []events.Event {
	buf = gunzipMaybe(buf)
	xmlText := string(buf)
	matches := situationRe.FindAllStringSubmatch(xmlText, -1)
	out := make([]events.Event, 0, len(matches))
	for _, m := range matches {
		id, block := m[1], m[2]
		lat, lon, ok := datexCentroid(block)
		if !ok {
			continue
		}
		typ := datexRecordType(block)
		severity := tagValue(block, "overallSeverity")
		ts := firstNonZero(
			parseTimeString(tagValue(block, "situationVersionTime")),
			parseTimeString(tagValue(block, "situationRecordVersionTime")),
			parseTimeString(tagValue(block, "situationRecordCreationTime")),
			parseTimeString(tagValue(block, "overallStartTime")),
		)
		end := parseTimeString(tagValue(block, "overallEndTime"))
		road := firstNonEmpty(tagValue(block, "roadNumber"), tagValue(block, "roadName"))
		comments := datexComments(block)
		title := strings.TrimSpace(strings.Join(nonEmpty(road, typ, severity), " - "))
		if title == "" {
			title = provider + " road incident"
		}
		desc := strings.Join(comments, " ")
		props := baseProps(provider, endpoint, publicURL, title, desc, typ)
		props["country"] = country
		props["incident_id"] = id
		props["severity"] = severity
		props["status"] = tagValue(block, "informationStatus")
		props["probability"] = tagValue(block, "probabilityOfOccurrence")
		props["road"] = road
		props["source_name"] = firstNonEmpty(tagValue(block, "sourceIdentification"), tagValue(block, "sourceName"))
		props["location"] = firstNonEmpty(tagValue(block, "alertCLocationName"), firstNonEmpty(comments...))
		props["labels"] = roadLabels(title + " " + desc + " " + typ)
		props["road_disruption_score"] = roadScore(title, typ, severity, desc)
		props["source_payload_validity"] = validity(ts, end)
		out = append(out, event(ts, prefix+":"+id, lat, lon, props))
	}
	return out
}

func datexRecordType(block string) string {
	if m := xsiTypeRe.FindStringSubmatch(block); len(m) > 1 {
		return m[1]
	}
	return firstNonEmpty(tagValue(block, "abnormalTrafficType"), tagValue(block, "obstructionType"), "road incident")
}

func datexCentroid(block string) (float64, float64, bool) {
	lats := tagValues(block, "latitude")
	lons := tagValues(block, "longitude")
	n := len(lats)
	if len(lons) < n {
		n = len(lons)
	}
	var latSum, lonSum float64
	var count int
	for i := 0; i < n; i++ {
		lat, latOK := floatAny(lats[i])
		lon, lonOK := floatAny(lons[i])
		if latOK && lonOK && validLatLon(lat, lon) {
			latSum += lat
			lonSum += lon
			count++
		}
	}
	if count == 0 {
		return 0, 0, false
	}
	return latSum / float64(count), lonSum / float64(count), true
}

func datexComments(block string) []string {
	values := tagValues(block, "value")
	out := []string{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" && len(v) < 500 {
			out = append(out, v)
		}
		if len(out) >= 6 {
			break
		}
	}
	return unique(out)
}

func tagValue(block, tag string) string {
	vals := tagValues(block, tag)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func tagValues(block, tag string) []string {
	re := regexp.MustCompile(`(?s)<(?:[A-Za-z0-9_]+:)?` + regexp.QuoteMeta(tag) + `(?:\s[^>]*)?>(.*?)</(?:[A-Za-z0-9_]+:)?` + regexp.QuoteMeta(tag) + `>`)
	matches := re.FindAllStringSubmatch(block, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		v := html.UnescapeString(xmlTagStripRe.ReplaceAllString(m[1], " "))
		v = xmlSpaceFoldRe.ReplaceAllString(v, " ")
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func gunzipMaybe(buf []byte) []byte {
	if len(buf) < 2 || buf[0] != 0x1f || buf[1] != 0x8b {
		return buf
	}
	r, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		return buf
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return buf
	}
	return out
}

func (c *Collector) fetchRSS(ctx context.Context, feed rssFeed) []events.Event {
	buf, err := curlBytes(ctx, feed.URL)
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
		lat, lon, ok := parsePoint(firstNonEmpty(it.GeoRSSPoint, it.Point))
		if !ok {
			lat, lon, ok = latLonStrings(it.Latitude, it.Longitude)
		}
		if !ok {
			lat, lon = feed.Lat, feed.Lon
		}
		if !validLatLon(lat, lon) {
			continue
		}
		ts := parseTimeString(firstNonEmpty(it.PubDate, it.Updated, it.Date, it.EventStart, it.OverallStart))
		end := parseTimeString(firstNonEmpty(it.EventEnd, it.OverallEnd))
		typ := firstNonEmpty(it.Category, firstWord(title), "road incident")
		id := firstNonEmpty(it.GUID, it.Link, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, title, desc, typ)
		props["state"] = feed.State
		if feed.Country != "" {
			props["country"] = feed.Country
		}
		props["road"] = strings.TrimSpace(it.Road)
		props["region"] = strings.TrimSpace(it.Region)
		props["county"] = strings.TrimSpace(it.County)
		props["reference"] = strings.TrimSpace(it.Reference)
		props["link"] = strings.TrimSpace(it.Link)
		props["labels"] = roadLabels(title + " " + desc)
		props["road_disruption_score"] = roadScore(title, typ, desc)
		props["source_payload_validity"] = validity(ts, end)
		out = append(out, event(ts, "rss:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	for _, e := range env.Entries {
		title := strings.TrimSpace(e.Title)
		desc := strip(firstNonEmpty(e.Summary, e.Content))
		lat, lon, ok := parsePoint(firstNonEmpty(e.GeoRSSPoint, e.Point))
		if !ok {
			lat, lon = feed.Lat, feed.Lon
		}
		if !validLatLon(lat, lon) {
			continue
		}
		ts := parseTimeString(firstNonEmpty(e.Updated, e.Published))
		id := firstNonEmpty(e.ID, e.Link.Href, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, feed.PublicURL, title, desc, firstWord(title))
		props["state"] = feed.State
		if feed.Country != "" {
			props["country"] = feed.Country
		}
		props["link"] = firstNonEmpty(e.Link.Href, e.ID)
		props["labels"] = roadLabels(title + " " + desc)
		props["road_disruption_score"] = roadScore(title, "", desc)
		out = append(out, event(ts, "atom:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	return out
}

type feedEnvelope struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	Title        string `xml:"title"`
	Link         string `xml:"link"`
	Description  string `xml:"description"`
	PubDate      string `xml:"pubDate"`
	Updated      string `xml:"updated"`
	Date         string `xml:"date"`
	GUID         string `xml:"guid"`
	Category     string `xml:"category"`
	GeoRSSPoint  string `xml:"http://www.georss.org/georss point"`
	Point        string `xml:"point"`
	Latitude     string `xml:"latitude"`
	Longitude    string `xml:"longitude"`
	Reference    string `xml:"reference"`
	Road         string `xml:"road"`
	Region       string `xml:"region"`
	County       string `xml:"county"`
	OverallStart string `xml:"overallStart"`
	OverallEnd   string `xml:"overallEnd"`
	EventStart   string `xml:"eventStart"`
	EventEnd     string `xml:"eventEnd"`
}

type atomEntry struct {
	Title       string `xml:"title"`
	Updated     string `xml:"updated"`
	Published   string `xml:"published"`
	ID          string `xml:"id"`
	Summary     string `xml:"summary"`
	Content     string `xml:"content"`
	GeoRSSPoint string `xml:"http://www.georss.org/georss point"`
	Point       string `xml:"point"`
	Link        struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}

func baseProps(provider, endpoint, publicURL, title, desc, incidentType string) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"source_public_url":    publicURL,
		"title":                strings.TrimSpace(title),
		"description":          strings.TrimSpace(desc),
		"incident_type":        strings.TrimSpace(incidentType),
		"source_provider_kind": "official_transport_incident_feed",
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	addRoadSubtypeScores(props)
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func roadLabels(text string) []string {
	l := strings.ToLower(text)
	out := []string{"road_incident"}
	if containsAny(l, "crash", "collision", "accident", "vehicle") {
		out = append(out, "road_accident")
	}
	if containsAny(l, "closed", "closure", "blocked", "detour", "lane closure") {
		out = append(out, "road_closure")
	}
	if containsAny(l, "flood", "high water", "water over") {
		out = append(out, "flood")
	}
	if containsAny(l, "snow", "ice", "winter") {
		out = append(out, "winter_weather")
	}
	if containsAny(l, "fire", "smoke") {
		out = append(out, "fire_or_smoke")
	}
	if containsAny(l, "hazmat", "spill", "chemical") {
		out = append(out, "hazmat")
	}
	return unique(out)
}

func roadScore(parts ...string) float64 {
	l := strings.ToLower(strings.Join(parts, " "))
	score := 0.6
	if containsAny(l, "crash", "collision", "accident") {
		score = math.Max(score, 1.2)
	}
	if containsAny(l, "closed", "closure", "blocked", "detour", "all lanes") {
		score = math.Max(score, 1.8)
	}
	if containsAny(l, "hazmat", "chemical", "fuel spill", "major", "fatal") {
		score = math.Max(score, 2.4)
	}
	if containsAny(l, "interstate", "i-", "bridge", "tunnel") {
		score += 0.3
	}
	if containsAny(l, "flood", "wildfire", "fire", "smoke", "snow", "ice") {
		score += 0.2
	}
	return propx.ClampFloat(score, 0, 3)
}

func addRoadSubtypeScores(props map[string]any) {
	score, _ := floatAny(props["road_disruption_score"])
	text := strings.ToLower(strings.Join([]string{
		textAny(props["title"]),
		textAny(props["description"]),
		textAny(props["incident_type"]),
		textAny(props["condition"]),
	}, " "))
	labels := labelSet(props["labels"])
	if labels["road_closure"] || containsAny(text, "closed", "closure", "blocked", "detour", "all lanes") {
		props["closure_score"] = propx.ClampFloat(math.Max(score, 1.2), 0, 3)
	}
	if labels["road_accident"] || containsAny(text, "crash", "collision", "accident") {
		props["crash_score"] = propx.ClampFloat(math.Max(score, 0.9), 0, 3)
	}
	if labels["hazmat"] || containsAny(text, "hazmat", "chemical", "spill") {
		props["hazmat_score"] = propx.ClampFloat(math.Max(score, 1.4), 0, 3)
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

func copyProps(dst map[string]any, src map[string]any) {
	for k, v := range src {
		if _, exists := dst[k]; exists {
			dst["raw_"+k] = v
		} else {
			dst[k] = v
		}
	}
}

func mapsAt(raw map[string]any, key string) []map[string]any {
	v, ok := raw[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func pointFromMap(m map[string]any) (float64, float64, bool) {
	for _, ks := range [][2]string{
		{"lat", "lon"}, {"lat", "lng"}, {"latitude", "longitude"}, {"Latitude", "Longitude"},
		{"y", "x"}, {"Y", "X"},
	} {
		lat, latOK := floatAny(m[ks[0]])
		lon, lonOK := floatAny(m[ks[1]])
		if latOK && lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	if loc, ok := m["location"].(map[string]any); ok {
		return pointFromMap(loc)
	}
	return 0, 0, false
}

func pointFromGeometry(g map[string]any) (float64, float64, bool) {
	x, xOK := floatAny(g["x"])
	y, yOK := floatAny(g["y"])
	if xOK && yOK {
		if math.Abs(x) > 180 || math.Abs(y) > 90 {
			lon, lat := webMercatorToLonLat(x, y)
			return lat, lon, validLatLon(lat, lon)
		}
		return y, x, validLatLon(y, x)
	}
	return 0, 0, false
}

func pointFromOregon(attrs, geom map[string]any) (float64, float64, bool) {
	lat, latOK := floatAny(attrs["startLatitude"])
	lon, lonOK := floatAny(attrs["startLongitude"])
	if latOK && lonOK && validLatLon(lat, lon) {
		return lat, lon, true
	}
	return pointFromGeometry(geom)
}

func geoJSONPoint(typ string, raw json.RawMessage) (float64, float64, bool) {
	if strings.EqualFold(typ, "Point") {
		var c []float64
		if err := json.Unmarshal(raw, &c); err == nil && len(c) >= 2 && validLatLon(c[1], c[0]) {
			return c[1], c[0], true
		}
	}
	return 0, 0, false
}

func geoJSONCentroid(typ string, raw json.RawMessage) (float64, float64, bool) {
	if lat, lon, ok := geoJSONPoint(typ, raw); ok {
		return lat, lon, true
	}
	var coords any
	if err := json.Unmarshal(raw, &coords); err != nil {
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
			if lon, lonOK := floatAny(arr[0]); lonOK {
				if lat, latOK := floatAny(arr[1]); latOK && validLatLon(lat, lon) {
					latSum += lat
					lonSum += lon
					count++
					return
				}
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

func qldRoadSummary(row map[string]any) (string, string) {
	m, _ := row["road_summary"].(map[string]any)
	if m == nil {
		return "", ""
	}
	road := textAny(m["road_name"])
	locality := strings.TrimSpace(strings.Join(nonEmpty(textAny(m["locality"]), textAny(m["local_government_area"]), textAny(m["district"])), ", "))
	return road, locality
}

func qldImpactText(row map[string]any) string {
	m, _ := row["impact"].(map[string]any)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(strings.Join(nonEmpty(textAny(m["impact_type"]), textAny(m["impact_subtype"]), textAny(m["direction"]), textAny(m["delay"])), " "))
}

func nestedAny(m map[string]any, keys ...string) any {
	var cur any = m
	for _, key := range keys {
		next, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = next[key]
	}
	return cur
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

func latLonStrings(latRaw, lonRaw string) (float64, float64, bool) {
	lat, latOK := floatAny(latRaw)
	lon, lonOK := floatAny(lonRaw)
	return lat, lon, latOK && lonOK && validLatLon(lat, lon)
}

func webMercatorToLonLat(x, y float64) (float64, float64) {
	lon := x * 180.0 / 20037508.34
	lat := math.Atan(math.Exp(y*math.Pi/20037508.34))*360.0/math.Pi - 90.0
	return lon, lat
}

func parseTimeAny(vals ...any) time.Time {
	for _, v := range vals {
		if t := parseTimeString(textAny(v)); !t.IsZero() {
			return t
		}
	}
	return time.Now().UTC()
}

func firstNonZero(vals ...time.Time) time.Time {
	for _, t := range vals {
		if !t.IsZero() {
			return t.UTC()
		}
	}
	return time.Time{}
}

func parseTimeString(s string) time.Time {
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
		time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02 at 15:04",
		"20060102 150405 -0700", "01/02/2006 03:04:05 PM", "01/02/2006 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(6 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": "official_road_incident_active_window",
	}
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

func firstWord(s string) string {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
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

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}
