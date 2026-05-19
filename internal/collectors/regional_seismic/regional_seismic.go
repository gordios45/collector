// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package regional_seismic ingests no-key regional earthquake authority feeds.
package regional_seismic

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "regional_seismic"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchGeoNet(ctx)...)
	for _, f := range rssFeeds {
		out = append(out, c.fetchRSS(ctx, f)...)
	}
	return dedupe(out), nil
}

type rssFeed struct {
	Provider string
	URL      string
}

var rssFeeds = []rssFeed{
	{Provider: "British Geological Survey", URL: "http://earthquakes.bgs.ac.uk/feeds/MhSeismology.xml"},
	{Provider: "Natural Resources Canada", URL: "http://www.earthquakescanada.nrcan.gc.ca/cache/earthquakes/canada-en.atom"},
	{Provider: "Geoscience Australia", URL: "https://earthquakes.ga.gov.au/feeds/all_recent.rss"},
	{Provider: "GFZ GEOFON", URL: "https://geofon.gfz-potsdam.de/eqinfo/list.php?fmt=rss"},
}

func (c *Collector) fetchGeoNet(ctx context.Context) []events.Event {
	const endpoint = "https://api.geonet.org.nz/quake?MMI=3"
	var raw struct {
		Features []struct {
			ID         string         `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		if !strings.EqualFold(f.Geometry.Type, "Point") || len(f.Geometry.Coordinates) < 2 {
			continue
		}
		lon, lat := f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
		if !validLatLon(lat, lon) {
			continue
		}
		mag, _ := floatAny(f.Properties["magnitude"])
		if mag < 2.5 {
			continue
		}
		id := firstNonEmpty(textAny(f.Properties["publicID"]), f.ID, stableID(fmt.Sprint(f.Properties)))
		ts := parseTimeAny(f.Properties["time"])
		props := baseProps("GeoNet New Zealand", endpoint, id, mag)
		copyProps(props, f.Properties)
		props["depth_km"], _ = floatAny(f.Properties["depth"])
		props["region"] = firstNonEmpty(textAny(f.Properties["locality"]), textAny(f.Properties["region"]))
		props["source_public_url"] = "https://www.geonet.org.nz/earthquake/" + id
		props["seismic_authority_score"] = authorityScore(mag)
		out = append(out, event(ts, "geonet:"+id, lat, lon, props))
	}
	return out
}

func (c *Collector) fetchRSS(ctx context.Context, feed rssFeed) []events.Event {
	buf, err := httpx.GetBytes(ctx, feed.URL, map[string]string{"Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*"})
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
		lat, lon, ok := parsePoint(firstNonEmpty(it.GeoRSSPoint, it.Point, it.Where.Point))
		if !ok {
			continue
		}
		mag := magnitude(title + " " + desc)
		if mag > 0 && mag < 2.5 {
			continue
		}
		ts := parseTimeString(firstNonEmpty(it.PubDate, it.Updated, it.Date))
		id := firstNonEmpty(it.GUID, it.Link, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, id, mag)
		props["title"] = title
		props["description"] = desc
		props["link"] = it.Link
		props["depth_km"] = depthKM(title + " " + desc)
		props["region"] = regionText(title, desc)
		props["seismic_authority_score"] = authorityScore(mag)
		out = append(out, event(ts, "rss:"+stableID(feed.Provider)+":"+id, lat, lon, props))
	}
	for _, e := range env.Entries {
		title := strings.TrimSpace(e.Title)
		desc := strip(firstNonEmpty(e.Summary, e.Content))
		lat, lon, ok := parsePoint(firstNonEmpty(e.GeoRSSPoint, e.Point, e.Where.Point))
		if !ok {
			continue
		}
		mag := magnitude(title + " " + desc)
		if mag > 0 && mag < 2.5 {
			continue
		}
		ts := parseTimeString(firstNonEmpty(e.Updated, e.Published))
		id := firstNonEmpty(e.ID, e.Link.Href, stableID(feed.Provider+title+fmt.Sprint(lat, lon)))
		props := baseProps(feed.Provider, feed.URL, id, mag)
		props["title"] = title
		props["description"] = desc
		props["link"] = firstNonEmpty(e.Link.Href, e.ID)
		props["depth_km"] = depthKM(title + " " + desc)
		props["region"] = regionText(title, desc)
		props["seismic_authority_score"] = authorityScore(mag)
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
	Date        string `xml:"date"`
	GUID        string `xml:"guid"`
	GeoRSSPoint string `xml:"http://www.georss.org/georss point"`
	Point       string `xml:"point"`
	Where       struct {
		Point string `xml:"Point>pos"`
	} `xml:"where"`
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
	Where       struct {
		Point string `xml:"Point>pos"`
	} `xml:"where"`
	Link struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}

func baseProps(provider, endpoint, id string, mag float64) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"event_id":             id,
		"mag":                  mag,
		"labels":               []string{"earthquake", "seismic_hazard"},
		"source_provider_kind": "official_regional_seismic_authority",
	}
}

func event(ts time.Time, id string, lat, lon float64, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	props["source_payload_validity"] = map[string]any{
		"valid_start":    ts.Format(time.RFC3339),
		"valid_end":      ts.Add(24 * time.Hour).Format(time.RFC3339),
		"validity_basis": "official_seismic_event_time",
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Props: props}
}

func authorityScore(mag float64) float64 {
	if mag <= 0 {
		return 0.5
	}
	return propx.ClampFloat((mag-2.0)*0.8, 0.4, 4)
}

var (
	magRE    = regexp.MustCompile(`(?i)\bM(?:ag(?:nitude)?)?\s*([0-9]+(?:\.[0-9]+)?)`)
	altMagRE = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*(?:ML|Mw|Mb|M\b)`)
	depthRE  = regexp.MustCompile(`(?i)depth[^0-9-]*(-?[0-9]+(?:\.[0-9]+)?)\s*km`)
	tagRE    = regexp.MustCompile(`<[^>]+>`)
)

func magnitude(s string) float64 {
	for _, re := range []*regexp.Regexp{magRE, altMagRE} {
		if m := re.FindStringSubmatch(s); len(m) > 1 {
			if f, err := strconv.ParseFloat(m[1], 64); err == nil {
				return f
			}
		}
	}
	return 0
}

func depthKM(s string) float64 {
	if m := depthRE.FindStringSubmatch(s); len(m) > 1 {
		if f, err := strconv.ParseFloat(m[1], 64); err == nil {
			return f
		}
	}
	return 0
}

func regionText(title, desc string) string {
	s := strings.TrimSpace(title)
	if i := strings.Index(strings.ToLower(s), " - "); i > 0 && i+3 < len(s) {
		return strings.TrimSpace(s[i+3:])
	}
	if len(desc) > 120 {
		return desc[:120]
	}
	return desc
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
		"2006-01-02 15:04:05 MST", "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02",
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
