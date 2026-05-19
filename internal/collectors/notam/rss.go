// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Generic NOTAM/aviation RSS mirror. Reads a comma-separated list of feed
// URLs from NOTAM_RSS_FEEDS and emits one event per item. Items without a
// usable lat/lon fall back to the feed's overall geolocation or are dropped.
//
// Example env:
//
//	NOTAM_RSS_FEEDS=https://www.aopa.org/rss/advocacy,https://www.faa.gov/news/rss/notams
package notam

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type RSSCollector struct {
	feeds []string
}

func NewRSS() (*RSSCollector, error) {
	raw := os.Getenv("NOTAM_RSS_FEEDS")
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("NOTAM_RSS_FEEDS not set")
	}
	feeds := []string{}
	for _, u := range strings.Split(raw, ",") {
		u = strings.TrimSpace(u)
		if u != "" {
			feeds = append(feeds, u)
		}
	}
	if len(feeds) == 0 {
		return nil, fmt.Errorf("NOTAM_RSS_FEEDS empty")
	}
	return &RSSCollector{feeds: feeds}, nil
}

func (c *RSSCollector) ID() string               { return "notam_rss" }
func (c *RSSCollector) PollEvery() time.Duration { return 30 * time.Minute }

// rss and atom share enough structure that a liberal struct covers both.
type rssItem struct {
	XMLName     xml.Name `xml:"item"`
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	Description string   `xml:"description"`
	PubDate     string   `xml:"pubDate"`
	GUID        string   `xml:"guid"`
	GeoLat      string   `xml:"lat"` // georss-simple
	GeoLong     string   `xml:"long"`
	GeoPoint    string   `xml:"point"` // georss point: "lat lon"
}
type atomEntry struct {
	XMLName  xml.Name `xml:"entry"`
	Title    string   `xml:"title"`
	Updated  string   `xml:"updated"`
	ID       string   `xml:"id"`
	Summary  string   `xml:"summary"`
	GeoPoint string   `xml:"point"` // sometimes georss
	Link     struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}
type feedEnvelope struct {
	XMLName xml.Name    `xml:"-"`
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

var coordInText = regexp.MustCompile(`(-?\d{1,3}\.\d{2,6})[,\s]+(-?\d{1,3}\.\d{2,6})`)

func (c *RSSCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	for _, url := range c.feeds {
		buf, err := httpx.GetBytes(ctx, url, map[string]string{
			"Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
		})
		if err != nil {
			continue
		}
		var env feedEnvelope
		if err := xml.Unmarshal(buf, &env); err != nil {
			continue
		}
		now := time.Now().UTC()
		for _, it := range env.Items {
			lat, lon, ok := itemCoords(it.GeoLat, it.GeoLong, it.GeoPoint, it.Description+" "+it.Title)
			if !ok {
				continue
			}
			id := it.GUID
			if id == "" {
				id = it.Link
			}
			if id == "" {
				id = it.Title
			}
			ts := now
			if t, err := time.Parse(time.RFC1123Z, it.PubDate); err == nil {
				ts = t.UTC()
			} else if t, err := time.Parse(time.RFC1123, it.PubDate); err == nil {
				ts = t.UTC()
			}
			out = append(out, events.Event{
				Ts: ts, Source: "notam_rss", ExtID: id,
				Lat: lat, Lon: lon,
				Props: map[string]any{
					"title":       it.Title,
					"link":        it.Link,
					"description": it.Description,
					"feed":        url,
				},
			})
		}
		for _, e := range env.Entries {
			lat, lon, ok := itemCoords("", "", e.GeoPoint, e.Summary+" "+e.Title)
			if !ok {
				continue
			}
			id := e.ID
			if id == "" {
				id = e.Link.Href
			}
			ts := now
			if t, err := time.Parse(time.RFC3339, e.Updated); err == nil {
				ts = t.UTC()
			}
			out = append(out, events.Event{
				Ts: ts, Source: "notam_rss", ExtID: id,
				Lat: lat, Lon: lon,
				Props: map[string]any{
					"title":   e.Title,
					"link":    e.Link.Href,
					"summary": e.Summary,
					"feed":    url,
				},
			})
		}
	}
	return out, nil
}

// itemCoords tries geo tags first, then a "lat, lon" pattern in free text.
func itemCoords(lat, lon, point, text string) (float64, float64, bool) {
	if lat != "" && lon != "" {
		la, err1 := strconv.ParseFloat(strings.TrimSpace(lat), 64)
		lo, err2 := strconv.ParseFloat(strings.TrimSpace(lon), 64)
		if err1 == nil && err2 == nil {
			return la, lo, true
		}
	}
	if point != "" {
		parts := strings.Fields(strings.TrimSpace(point))
		if len(parts) >= 2 {
			la, err1 := strconv.ParseFloat(parts[0], 64)
			lo, err2 := strconv.ParseFloat(parts[1], 64)
			if err1 == nil && err2 == nil {
				return la, lo, true
			}
		}
	}
	m := coordInText.FindStringSubmatch(text)
	if len(m) >= 3 {
		la, err1 := strconv.ParseFloat(m[1], 64)
		lo, err2 := strconv.ParseFloat(m[2], 64)
		// sanity: lat in [-90,90], lon in [-180,180]
		if err1 == nil && err2 == nil && la >= -90 && la <= 90 && lo >= -180 && lo <= 180 {
			return la, lo, true
		}
	}
	return 0, 0, false
}
