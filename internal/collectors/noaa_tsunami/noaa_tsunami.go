// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA/NWS tsunami messages from the public NTWC and PTWC Atom feeds.
package noaa_tsunami

import (
	"context"
	"encoding/xml"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	ntwcURL = "https://www.tsunami.gov/events/xml/PAAQAtom.xml"
	ptwcURL = "https://www.tsunami.gov/events/xml/PHEBAtom.xml"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "noaa_tsunami" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

type atomFeed struct {
	Title   string      `xml:"title"`
	Updated string      `xml:"updated"`
	Author  atomAuthor  `xml:"author"`
	Entries []atomEntry `xml:"entry"`
}

type atomAuthor struct {
	Name string `xml:"name"`
}

type atomEntry struct {
	ID      string     `xml:"id"`
	Title   string     `xml:"title"`
	Updated string     `xml:"updated"`
	Lat     string     `xml:"lat"`
	Lon     string     `xml:"long"`
	Summary innerXML   `xml:"summary"`
	Links   []atomLink `xml:"link"`
}

type innerXML struct {
	Inner string `xml:",innerxml"`
}

type atomLink struct {
	Rel   string `xml:"rel,attr"`
	Title string `xml:"title,attr"`
	Href  string `xml:"href,attr"`
	Type  string `xml:"type,attr"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	for _, src := range []struct {
		center string
		url    string
	}{
		{center: "NTWC", url: ntwcURL},
		{center: "PTWC", url: ptwcURL},
	} {
		evs, err := fetchFeed(ctx, src.center, src.url)
		if err != nil {
			return nil, err
		}
		out = append(out, evs...)
	}
	return out, nil
}

func fetchFeed(ctx context.Context, center, url string) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, url, map[string]string{"Accept": "application/atom+xml, application/xml"})
	if err != nil {
		return nil, err
	}
	var feed atomFeed
	if err := xml.Unmarshal(buf, &feed); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(feed.Entries))
	for _, entry := range feed.Entries {
		ev, ok := eventFromEntry(center, url, feed, entry)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromEntry(center, feedURL string, feed atomFeed, entry atomEntry) (events.Event, bool) {
	lat, ok1 := parseFloat(entry.Lat)
	lon, ok2 := parseFloat(entry.Lon)
	if !ok1 || !ok2 {
		return events.Event{}, false
	}
	ts := parseTime(entry.Updated)
	if ts.IsZero() {
		ts = parseTime(feed.Updated)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	text := normalizeSummary(entry.Summary.Inner)
	fields := summaryFields(entry.Summary.Inner)
	capURL, bulletinURL := linkURLs(entry.Links)
	ext := entry.ID
	if ext == "" {
		ext = center + ":" + entry.Title + ":" + ts.Format(time.RFC3339)
	}
	props := map[string]any{
		"id":              entry.ID,
		"title":           strings.TrimSpace(entry.Title),
		"category":        firstNonEmpty(fields["category"], "Unknown"),
		"magnitude":       fields["preliminary magnitude"],
		"affected_region": firstNonEmpty(fields["affected region"], entry.Title),
		"note":            fields["note"],
		"definition":      fields["definition"],
		"summary":         text,
		"center":          center,
		"feed_title":      strings.TrimSpace(feed.Title),
		"feed_url":        feedURL,
		"updated":         entry.Updated,
		"author":          feed.Author.Name,
		"cap_url":         capURL,
		"bulletin_url":    bulletinURL,
	}
	return events.Event{
		Ts:     ts,
		Source: "noaa_tsunami",
		ExtID:  ext,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

var (
	tagRe       = regexp.MustCompile(`<[^>]+>`)
	brRe        = regexp.MustCompile(`(?i)<br\s*/?>`)
	spaceRe     = regexp.MustCompile(`\s+`)
	fieldLineRe = regexp.MustCompile(`(?is)<(?:strong|b)>\s*([^:<]+):?\s*</(?:strong|b)>\s*([^<]+)`)
)

func normalizeSummary(raw string) string {
	s := brRe.ReplaceAllString(raw, "\n")
	s = tagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = spaceRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func summaryFields(raw string) map[string]string {
	out := map[string]string{}
	for _, m := range fieldLineRe.FindAllStringSubmatch(raw, -1) {
		key := strings.ToLower(strings.TrimSpace(html.UnescapeString(m[1])))
		val := strings.TrimSpace(html.UnescapeString(tagRe.ReplaceAllString(m[2], " ")))
		val = spaceRe.ReplaceAllString(val, " ")
		if key != "" && val != "" {
			out[key] = val
		}
	}
	return out
}

func linkURLs(links []atomLink) (capURL, bulletinURL string) {
	for _, l := range links {
		if strings.Contains(strings.ToLower(l.Type), "cap") && capURL == "" {
			capURL = l.Href
		}
		if (strings.Contains(strings.ToLower(l.Title), "bulletin") || strings.Contains(strings.ToLower(l.Href), ".txt")) && bulletinURL == "" {
			bulletinURL = l.Href
		}
	}
	return capURL, bulletinURL
}

func parseFloat(s string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v, err == nil
}

func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(s)); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
