// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package health_outbreaks ingests no-token official disease/outbreak feeds:
// WHO Disease Outbreak News plus CDC/ECDC RSS. Rows are country-centroid
// events for analyst context and humanitarian/severity priors.
package health_outbreaks

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

const whoDONURL = "https://www.who.int/api/emergencies/diseaseoutbreaknews?sf_provider=dynamicProvider372&sf_culture=en&$orderby=PublicationDateAndTime%20desc&$select=Title,ItemDefaultUrl,PublicationDateAndTime&$top=40"

var rssFeeds = []struct {
	Name string
	URL  string
}{
	{"CDC Health Alert Network", "https://tools.cdc.gov/api/v2/resources/media/132608.rss"},
	{"CDC Travel Health Notices", "https://wwwnc.cdc.gov/travel/rss/notices.xml"},
	{"ECDC Epidemiological Updates", "https://www.ecdc.europa.eu/en/taxonomy/term/1310/feed"},
	{"ECDC Threat Reports", "https://www.ecdc.europa.eu/en/taxonomy/term/1505/feed"},
	{"ECDC Risk Assessments", "https://www.ecdc.europa.eu/en/taxonomy/term/1295/feed"},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "health_outbreaks" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := c.fetchWHO(ctx)
	for _, f := range rssFeeds {
		out = append(out, c.fetchRSS(ctx, f.Name, f.URL)...)
	}
	return dedupe(out), nil
}

func (c *Collector) fetchWHO(ctx context.Context) []events.Event {
	var resp struct {
		Value []struct {
			Title                  string `json:"Title"`
			ItemDefaultURL         string `json:"ItemDefaultUrl"`
			PublicationDateAndTime string `json:"PublicationDateAndTime"`
		} `json:"value"`
	}
	if err := httpx.GetJSON(ctx, whoDONURL, map[string]string{"Accept": "application/json"}, &resp); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(resp.Value))
	for _, item := range resp.Value {
		link := item.ItemDefaultURL
		if link != "" && !strings.HasPrefix(link, "http") {
			link = "https://www.who.int" + link
		}
		out = append(out, outbreakEvent("WHO Disease Outbreak News", item.Title, "", link, item.PublicationDateAndTime, whoDONURL))
	}
	return out
}

func (c *Collector) fetchRSS(ctx context.Context, name, url string) []events.Event {
	buf, err := httpx.GetBytes(ctx, url, map[string]string{
		"Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
	})
	if err != nil {
		return nil
	}
	var env feedEnv
	if err := xml.Unmarshal(buf, &env); err != nil {
		return nil
	}
	out := []events.Event{}
	for _, it := range env.Items {
		out = append(out, outbreakEvent(name, it.Title, strip(it.Description), it.Link, it.PubDate, url))
	}
	for _, e := range env.Entries {
		body := e.Summary
		if body == "" {
			body = e.Content
		}
		out = append(out, outbreakEvent(name, e.Title, strip(body), firstNonEmpty(e.Link.Href, e.ID), firstNonEmpty(e.Updated, e.Published), url))
	}
	return out
}

func outbreakEvent(source, title, desc, link, date, endpoint string) events.Event {
	text := strings.TrimSpace(title + " " + desc)
	lat, lon, country, cc, ok := locate(text)
	if !ok && strings.Contains(strings.ToLower(source), "cdc") {
		lat, lon, country, cc, ok = countryForCode("US")
	}
	if !ok {
		return events.Event{}
	}
	disease := detectDisease(title + " " + desc)
	level := detectLevel(title + " " + desc)
	ts := parseTime(date)
	props := map[string]any{
		"title":               strings.TrimSpace(title),
		"description":         desc,
		"link":                strings.TrimSpace(link),
		"source":              source,
		"country":             country,
		"country_code":        cc,
		"disease":             disease,
		"alert_level":         level,
		"date":                date,
		"source_api_endpoint": endpoint,
	}
	return events.Event{
		Ts:     ts,
		Source: "health_outbreaks",
		ExtID:  stableID(source + ":" + firstNonEmpty(link, title, date)),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}
}

type rssItem struct {
	XMLName     xml.Name `xml:"item"`
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	Description string   `xml:"description"`
	PubDate     string   `xml:"pubDate"`
	GUID        string   `xml:"guid"`
}

type atomEntry struct {
	XMLName   xml.Name `xml:"entry"`
	Title     string   `xml:"title"`
	Updated   string   `xml:"updated"`
	Published string   `xml:"published"`
	ID        string   `xml:"id"`
	Summary   string   `xml:"summary"`
	Content   string   `xml:"content"`
	Link      struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}

type feedEnv struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

var tagRE = regexp.MustCompile(`<[^>]+>`)

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(tagRE.ReplaceAllString(s, " "))
}

func parseTime(s string) time.Time {
	for _, layout := range []string{
		time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"2006-01-02T15:04:05Z", "2006-01-02 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func detectDisease(text string) string {
	l := strings.ToLower(text)
	for _, d := range []string{
		"mpox", "ebola", "cholera", "covid", "dengue", "measles", "polio",
		"marburg", "lassa", "plague", "yellow fever", "typhoid", "influenza",
		"avian influenza", "h5n1", "anthrax", "rabies", "meningitis", "hepatitis",
		"nipah", "malaria", "diphtheria", "chikungunya", "norovirus",
	} {
		if strings.Contains(l, d) {
			return d
		}
	}
	return "unknown"
}

func detectLevel(text string) string {
	l := strings.ToLower(text)
	if strings.Contains(l, "outbreak") || strings.Contains(l, "emergency") || strings.Contains(l, "epidemic") || strings.Contains(l, "pandemic") {
		return "alert"
	}
	if strings.Contains(l, "warning") || strings.Contains(l, "increasing") || strings.Contains(l, "spread") {
		return "warning"
	}
	return "watch"
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.ExtID == "" || !e.HasPoint() {
			continue
		}
		if seen[e.ExtID] {
			continue
		}
		seen[e.ExtID] = true
		out = append(out, e)
	}
	return out
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(h[:])
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

var countryCodes = map[string]string{
	"afghanistan": "AF", "australia": "AU", "bangladesh": "BD", "brazil": "BR",
	"canada": "CA", "china": "CN", "colombia": "CO", "congo": "CG",
	"democratic republic of the congo": "CD", "dr congo": "CD", "ecuador": "EC",
	"egypt": "EG", "ethiopia": "ET", "france": "FR", "germany": "DE",
	"ghana": "GH", "haiti": "HT", "india": "IN", "indonesia": "ID",
	"iran": "IR", "iraq": "IQ", "israel": "IL", "italy": "IT",
	"japan": "JP", "kenya": "KE", "lebanon": "LB", "mexico": "MX",
	"myanmar": "MM", "nigeria": "NG", "pakistan": "PK", "peru": "PE",
	"philippines": "PH", "russia": "RU", "saudi arabia": "SA", "somalia": "SO",
	"south africa": "ZA", "south sudan": "SS", "sudan": "SD", "syria": "SY",
	"thailand": "TH", "turkey": "TR", "uganda": "UG", "ukraine": "UA",
	"united kingdom": "GB", "united states": "US", "usa": "US", "venezuela": "VE",
	"vietnam": "VN", "yemen": "YE", "zambia": "ZM", "zimbabwe": "ZW",
}

func locate(text string) (lat, lon float64, country, code string, ok bool) {
	l := strings.ToLower(text)
	names := make([]string, 0, len(countryCodes))
	for name := range countryCodes {
		names = append(names, name)
	}
	// Longer names first, so "democratic republic..." wins before "congo".
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if len(names[j]) > len(names[i]) {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	for _, name := range names {
		if strings.Contains(l, name) {
			return countryForCode(countryCodes[name])
		}
	}
	return 0, 0, "", "", false
}

func countryForCode(code string) (lat, lon float64, country, outCode string, ok bool) {
	cc := strings.ToUpper(strings.TrimSpace(code))
	c := geo.Centroids[cc]
	if c.Lat == 0 && c.Lon == 0 {
		return 0, 0, "", "", false
	}
	name := cc
	for n, v := range countryCodes {
		if strings.EqualFold(v, cc) {
			name = strings.Title(n)
			break
		}
	}
	return c.Lat, c.Lon, name, cc, true
}
