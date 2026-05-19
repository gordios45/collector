// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Travel advisories aggregator — US State Dept RSS + UK FCDO Atom.
// Country-name match against a centroid table → point events.
package travel_advisories

import (
	"context"
	"encoding/xml"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type feedSpec struct {
	source string
	url    string
}

var feeds = []feedSpec{
	{"US State Dept", "https://travel.state.gov/_res/rss/TAsTWs.xml"},
	{"UK FCDO", "https://www.gov.uk/foreign-travel-advice.atom"},
}

// Country-name → centroid table (lon, lat). Mirrors the frontend lookup.
var countryCentroids = map[string][2]float64{
	"Afghanistan": {67, 33}, "Albania": {20, 41}, "Algeria": {3, 28}, "Argentina": {-64, -34},
	"Australia": {133, -25}, "Bangladesh": {90, 24}, "Belarus": {28, 53}, "Brazil": {-52, -14},
	"Burkina Faso": {-2, 12}, "Cameroon": {12, 6}, "Canada": {-106, 56}, "Central African Republic": {21, 7},
	"Chad": {19, 15}, "China": {104, 35}, "Colombia": {-72, 4}, "Congo": {-15, 1},
	"Cuba": {-80, 22}, "DRC": {24, -3}, "Ecuador": {-78, -2}, "Egypt": {30, 27},
	"El Salvador": {-89, 14}, "Eritrea": {39, 16}, "Ethiopia": {40, 9}, "France": {2, 46},
	"Germany": {10, 51}, "Ghana": {-2, 8}, "Guatemala": {-90, 16}, "Haiti": {-72, 19},
	"Honduras": {-87, 15}, "India": {79, 21}, "Indonesia": {120, -5}, "Iran": {53, 32},
	"Iraq": {44, 33}, "Israel": {35, 31}, "Italy": {13, 42}, "Jamaica": {-77, 18},
	"Japan": {138, 36}, "Jordan": {36, 31}, "Kazakhstan": {67, 48}, "Kenya": {38, 0},
	"North Korea": {127, 40}, "South Korea": {128, 36}, "Kuwait": {48, 29}, "Lebanon": {36, 34},
	"Libya": {17, 27}, "Mali": {-4, 17}, "Mauritania": {-10, 20}, "Mexico": {-102, 23},
	"Morocco": {-6, 32}, "Mozambique": {35, -18}, "Myanmar": {96, 22}, "Nepal": {84, 28},
	"Nicaragua": {-85, 13}, "Niger": {8, 16}, "Nigeria": {8, 10}, "Pakistan": {69, 30},
	"Palestine": {35, 32}, "Papua New Guinea": {147, -6}, "Peru": {-76, -10}, "Philippines": {122, 13},
	"Russia": {100, 60}, "Rwanda": {30, -2}, "Saudi Arabia": {45, 24}, "Somalia": {46, 6},
	"South Africa": {25, -30}, "South Sudan": {32, 7}, "Spain": {-4, 40}, "Sri Lanka": {81, 8},
	"Sudan": {30, 16}, "Syria": {38, 35}, "Taiwan": {121, 24}, "Thailand": {101, 15},
	"Tunisia": {9, 34}, "Turkey": {35, 39}, "Turkmenistan": {60, 39}, "Uganda": {32, 1},
	"Ukraine": {32, 49}, "United Kingdom": {-3, 55}, "Venezuela": {-67, 7}, "Yemen": {48, 16},
	"Zimbabwe": {30, -19},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "travel_advisories" }
func (c *Collector) PollEvery() time.Duration { return 2 * time.Hour }

// findCoords does what the frontend does: linear scan for country substring.
func findCoords(title string) (lat, lon float64, country string, ok bool) {
	t := strings.ToLower(title)
	for name, c := range countryCentroids {
		if strings.Contains(t, strings.ToLower(name)) {
			return c[1], c[0], name, true
		}
	}
	return 0, 0, "", false
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
	XMLName xml.Name `xml:"entry"`
	Title   string   `xml:"title"`
	Updated string   `xml:"updated"`
	ID      string   `xml:"id"`
	Summary string   `xml:"summary"`
	Content string   `xml:"content"`
	Link    struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}
type feedEnv struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

var htmlTagRE = regexp.MustCompile(`<[^>]+>`)

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	seen := map[string]bool{} // country-source dedup key
	out := []events.Event{}
	for _, f := range feeds {
		buf, err := httpx.GetBytes(ctx, f.url, map[string]string{
			"Accept":     "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
			"User-Agent": "Mozilla/5.0 (gordios)",
		})
		if err != nil {
			continue
		}
		var env feedEnv
		if err := xml.Unmarshal(buf, &env); err != nil {
			continue
		}

		// RSS (UK FCDO publishes Atom; US State Dept publishes RSS.)
		for _, it := range env.Items {
			addItem(it.Title, strip(it.Description), it.Link, it.PubDate, it.GUID, f.source, seen, &out)
		}
		for _, e := range env.Entries {
			body := e.Summary
			if body == "" {
				body = e.Content
			}
			addItem(e.Title, strip(body), e.Link.Href, e.Updated, e.ID, f.source, seen, &out)
		}
	}
	return out, nil
}

func addItem(title, desc, link, date, guid, source string, seen map[string]bool, out *[]events.Event) {
	lat, lon, country, ok := findCoords(title)
	if !ok {
		return
	}
	key := country + "|" + source
	if seen[key] {
		return
	}
	seen[key] = true

	id := guid
	if id == "" {
		id = link
	}
	if id == "" {
		id = key
	}
	ts := time.Now().UTC()
	for _, fmt := range []string{time.RFC1123Z, time.RFC1123, time.RFC3339} {
		if t, err := time.Parse(fmt, date); err == nil {
			ts = t.UTC()
			break
		}
	}

	*out = append(*out, events.Event{
		Ts: ts, Source: "travel_advisories", ExtID: id,
		Lat: lat, Lon: lon,
		Props: map[string]any{
			"title":       title,
			"description": desc,
			"link":        link,
			"source":      source,
			"country":     country,
			"date":        date,
		},
	})
}

func strip(s string) string { return strings.TrimSpace(htmlTagRE.ReplaceAllString(s, "")) }
