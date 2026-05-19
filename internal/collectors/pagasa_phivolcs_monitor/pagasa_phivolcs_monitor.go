// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package pagasa_phivolcs_monitor ingests official Philippines weather,
// earthquake, and volcano status pages from PAGASA and PHIVOLCS.
package pagasa_phivolcs_monitor

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	"golang.org/x/net/html"
)

const (
	sourceID         = "pagasa_phivolcs_monitor"
	phivolcsQuakeURL = "https://earthquake.phivolcs.dost.gov.ph/"
	wovodatURL       = "https://wovodat.phivolcs.dost.gov.ph/"
	pagasaTCURL      = "https://bagong.pagasa.dost.gov.ph/tropical-cyclone/severe-weather-bulletin"
)

type Collector struct {
	maxQuakes int
	client    *http.Client
}

func New() (*Collector, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// PHIVOLCS serves an official endpoint chain that the slim runtime image
	// does not currently validate. Keep this client scoped to PHIVOLCS only.
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	return &Collector{
		maxQuakes: collectorutil.EnvInt("PHIVOLCS_MAX_EARTHQUAKES", 40, 1, 200),
		client:    &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("PAGASA_PHIVOLCS_POLL_EVERY_S", 600, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error

	if buf, err := c.getPHIVOLCS(ctx, phivolcsQuakeURL); err == nil {
		rows, err := parsePHIVOLCSEarthquakes(string(buf), phivolcsQuakeURL)
		if err == nil {
			if len(rows) > c.maxQuakes {
				rows = rows[:c.maxQuakes]
			}
			out = append(out, earthquakeEvents(rows)...)
		} else if firstErr == nil {
			firstErr = err
		}
	} else if firstErr == nil {
		firstErr = err
	}

	if buf, err := c.getPHIVOLCS(ctx, wovodatURL); err == nil {
		out = append(out, volcanoEvents(parseVolcanoStatuses(string(buf)))...)
	} else if firstErr == nil {
		firstErr = err
	}

	if buf, err := httpx.GetBytes(ctx, pagasaTCURL, map[string]string{"Accept": "text/html"}); err == nil {
		out = append(out, pagasaEvents(string(buf), pagasaTCURL)...)
	} else if firstErr == nil {
		firstErr = err
	}

	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (c *Collector) getPHIVOLCS(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/html")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("%s: %d %s", rawURL, resp.StatusCode, string(buf))
	}
	return io.ReadAll(resp.Body)
}

type quakeRow struct {
	Date      string
	Lat       float64
	Lon       float64
	DepthKM   float64
	Magnitude float64
	Location  string
	URL       string
}

func parsePHIVOLCSEarthquakes(rawHTML, baseURL string) ([]quakeRow, error) {
	doc, err := html.Parse(strings.NewReader(rawHTML))
	if err != nil {
		return nil, err
	}
	rows := []quakeRow{}
	for _, tr := range nodesByTag(doc, "tr") {
		cells := directChildrenByTag(tr, "td")
		if len(cells) < 6 {
			continue
		}
		date := normalize(textContent(cells[0]))
		if !strings.Contains(date, " - ") {
			continue
		}
		lat, latOK := parseFloatText(cells[1])
		lon, lonOK := parseFloatText(cells[2])
		depth, depthOK := parseFloatText(cells[3])
		mag, magOK := parseFloatText(cells[4])
		if !latOK || !lonOK || !depthOK || !magOK || !collectorutil.ValidLatLon(lat, lon) {
			continue
		}
		rows = append(rows, quakeRow{
			Date:      date,
			Lat:       lat,
			Lon:       lon,
			DepthKM:   depth,
			Magnitude: mag,
			Location:  normalize(textContent(cells[5])),
			URL:       absoluteURL(baseURL, firstLinkHref(cells[0])),
		})
	}
	return rows, nil
}

func earthquakeEvents(rows []quakeRow) []events.Event {
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		ts := parsePhilippinesDateTime(row.Date)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		props := map[string]any{
			"source_provider":     "PHIVOLCS",
			"source_api_endpoint": phivolcsQuakeURL,
			"source_url":          row.URL,
			"country":             "Philippines",
			"country_code":        "PH",
			"hazard_type":         "earthquake",
			"date":                row.Date,
			"magnitude":           row.Magnitude,
			"depth_km":            row.DepthKM,
			"location":            row.Location,
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  "phivolcs_quake:" + collectorutil.StableID(fmt.Sprintf("%s|%.4f|%.4f|%.1f", row.Date, row.Lat, row.Lon, row.Magnitude)),
			Lat:    row.Lat,
			Lon:    row.Lon,
			Props:  props,
		})
	}
	return out
}

type volcanoStatus struct {
	Name  string
	Level int
}

var volcanoStatusRE = regexp.MustCompile(`([A-Za-z]+)\s*-\s*([0-9]+)`)

func parseVolcanoStatuses(rawHTML string) []volcanoStatus {
	doc, err := html.Parse(strings.NewReader(rawHTML))
	if err != nil {
		return nil
	}
	text := normalize(textContent(doc))
	matches := volcanoStatusRE.FindAllStringSubmatch(text, -1)
	out := []volcanoStatus{}
	seen := map[string]struct{}{}
	for _, m := range matches {
		name := strings.TrimSpace(m[1])
		if _, ok := volcanoCoords[name]; !ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		level, _ := strconv.Atoi(m[2])
		seen[name] = struct{}{}
		out = append(out, volcanoStatus{Name: name, Level: level})
	}
	return out
}

func volcanoEvents(rows []volcanoStatus) []events.Event {
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		loc := volcanoCoords[row.Name]
		props := map[string]any{
			"source_provider":     "PHIVOLCS WOVOdat",
			"source_api_endpoint": wovodatURL,
			"source_url":          wovodatURL,
			"country":             "Philippines",
			"country_code":        "PH",
			"hazard_type":         "volcano",
			"volcano":             row.Name,
			"alert_level":         row.Level,
		}
		out = append(out, events.Event{
			Ts:     now,
			Source: sourceID,
			ExtID:  fmt.Sprintf("phivolcs_volcano:%s:%s", strings.ToLower(row.Name), now.Format("20060102T1504")),
			Lat:    loc.Lat,
			Lon:    loc.Lon,
			Props:  props,
		})
	}
	return out
}

func pagasaEvents(rawHTML, endpoint string) []events.Event {
	doc, err := html.Parse(strings.NewReader(rawHTML))
	if err != nil {
		return nil
	}
	text := normalize(textContent(doc))
	title := "PAGASA Tropical Cyclone Bulletin"
	active := !strings.Contains(strings.ToLower(text), "no active tropical cyclone")
	if !active {
		title = "No Active Tropical Cyclone within the Philippine Area of Responsibility"
	}
	props := map[string]any{
		"source_provider":         "PAGASA",
		"source_api_endpoint":     endpoint,
		"source_url":              endpoint,
		"country":                 "Philippines",
		"country_code":            "PH",
		"hazard_type":             "tropical_cyclone",
		"title":                   title,
		"active_tropical_cyclone": active,
	}
	now := time.Now().UTC()
	return []events.Event{{
		Ts:     now,
		Source: sourceID,
		ExtID:  "pagasa_tc:" + collectorutil.StableID(title+"|"+now.Format("20060102T1504")),
		Lat:    12.8797,
		Lon:    121.7740,
		Props:  props,
	}}
}

type latLon struct{ Lat, Lon float64 }

var volcanoCoords = map[string]latLon{
	"Taal":     {14.0020, 120.9930},
	"Kanlaon":  {10.4120, 123.1320},
	"Bulusan":  {12.7700, 124.0500},
	"Pinatubo": {15.1429, 120.3496},
	"Mayon":    {13.2570, 123.6850},
}

func nodesByTag(n *html.Node, tag string) []*html.Node {
	out := []*html.Node{}
	var walk func(*html.Node)
	walk = func(cur *html.Node) {
		if cur.Type == html.ElementNode && cur.Data == tag {
			out = append(out, cur)
		}
		for child := cur.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return out
}

func directChildrenByTag(n *html.Node, tag string) []*html.Node {
	out := []*html.Node{}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.ElementNode && child.Data == tag {
			out = append(out, child)
		}
	}
	return out
}

func textContent(n *html.Node) string {
	if n == nil {
		return ""
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	var b strings.Builder
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if s := textContent(child); s != "" {
			b.WriteString(s)
			b.WriteByte(' ')
		}
	}
	return b.String()
}

func firstLinkHref(n *html.Node) string {
	if n == nil {
		return ""
	}
	if n.Type == html.ElementNode && n.Data == "a" {
		for _, attr := range n.Attr {
			if attr.Key == "href" {
				return attr.Val
			}
		}
	}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if href := firstLinkHref(child); href != "" {
			return href
		}
	}
	return ""
}

func absoluteURL(baseURL, href string) string {
	href = strings.ReplaceAll(strings.TrimSpace(href), `\`, "/")
	if href == "" {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return href
	}
	if u.IsAbs() {
		return u.String()
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return href
	}
	return base.ResolveReference(u).String()
}

func parseFloatText(n *html.Node) (float64, bool) {
	fields := strings.Fields(textContent(n))
	if len(fields) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	return v, err == nil
}

func parsePhilippinesDateTime(raw string) time.Time {
	loc := time.FixedZone("PST", 8*3600)
	for _, layout := range []string{"2 January 2006 - 03:04 PM", "02 January 2006 - 03:04 PM"} {
		if t, err := time.ParseInLocation(layout, strings.TrimSpace(raw), loc); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func normalize(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
