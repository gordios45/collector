// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package thailand_tmd_alerts ingests official warning bulletins from the Thai
// Meteorological Department warning page.
package thailand_tmd_alerts

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	"golang.org/x/net/html"
)

const (
	sourceID   = "thailand_tmd_alerts"
	defaultURL = "https://www.tmd.go.th/en/warning-and-events/warning-storm"
)

type Collector struct {
	endpoint string
	maxItems int
}

func New() (*Collector, error) {
	endpoint := strings.TrimSpace(os.Getenv("THAILAND_TMD_WARNINGS_URL"))
	if endpoint == "" {
		endpoint = defaultURL
	}
	return &Collector{
		endpoint: endpoint,
		maxItems: collectorutil.EnvInt("THAILAND_TMD_MAX_WARNINGS", 20, 1, 100),
	}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("THAILAND_TMD_POLL_EVERY_S", 900, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, c.endpoint, map[string]string{"Accept": "text/html,application/xhtml+xml"})
	if err != nil {
		return nil, err
	}
	bulletins, err := parseBulletins(strings.NewReader(string(buf)), c.endpoint)
	if err != nil {
		return nil, err
	}
	if len(bulletins) > c.maxItems {
		bulletins = bulletins[:c.maxItems]
	}
	return eventsFromBulletins(c.endpoint, bulletins), nil
}

type bulletin struct {
	Title       string
	Description string
	Date        string
	URL         string
}

func parseBulletins(r *strings.Reader, baseURL string) ([]bulletin, error) {
	doc, err := html.Parse(r)
	if err != nil {
		return nil, err
	}
	out := []bulletin{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && hasClass(n, "link-list-content") {
			b := bulletin{
				Title:       linkText(firstDescendantWithClass(n, "link-list-title")),
				Description: linkText(firstDescendantWithClass(n, "link-list-description")),
				Date:        captionDate(firstDescendantWithClass(n, "link-list-caption")),
				URL:         absoluteURL(baseURL, firstLinkHref(firstDescendantWithClass(n, "link-list-title"))),
			}
			if b.Title != "" {
				out = append(out, b)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return out, nil
}

func eventsFromBulletins(endpoint string, rows []bulletin) []events.Event {
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		ts := parseDate(row.Date)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		props := map[string]any{
			"source_provider":     "Thai Meteorological Department",
			"source_api_endpoint": endpoint,
			"source_url":          row.URL,
			"country":             "Thailand",
			"country_code":        "TH",
			"title":               row.Title,
			"description":         row.Description,
			"date":                row.Date,
			"hazard_type":         hazardType(row.Title + " " + row.Description),
			"severity":            severity(row.Title + " " + row.Description),
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s", row.Date, row.Title, row.URL)),
			Lat:    15.8700,
			Lon:    100.9925,
			Props:  props,
		})
	}
	return out
}

func firstDescendantWithClass(n *html.Node, class string) *html.Node {
	if n == nil {
		return nil
	}
	if n.Type == html.ElementNode && hasClass(n, class) {
		return n
	}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if found := firstDescendantWithClass(child, class); found != nil {
			return found
		}
	}
	return nil
}

func hasClass(n *html.Node, class string) bool {
	for _, attr := range n.Attr {
		if attr.Key != "class" {
			continue
		}
		for _, part := range strings.Fields(attr.Val) {
			if part == class {
				return true
			}
		}
	}
	return false
}

func linkText(n *html.Node) string {
	if n == nil {
		return ""
	}
	return normalize(textContent(n))
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

func captionDate(n *html.Node) string {
	text := normalize(textContent(n))
	if text == "" {
		return ""
	}
	parts := strings.Split(text, "Date:")
	if len(parts) < 2 {
		return ""
	}
	rest := strings.TrimSpace(parts[1])
	if idx := strings.Index(rest, "|"); idx >= 0 {
		rest = rest[:idx]
	}
	return normalize(rest)
}

func absoluteURL(baseURL, href string) string {
	href = strings.TrimSpace(href)
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

func parseDate(raw string) time.Time {
	loc := time.FixedZone("ICT", 7*3600)
	if t, err := time.ParseInLocation("2 January 2006", strings.TrimSpace(raw), loc); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func hazardType(text string) string {
	l := strings.ToLower(text)
	switch {
	case strings.Contains(l, "rain") || strings.Contains(l, "flood"):
		return "heavy_rain"
	case strings.Contains(l, "storm") || strings.Contains(l, "cyclone"):
		return "storm"
	case strings.Contains(l, "wind") || strings.Contains(l, "wave"):
		return "marine_weather"
	default:
		return "weather"
	}
}

func severity(text string) string {
	l := strings.ToLower(text)
	switch {
	case strings.Contains(l, "very heavy") || strings.Contains(l, "severe"):
		return "high"
	case strings.Contains(l, "heavy") || strings.Contains(l, "strong"):
		return "moderate"
	default:
		return "unknown"
	}
}

func normalize(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
