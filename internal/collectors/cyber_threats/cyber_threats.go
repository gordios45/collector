// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Cyber threats — no-token OSINT feeds:
//   - abuse.ch Feodo Tracker botnet C2 IPs
//   - abuse.ch URLhaus recent malware URLs (IP hosts only, bounded)
//   - C2IntelFeeds 30-day C2 IP CSV
//   - Ransomware.live recent victims when its public API is available
package cyber_threats

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	feodoURL      = "https://feodotracker.abuse.ch/downloads/ipblocklist.json"
	urlhausCSVURL = "https://urlhaus.abuse.ch/downloads/csv_recent/"
	c2IntelURL    = "https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv"
	ransomwareURL = "https://api.ransomware.live/v2/recentvictims"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "cyber_threats" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	rows := []map[string]any{}
	var firstErr error

	var raw []map[string]any
	if err := httpx.GetJSON(ctx, feodoURL, nil, &raw); err != nil {
		firstErr = err
	} else {
		for _, row := range raw {
			row["feed"] = "feodo"
			row["indicator_type"] = "ip"
			rows = append(rows, row)
		}
	}
	rows = append(rows, c.fetchURLhaus(ctx)...)
	rows = append(rows, c.fetchC2Intel(ctx)...)
	rows = append(rows, c.fetchRansomware(ctx)...)
	if len(rows) == 0 && firstErr != nil {
		return nil, firstErr
	}

	hydrateGeo(ctx, rows)

	now := time.Now().UTC()
	counts := map[string]int{} // per-country counter for jitter
	out := make([]events.Event, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		ip := strings.TrimSpace(fmt.Sprint(row["ip_address"]))
		indicator := strings.TrimSpace(fmt.Sprint(row["indicator"]))
		if indicator == "" {
			indicator = ip
		}
		if indicator == "" {
			continue
		}
		country := strings.ToUpper(fmt.Sprintf("%v", row["country"]))
		var lat, lon float64
		if v, ok := propx.Float(row["latitude"]); ok {
			lat = v
		}
		if v, ok := propx.Float(row["longitude"]); ok {
			lon = v
		}
		if lat == 0 && lon == 0 {
			cc := geo.Centroids[country]
			if cc.Lat == 0 && cc.Lon == 0 {
				continue
			}
			// Tiny spiral jitter so C2s in the same country don't overlap.
			n := counts[country]
			counts[country] = n + 1
			angle := float64(n) * 0.5
			radius := 0.15 + float64(n)*0.02
			lat = cc.Lat + math.Sin(angle)*radius
			lon = cc.Lon + math.Cos(angle)*radius
		}
		feed := strings.TrimSpace(fmt.Sprint(row["feed"]))
		if feed == "" {
			feed = "feodo"
		}
		extID := feed + ":" + indicator
		if seen[extID] {
			continue
		}
		seen[extID] = true
		if _, ok := row["source_api_endpoint"]; !ok {
			row["source_api_endpoint"] = endpointForFeed(feed)
		}
		if _, ok := row["malware"]; !ok {
			row["malware"] = row["malware_family"]
		}
		if _, ok := row["status"]; !ok {
			row["status"] = row["url_status"]
		}
		if _, ok := row["first_seen"]; !ok {
			row["first_seen"] = row["dateadded"]
		}
		// Use ingestion time as `ts` so the latest-snapshot query always
		// sees every active C2 entry. The blocklist's `first_seen` date
		// stays in props for intel-panel display.
		out = append(out, events.Event{
			Ts: now, Source: "cyber_threats", ExtID: extID,
			Lat: lat, Lon: lon, Props: row,
		})
	}
	return out, nil
}

func (c *Collector) fetchURLhaus(ctx context.Context) []map[string]any {
	buf, err := httpx.GetBytes(ctx, urlhausCSVURL, map[string]string{"Accept": "text/csv,*/*"})
	if err != nil {
		return nil
	}
	r := csv.NewReader(strings.NewReader(string(buf)))
	r.FieldsPerRecord = -1
	out := []map[string]any{}
	for {
		rec, err := r.Read()
		if err == io.EOF || len(out) >= 120 {
			break
		}
		if err != nil || len(rec) < 9 || strings.HasPrefix(rec[0], "#") || rec[0] == "id" {
			continue
		}
		ip := ipFromURL(rec[2])
		if ip == "" {
			continue
		}
		out = append(out, map[string]any{
			"feed":                "urlhaus",
			"id":                  rec[0],
			"dateadded":           rec[1],
			"url":                 rec[2],
			"url_status":          rec[3],
			"last_online":         rec[4],
			"threat":              rec[5],
			"tags":                rec[6],
			"urlhaus_link":        rec[7],
			"reporter":            rec[8],
			"indicator":           rec[2],
			"indicator_type":      "url",
			"ip_address":          ip,
			"malware_family":      rec[6],
			"source_api_endpoint": urlhausCSVURL,
		})
	}
	return out
}

func (c *Collector) fetchC2Intel(ctx context.Context) []map[string]any {
	buf, err := httpx.GetBytes(ctx, c2IntelURL, map[string]string{"Accept": "text/csv,*/*"})
	if err != nil {
		return nil
	}
	r := csv.NewReader(strings.NewReader(string(buf)))
	r.FieldsPerRecord = -1
	out := []map[string]any{}
	for {
		rec, err := r.Read()
		if err == io.EOF || len(out) >= 250 {
			break
		}
		if err != nil || len(rec) < 2 || strings.HasPrefix(rec[0], "#") || rec[0] == "ip" {
			continue
		}
		ip := strings.TrimSpace(rec[0])
		if net.ParseIP(ip) == nil {
			continue
		}
		out = append(out, map[string]any{
			"feed":                "c2intelfeeds",
			"indicator":           ip,
			"indicator_type":      "ip",
			"ip_address":          ip,
			"malware":             strings.TrimSpace(rec[1]),
			"status":              "listed",
			"first_seen":          time.Now().UTC().Format(time.RFC3339),
			"source_api_endpoint": c2IntelURL,
		})
	}
	return out
}

func (c *Collector) fetchRansomware(ctx context.Context) []map[string]any {
	var raw []map[string]any
	if err := httpx.GetJSON(ctx, ransomwareURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := []map[string]any{}
	for _, v := range raw {
		country := strings.ToUpper(strings.TrimSpace(fmt.Sprint(v["country"])))
		cc := geo.Centroids[country]
		if cc.Lat == 0 && cc.Lon == 0 {
			continue
		}
		v["feed"] = "ransomware_live"
		v["indicator"] = fmt.Sprint(v["victim"])
		v["indicator_type"] = "victim"
		v["status"] = "claimed"
		v["malware"] = fmt.Sprint(v["group"])
		v["first_seen"] = fmt.Sprint(v["discovered"])
		v["source_api_endpoint"] = ransomwareURL
		out = append(out, v)
	}
	return out
}

func hydrateGeo(ctx context.Context, rows []map[string]any) {
	client := &http.Client{Timeout: 2500 * time.Millisecond}
	seen := map[string]bool{}
	resolved := 0
	for _, row := range rows {
		if resolved >= 80 {
			return
		}
		if _, ok := propx.Float(row["latitude"]); ok {
			continue
		}
		ip := strings.TrimSpace(fmt.Sprint(row["ip_address"]))
		if ip == "" || seen[ip] || net.ParseIP(ip) == nil {
			continue
		}
		seen[ip] = true
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://freeipapi.com/api/json/"+ip, nil)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var body struct {
			Latitude    float64 `json:"latitude"`
			Longitude   float64 `json:"longitude"`
			CountryCode string  `json:"countryCode"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 1<<18)).Decode(&body)
		resp.Body.Close()
		if err != nil || body.Latitude == 0 && body.Longitude == 0 {
			continue
		}
		for _, r := range rows {
			if fmt.Sprint(r["ip_address"]) == ip {
				r["latitude"] = body.Latitude
				r["longitude"] = body.Longitude
				if strings.TrimSpace(fmt.Sprint(r["country"])) == "" {
					r["country"] = strings.ToUpper(body.CountryCode)
				}
			}
		}
		resolved++
	}
}

func ipFromURL(raw string) string {
	u, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		return host
	}
	return ""
}

func endpointForFeed(feed string) string {
	switch feed {
	case "urlhaus":
		return urlhausCSVURL
	case "c2intelfeeds":
		return c2IntelURL
	case "ransomware_live":
		return ransomwareURL
	default:
		return feodoURL
	}
}
