// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package internetdb_exposure queries Shodan InternetDB for a bounded list of
// configured public IPs. It never sweeps subnets or probes targets directly.
package internetdb_exposure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	internetDBBase = "https://internetdb.shodan.io/"
	ipAPIBase      = "http://ip-api.com/json/"
)

var defaultTargets = []string{"1.1.1.1", "8.8.8.8", "9.9.9.9"}

var nonInternetPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

type Collector struct {
	targets []netip.Addr
	client  *http.Client
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_INTERNETDB_EXPOSURE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_INTERNETDB_EXPOSURE=1")
	}
	targets, err := parseTargets(os.Getenv("INTERNETDB_TARGETS"))
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		targets, _ = parseTargets(strings.Join(defaultTargets, ","))
	}
	return &Collector{
		targets: targets,
		client:  &http.Client{Timeout: 12 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "internetdb_exposure" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	out := make([]events.Event, 0, len(c.targets))
	var lastErr error
	for _, ip := range c.targets {
		ev, ok, err := c.fetchIP(ctx, ip, now)
		if err != nil {
			lastErr = err
			continue
		}
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

func (c *Collector) fetchIP(ctx context.Context, ip netip.Addr, now time.Time) (events.Event, bool, error) {
	target := ip.String()
	raw, err := c.fetchInternetDB(ctx, target)
	if err != nil {
		return events.Event{}, false, err
	}
	if raw == nil {
		return events.Event{}, false, nil
	}
	if raw.IP == "" {
		raw.IP = target
	}
	geo := c.fetchGeo(ctx, target)
	props := propsFromInternetDB(*raw, geo)
	props["source_api_endpoint"] = internetDBBase + target
	props["source_provider"] = "Shodan InternetDB"
	props["observed_at"] = now.Format(time.RFC3339)

	return events.Event{
		Ts:     now,
		Source: "internetdb_exposure",
		ExtID:  target,
		Lat:    geo.Lat,
		Lon:    geo.Lon,
		Props:  props,
	}, true, nil
}

func (c *Collector) fetchInternetDB(ctx context.Context, ip string) (*internetDBRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, internetDBBase+ip, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return nil, fmt.Errorf("internetdb %s status %d: %s", ip, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var raw internetDBRecord
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return &raw, nil
}

func (c *Collector) fetchGeo(ctx context.Context, ip string) ipGeo {
	var raw ipGeo
	url := ipAPIBase + ip + "?fields=status,message,country,countryCode,regionName,city,lat,lon,isp,org,as,asname,proxy,hosting,query"
	if err := httpx.GetJSONWithClient(ctx, c.client, url, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return ipGeo{}
	}
	if !strings.EqualFold(raw.Status, "success") {
		return ipGeo{}
	}
	return raw
}

type internetDBRecord struct {
	CPES      []string `json:"cpes"`
	Hostnames []string `json:"hostnames"`
	IP        string   `json:"ip"`
	Ports     []int    `json:"ports"`
	Tags      []string `json:"tags"`
	Vulns     []string `json:"vulns"`
}

type ipGeo struct {
	Status      string  `json:"status"`
	Country     string  `json:"country"`
	CountryCode string  `json:"countryCode"`
	Region      string  `json:"regionName"`
	City        string  `json:"city"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	ISP         string  `json:"isp"`
	Org         string  `json:"org"`
	AS          string  `json:"as"`
	ASName      string  `json:"asname"`
	Proxy       bool    `json:"proxy"`
	Hosting     bool    `json:"hosting"`
	Query       string  `json:"query"`
}

func propsFromInternetDB(raw internetDBRecord, geo ipGeo) map[string]any {
	ports := append([]int{}, raw.Ports...)
	slices.Sort(ports)
	props := map[string]any{
		"ip":              raw.IP,
		"ports":           ports,
		"hostnames":       raw.Hostnames,
		"cpes":            raw.CPES,
		"tags":            raw.Tags,
		"vulns":           raw.Vulns,
		"vuln_count":      len(raw.Vulns),
		"open_port_count": len(raw.Ports),
		"device_type":     classifyDevice(ports, raw.CPES, raw.Tags),
		"risk_level":      riskLevel(ports, raw.Vulns),
	}
	if geo.CountryCode != "" {
		props["country"] = geo.CountryCode
		props["country_name"] = geo.Country
		props["region"] = geo.Region
		props["city"] = geo.City
		props["isp"] = geo.ISP
		props["org"] = geo.Org
		props["as"] = geo.AS
		props["as_name"] = geo.ASName
		props["is_proxy"] = geo.Proxy
		props["is_hosting"] = geo.Hosting
	}
	return props
}

func parseTargets(raw string) ([]netip.Addr, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	out := []netip.Addr{}
	seen := map[netip.Addr]bool{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "/") {
			return nil, fmt.Errorf("INTERNETDB_TARGETS only accepts individual public IPs, not CIDRs: %s", part)
		}
		ip, err := netip.ParseAddr(part)
		if err != nil {
			return nil, fmt.Errorf("bad INTERNETDB_TARGETS IP %q: %w", part, err)
		}
		if !isPublicIP(ip) {
			return nil, fmt.Errorf("INTERNETDB_TARGETS contains private/reserved IP: %s", ip)
		}
		if !seen[ip] {
			seen[ip] = true
			out = append(out, ip)
		}
	}
	maxTargets := envInt("INTERNETDB_MAX_TARGETS", 32)
	if maxTargets < 1 {
		maxTargets = 1
	}
	if len(out) > maxTargets {
		out = out[:maxTargets]
	}
	return out, nil
}

func isPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range nonInternetPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func classifyDevice(ports []int, cpes, tags []string) string {
	hasPort := func(p int) bool {
		_, ok := slices.BinarySearch(ports, p)
		return ok
	}
	contains := func(xs []string, needles ...string) bool {
		for _, x := range xs {
			lower := strings.ToLower(x)
			for _, needle := range needles {
				if strings.Contains(lower, needle) {
					return true
				}
			}
		}
		return false
	}
	switch {
	case hasPort(554) || hasPort(8554) || contains(cpes, "camera", "dvr", "hikvision", "dahua", "axis"):
		return "camera_or_dvr"
	case hasPort(9100) || contains(cpes, "printer", "laserjet", "epson", "brother"):
		return "printer"
	case hasPort(1883) || hasPort(8883) || contains(tags, "iot"):
		return "iot"
	case hasPort(5060) || hasPort(5061):
		return "voip"
	case hasPort(161) || hasPort(8291) || contains(cpes, "mikrotik", "ubiquiti", "cisco", "juniper", "fortinet"):
		return "router_or_switch"
	case hasPort(3306) || hasPort(5432) || hasPort(27017) || hasPort(6379) || hasPort(9200):
		return "database"
	case hasPort(25) || hasPort(587) || hasPort(993) || hasPort(995) || hasPort(110) || hasPort(143):
		return "mail_server"
	case hasPort(53):
		return "dns_server"
	case hasPort(3389):
		return "windows_rdp"
	case hasPort(22) && !hasPort(80) && !hasPort(443):
		return "ssh_server"
	case hasPort(80) || hasPort(443) || hasPort(8080) || hasPort(8443):
		return "web_server"
	default:
		return "unknown"
	}
}

func riskLevel(ports []int, vulns []string) string {
	hasPort := func(p int) bool {
		_, ok := slices.BinarySearch(ports, p)
		return ok
	}
	switch {
	case len(vulns) > 5:
		return "critical"
	case len(vulns) > 0:
		return "high"
	case hasPort(23) || hasPort(21) || hasPort(161):
		return "medium"
	case len(ports) > 5:
		return "low"
	default:
		return "info"
	}
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}
