// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package tor_exit_nodes ingests the current Tor bulk exit-node list as
// non-geospatial cyber context.
package tor_exit_nodes

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://check.torproject.org/torbulkexitlist"

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
	maxNodes int
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_TOR_EXIT_NODES") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_TOR_EXIT_NODES=1")
	}
	return &Collector{maxNodes: envInt("TOR_EXIT_MAX_NODES", 5000)}, nil
}

func (c *Collector) ID() string               { return "tor_exit_nodes" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	body, err := httpx.GetBytes(ctx, endpoint, map[string]string{"Accept": "text/plain,*/*"})
	if err != nil {
		return nil, err
	}
	ips := parseExitList(body, c.maxNodes)
	now := time.Now().UTC().Truncate(24 * time.Hour)
	out := make([]events.Event, 0, len(ips))
	for _, ip := range ips {
		out = append(out, events.Event{
			Ts:     now,
			Source: "tor_exit_nodes",
			ExtID:  ip.String(),
			Props: map[string]any{
				"ip":                  ip.String(),
				"indicator":           ip.String(),
				"indicator_type":      "ip",
				"network_role":        "tor_exit_node",
				"source_provider":     "Tor Project",
				"source_api_endpoint": endpoint,
				"observed_at":         time.Now().UTC().Format(time.RFC3339),
				"source_payload_validity": map[string]any{
					"valid_start":    now.Format(time.RFC3339),
					"valid_end":      now.Add(24 * time.Hour).Format(time.RFC3339),
					"validity_basis": "tor_bulk_exit_daily_snapshot",
				},
			},
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("tor exit list contained no public IPs")
	}
	return out, nil
}

func parseExitList(body []byte, maxNodes int) []netip.Addr {
	if maxNodes <= 0 {
		maxNodes = 5000
	}
	seen := map[netip.Addr]bool{}
	out := make([]netip.Addr, 0, 1024)
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ip, err := netip.ParseAddr(line)
		if err != nil || !isPublicIP(ip) || seen[ip] {
			continue
		}
		seen[ip] = true
		out = append(out, ip)
		if len(out) >= maxNodes {
			break
		}
	}
	return out
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
