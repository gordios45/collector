// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// BGP visibility collector — RIPEstat country-asns.
//
// For each watchlist country, polls:
//
//	https://stat.ripe.net/data/country-asns/data.json?resource=XX&lod=1
//
// and records the (registered_asns, routed_asns) pair. A sudden drop in
// routed_asns/registered_asns → country-scale internet isolation event
// (Iran blackout story in the Epic Fury video).
//
// Low volume: one row per country per tick. Default watchlist is the
// 2024–26 crisis-zone set; override with BGP_WATCHLIST=IR,IL,LB,UA,…
package bgp_visibility

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/actors"
	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"
)

var defaultList = []string{"IR", "IL", "LB", "UA", "RU", "SY", "IQ", "YE", "SD", "MM", "CN", "KP"}

type Collector struct {
	countries []string
}

func New() (*Collector, error) {
	list := defaultList
	if env := strings.TrimSpace(os.Getenv("BGP_WATCHLIST")); env != "" {
		list = nil
		for _, c := range strings.Split(env, ",") {
			if c = strings.TrimSpace(strings.ToUpper(c)); c != "" {
				list = append(list, c)
			}
		}
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("BGP_WATCHLIST empty")
	}
	return &Collector{countries: list}, nil
}

func (c *Collector) ID() string               { return "bgp_visibility" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type resp struct {
	Data struct {
		Countries []struct {
			Stats struct {
				Registered int `json:"registered"`
				Routed     int `json:"routed"`
			} `json:"stats"`
			Resource string `json:"resource"`
		} `json:"countries"`
	} `json:"data"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(c.countries))
	for _, cc := range c.countries {
		u := "https://stat.ripe.net/data/country-asns/data.json?resource=" + cc + "&lod=1"
		var r resp
		if err := httpx.GetJSON(ctx, u, nil, &r); err != nil {
			continue // single country failure shouldn't kill the tick
		}
		if len(r.Data.Countries) == 0 {
			continue
		}
		stats := r.Data.Countries[0].Stats
		if stats.Registered == 0 {
			continue
		}
		ll, ok := geo.Centroids[cc]
		if !ok {
			continue
		}
		ratio := float64(stats.Routed) / float64(stats.Registered)
		props := actors.EnrichNetworkCountryProps(map[string]any{
			"country":         cc,
			"registered":      stats.Registered,
			"routed":          stats.Routed,
			"routed_ratio":    ratio,
			"missing_asns":    stats.Registered - stats.Routed,
			"outage_severity": collectorutil.BGPOutageSeverity(ratio, stats.Registered-stats.Routed),
		}, cc)
		out = append(out, events.Event{
			Ts:     now,
			Source: "bgp_visibility",
			ExtID:  cc,
			Lat:    ll.Lat, Lon: ll.Lon,
			Props: props,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("RIPEstat returned no usable data for any watchlist country")
	}
	return out, nil
}
