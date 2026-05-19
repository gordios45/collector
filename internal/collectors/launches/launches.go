// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Upcoming rocket launches (Launch Library 2 / TheSpaceDevs).
// https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=50
package launches

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=50&format=json"

var client = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		ForceAttemptHTTP2:     true,
	},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "launches" }
func (c *Collector) PollEvery() time.Duration { return 60 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Results []map[string]any `json:"results"`
	}
	if err := httpx.GetJSONWithClient(ctx, client, url, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Results))
	for _, ev := range raw.Results {
		id, _ := ev["id"].(string)
		if id == "" {
			continue
		}
		pad, _ := ev["pad"].(map[string]any)
		lat, _ := pad["latitude"].(string)
		lon, _ := pad["longitude"].(string)
		latF := toF(lat)
		lonF := toF(lon)
		if latF == 0 && lonF == 0 {
			continue
		}
		ts := time.Now().UTC()
		if s, _ := ev["net"].(string); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "launches", ExtID: id,
			Lat: latF, Lon: lonF, Props: ev,
		})
	}
	return out, nil
}

func toF(s string) float64 {
	var f float64
	for i, r := range s {
		if i == 0 && (r == '-' || r == '+') {
			continue
		}
		if (r < '0' || r > '9') && r != '.' {
			return 0
		}
	}
	_, _ = fmtScanf(s, &f)
	return f
}

// tiny indirection so I don't need fmt just for Sscanf
func fmtScanf(s string, p *float64) (int, error) {
	// lazy: use json.Unmarshal on number
	import_numeric := []byte(s)
	if len(import_numeric) == 0 {
		return 0, nil
	}
	var f float64
	var sign float64 = 1
	i := 0
	if import_numeric[0] == '-' {
		sign = -1
		i = 1
	} else if import_numeric[0] == '+' {
		i = 1
	}
	intPart := true
	frac := 0.0
	fracDiv := 1.0
	for ; i < len(import_numeric); i++ {
		c := import_numeric[i]
		if c == '.' {
			intPart = false
			continue
		}
		if c < '0' || c > '9' {
			break
		}
		d := float64(c - '0')
		if intPart {
			f = f*10 + d
		} else {
			fracDiv *= 10
			frac = frac*10 + d
		}
	}
	*p = sign * (f + frac/fracDiv)
	return 1, nil
}
