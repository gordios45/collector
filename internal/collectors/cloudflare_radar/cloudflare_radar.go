// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Cloudflare Radar collector — internet outages + DDoS attack origins.
//
// Two REST calls per tick:
//
//	GET /radar/annotations/outages?limit=100&dateRange=30d
//	    → country-level outage annotations (submarine cable cuts, state
//	      shutdowns, carrier failures).
//	GET /radar/attacks/layer3/top/locations/origin?dateRange=7d&limit=20
//	    → top origin countries for L3 DDoS by traffic share.
//
// Both are rendered as country-centroid points (no sub-country geo).
// Gated by CF_RADAR_TOKEN (Cloudflare API token with Radar:Read).
package cloudflare_radar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
)

const (
	outagesURL = "https://api.cloudflare.com/client/v4/radar/annotations/outages?limit=100&dateRange=30d"
	attacksURL = "https://api.cloudflare.com/client/v4/radar/attacks/layer3/top/locations/origin?dateRange=7d&limit=20"
)

type Collector struct {
	token  string
	client *http.Client
}

func New() (*Collector, error) {
	tok := strings.TrimSpace(os.Getenv("CF_RADAR_TOKEN"))
	if tok == "" {
		return nil, fmt.Errorf("CF_RADAR_TOKEN not set")
	}
	return &Collector{token: tok, client: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (c *Collector) ID() string               { return "cloudflare_radar" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	outages, err1 := c.fetchOutages(ctx)
	attacks, err2 := c.fetchAttacks(ctx)
	// Partial success is fine — one endpoint failing shouldn't zero the tick.
	if err1 != nil && err2 != nil {
		return nil, fmt.Errorf("both endpoints failed: outages=%v attacks=%v", err1, err2)
	}
	out := make([]events.Event, 0, len(outages)+len(attacks))
	out = append(out, outages...)
	out = append(out, attacks...)
	return out, nil
}

// ---- outages ----

type outagesResp struct {
	Result struct {
		Annotations []outage `json:"annotations"`
	} `json:"result"`
}

type outage struct {
	ID               string   `json:"id"`
	EventType        string   `json:"eventType"`
	StartDate        string   `json:"startDate"`
	EndDate          string   `json:"endDate"`
	Locations        []string `json:"locations"`
	LocationsDetails []struct {
		Code string `json:"code"`
		Name string `json:"name"`
	} `json:"locationsDetails"`
	Outage struct {
		OutageCause string `json:"outageCause"`
		OutageType  string `json:"outageType"`
	} `json:"outage"`
}

func (c *Collector) fetchOutages(ctx context.Context) ([]events.Event, error) {
	var raw outagesResp
	if err := c.get(ctx, outagesURL, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.Result.Annotations))
	for _, o := range raw.Result.Annotations {
		codes := o.Locations
		if len(codes) == 0 {
			for _, d := range o.LocationsDetails {
				if d.Code != "" {
					codes = append(codes, d.Code)
				}
			}
		}
		for _, code := range codes {
			ll, ok := geo.Centroids[strings.ToUpper(code)]
			if !ok {
				continue
			}
			ts := now
			if o.StartDate != "" {
				if t, err := time.Parse(time.RFC3339, o.StartDate); err == nil {
					ts = t
				}
			}
			props := map[string]any{
				"country":    strings.ToUpper(code),
				"cause":      o.Outage.OutageCause,
				"type":       o.Outage.OutageType,
				"event_type": o.EventType,
				"start":      o.StartDate,
				"end":        o.EndDate,
			}
			out = append(out, events.Event{
				Ts:     ts,
				Source: "cloudflare_radar",
				ExtID:  fmt.Sprintf("outage_%s_%s", o.ID, strings.ToUpper(code)),
				Lat:    ll.Lat, Lon: ll.Lon,
				Props: props,
			})
		}
	}
	return out, nil
}

// ---- attacks (DDoS L3 top origins) ----

type attacksResp struct {
	Result struct {
		Top0 []attack `json:"top_0"`
	} `json:"result"`
}

type attack struct {
	ClientCountryAlpha2 string `json:"clientCountryAlpha2"`
	OriginCountryAlpha2 string `json:"originCountryAlpha2"`
	Value               string `json:"value"`
	Rank                int    `json:"rank"`
}

func (c *Collector) fetchAttacks(ctx context.Context) ([]events.Event, error) {
	var raw attacksResp
	if err := c.get(ctx, attacksURL, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.Result.Top0))
	for i, a := range raw.Result.Top0 {
		code := a.OriginCountryAlpha2
		if code == "" {
			code = a.ClientCountryAlpha2
		}
		ll, ok := geo.Centroids[strings.ToUpper(code)]
		if !ok {
			continue
		}
		rank := a.Rank
		if rank == 0 {
			rank = i + 1 // API sometimes omits rank; fall back to array index
		}
		props := map[string]any{
			"country": strings.ToUpper(code),
			"share":   a.Value,
			"rank":    rank,
		}
		out = append(out, events.Event{
			Ts:     now,
			Source: "cloudflare_radar",
			ExtID:  fmt.Sprintf("ddos_%s", strings.ToUpper(code)),
			Lat:    ll.Lat, Lon: ll.Lon,
			Props: props,
		})
	}
	return out, nil
}

// ---- HTTP helper with Bearer auth ----

func (c *Collector) get(ctx context.Context, url string, into any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios/0.1")

	r, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 512))
		return fmt.Errorf("cloudflare %d: %s", r.StatusCode, string(body))
	}
	return json.NewDecoder(r.Body).Decode(into)
}
