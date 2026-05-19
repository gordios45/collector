// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// PeeringDB facility inventory collector.
//
// PeeringDB is static/slow-change network infrastructure context. The
// collector writes facilities into the features table so network observations
// can be spatially interpreted near carrier hotels and exchanges without
// letting the inventory seed incidents by itself.
package peeringdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultAPIURL = "https://www.peeringdb.com/api/fac"
	featureSource = "peeringdb_facilities"
)

type Collector struct {
	pool     *pgxpool.Pool
	endpoint string
	limit    int
	client   *http.Client
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	endpoint := strings.TrimSpace(os.Getenv("PEERINGDB_API_URL"))
	if endpoint == "" {
		endpoint = defaultAPIURL
	}
	limit := envInt("PEERINGDB_FAC_LIMIT", 5000)
	if limit <= 0 {
		limit = 5000
	}
	return &Collector{
		pool:     pool,
		endpoint: endpoint,
		limit:    limit,
		client: &http.Client{
			Timeout: 45 * time.Second,
			Transport: &http.Transport{
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 20 * time.Second,
			},
		},
	}, nil
}

func (c *Collector) ID() string               { return "peeringdb" }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

type response struct {
	Data []facility `json:"data"`
}

type facility struct {
	ID        int      `json:"id"`
	Name      string   `json:"name"`
	OrgID     int      `json:"org_id"`
	OrgName   string   `json:"org_name"`
	City      string   `json:"city"`
	State     string   `json:"state"`
	Country   string   `json:"country"`
	Status    string   `json:"status"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	NetCount  int      `json:"net_count"`
	IXCount   int      `json:"ix_count"`
	Updated   string   `json:"updated"`
	Created   string   `json:"created"`
	Website   string   `json:"website"`
	Address1  string   `json:"address1"`
	Address2  string   `json:"address2"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	u := c.endpoint
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	u = fmt.Sprintf("%s%slimit=%d", u, sep, c.limit)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios/0.1")

	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if r.StatusCode != http.StatusOK {
		if r.StatusCode == http.StatusTooManyRequests {
			// PeeringDB's unauthenticated free quota is shared and slow to reset.
			// Keep the static facility layer usable from the last successful run
			// instead of marking the collector unhealthy for an upstream throttle.
			return nil, nil
		}
		return nil, fmt.Errorf("peeringdb %d: %s", r.StatusCode, string(body[:min(len(body), 400)]))
	}

	feats, err := parseFacilities(body)
	if err != nil {
		return nil, err
	}
	if _, err := features.Upsert(ctx, c.pool, featureSource, feats); err != nil {
		return nil, err
	}
	return nil, nil
}

func parseFacilities(body []byte) ([]features.Feature, error) {
	var raw response
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse peeringdb facilities: %w", err)
	}
	out := make([]features.Feature, 0, len(raw.Data))
	for _, f := range raw.Data {
		if f.ID == 0 || f.Latitude == nil || f.Longitude == nil {
			continue
		}
		lat, lon := *f.Latitude, *f.Longitude
		if !validLatLon(lat, lon) {
			continue
		}
		props := map[string]any{
			"source_provider":    "peeringdb",
			"source_kind":        "network_facility_inventory",
			"facility_id":        f.ID,
			"name":               strings.TrimSpace(f.Name),
			"org_id":             f.OrgID,
			"org_name":           strings.TrimSpace(f.OrgName),
			"city":               strings.TrimSpace(f.City),
			"state":              strings.TrimSpace(f.State),
			"country":            strings.TrimSpace(f.Country),
			"status":             strings.TrimSpace(f.Status),
			"net_count":          f.NetCount,
			"ix_count":           f.IXCount,
			"updated":            strings.TrimSpace(f.Updated),
			"created":            strings.TrimSpace(f.Created),
			"abi_context_class":  "network_interconnection_site",
			"network_rank_score": networkRankScore(f.NetCount, f.IXCount),
		}
		propx.SetNonEmpty(props, "website", f.Website)
		propx.SetNonEmpty(props, "address1", f.Address1)
		propx.SetNonEmpty(props, "address2", f.Address2)
		out = append(out, features.Feature{
			ExtID:   fmt.Sprintf("fac:%d", f.ID),
			GeomWKT: fmt.Sprintf("POINT(%f %f)", lon, lat),
			Props:   props,
		})
	}
	return out, nil
}

func networkRankScore(netCount, ixCount int) float64 {
	score := float64(netCount)/250.0 + float64(ixCount)/50.0
	if score > 3 {
		return 3
	}
	if score < 0 {
		return 0
	}
	return score
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

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
