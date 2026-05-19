// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package worldpop_exposure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"

	"github.com/jackc/pgx/v5/pgxpool"
)

const overpassInterpreterURL = "https://overpass-api.de/api/interpreter"

type OSMSettlementCollector struct {
	pool   *pgxpool.Pool
	client *http.Client
	maxAOI int
}

func NewOSMSettlementContext(pool *pgxpool.Pool) (*OSMSettlementCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_OSM_SETTLEMENT_CONTEXT") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_OSM_SETTLEMENT_CONTEXT=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	return &OSMSettlementCollector{
		pool:   pool,
		client: &http.Client{Timeout: 35 * time.Second},
		maxAOI: envInt("OSM_SETTLEMENT_MAX_AOIS", 3, 1, 10),
	}, nil
}

func (c *OSMSettlementCollector) ID() string               { return "osm_settlement_context" }
func (c *OSMSettlementCollector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *OSMSettlementCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois, err := watchConfiguredAOIs(ctx, c.pool, c.ID(), c.maxAOI)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := []events.Event{}
	for _, a := range aois {
		count, err := c.buildingCount(ctx, a.Lat, a.Lon, 1000)
		if err != nil {
			continue
		}
		score := settlementScoreFromBuildings(float64(count))
		props := map[string]any{
			"watch_aoi_id":              a.ID,
			"watch_aoi_kind":            a.Kind,
			"watch_aoi_label":           a.Label,
			"watch_aoi_priority":        a.Priority,
			"building_count_1km":        count,
			"radius_m":                  1000,
			"settlement_presence_score": round3(score),
			"source_api_endpoint":       overpassInterpreterURL,
			"source_dataset":            "OpenStreetMap buildings via Overpass",
			"integration_state":         "sampled",
		}
		out = append(out, events.Event{
			Ts:     now,
			Source: "osm_settlement_context",
			ExtID:  fmt.Sprintf("%s:%s", a.ID, now.Format("20060102")),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props:  props,
		})
	}
	return out, nil
}

func (c *OSMSettlementCollector) buildingCount(ctx context.Context, lat, lon float64, radiusM int) (int, error) {
	if !validLatLon(lat, lon) {
		return 0, errors.New("invalid lat/lon")
	}
	q := fmt.Sprintf(`[out:json][timeout:25];
(
  way["building"](around:%d,%.6f,%.6f);
  relation["building"](around:%d,%.6f,%.6f);
);
out count;`, radiusM, lat, lon, radiusM, lat, lon)
	form := url.Values{}
	form.Set("data", q)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, overpassInterpreterURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios-osm-settlement-context/0.1")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("overpass status %d", resp.StatusCode)
	}
	var body overpassCountResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	for _, el := range body.Elements {
		if strings.EqualFold(el.Type, "count") {
			if total := atoi(el.Tags.Total); total > 0 {
				return total, nil
			}
			return atoi(el.Tags.Ways) + atoi(el.Tags.Relations), nil
		}
	}
	return 0, nil
}

type overpassCountResp struct {
	Elements []struct {
		Type string `json:"type"`
		Tags struct {
			Total     string `json:"total"`
			Ways      string `json:"ways"`
			Relations string `json:"relations"`
		} `json:"tags"`
	} `json:"elements"`
}

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}
