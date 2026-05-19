// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package worldpop_exposure estimates population exposure using no-token public
// population/context sources.
package worldpop_exposure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	statsURL = "https://api.worldpop.org/v1/services/stats"
	tasksURL = "https://api.worldpop.org/v1/tasks/"
)

var coreExposureRadiiKM = []float64{1, 5, 25}

type Collector struct {
	pool   *pgxpool.Pool
	client *http.Client
	maxAOI int
	year   int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_WORLDPOP_EXPOSURE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_WORLDPOP_EXPOSURE=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	return &Collector{
		pool:   pool,
		client: &http.Client{Timeout: 25 * time.Second},
		maxAOI: envInt("WORLDPOP_MAX_AOIS", 4, 1, 30),
		year:   envInt("WORLDPOP_YEAR", 2020, 2000, 2020),
	}, nil
}

func (c *Collector) ID() string               { return "worldpop_exposure" }
func (c *Collector) PollEvery() time.Duration { return 12 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois, err := watchConfiguredAOIs(ctx, c.pool, c.ID(), c.maxAOI)
	if err != nil {
		return nil, err
	}
	out := []events.Event{}
	for _, a := range aois {
		samples := map[float64]populationSample{}
		for _, radius := range sampleRadiiForAOI(a) {
			pop, status, err := c.population(ctx, a, radius)
			if err != nil {
				continue
			}
			samples[radius] = populationSample{Population: pop, Status: status}
		}
		if len(samples) == 0 {
			continue
		}
		m := deriveExposureMetrics(samples, a.RadiusKM)
		props := map[string]any{
			"watch_aoi_id":                           a.ID,
			"watch_aoi_kind":                         a.Kind,
			"watch_aoi_label":                        a.Label,
			"watch_aoi_priority":                     a.Priority,
			"population_year":                        c.year,
			"radius_km":                              a.RadiusKM,
			"total_population":                       math.Round(m.TotalPopulation),
			"population_1km":                         math.Round(m.Population1KM),
			"population_5km":                         math.Round(m.Population5KM),
			"population_25km":                        math.Round(m.Population25KM),
			"population_1km_log":                     round3(populationLog10(m.Population1KM)),
			"population_5km_log":                     round3(populationLog10(m.Population5KM)),
			"population_25km_log":                    round3(populationLog10(m.Population25KM)),
			"density_1km_per_km2":                    round3(populationDensity(m.Population1KM, 1)),
			"density_5km_per_km2":                    round3(populationDensity(m.Population5KM, 5)),
			"density_25km_per_km2":                   round3(populationDensity(m.Population25KM, 25)),
			"settlement_presence_score":              round3(m.SettlementPresenceScore),
			"impact_prior_score":                     round3(m.ImpactPriorScore),
			"low_population_context_score":           round3(m.LowPopulationContextScore),
			"low_population_weak_signal_suppression": round3(m.LowPopulationContextScore),
			"exposure_status":                        m.Status,
			"source_api_endpoint":                    statsURL,
			"integration_state":                      "sampled",
			"source_priority":                        "worldpop_numeric_exposure",
			"source_dataset":                         "wpgppop",
		}
		out = append(out, events.Event{
			Ts:     time.Now().UTC(),
			Source: "worldpop_exposure",
			ExtID:  fmt.Sprintf("%s:%d", a.ID, c.year),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props:  props,
		})
	}
	return out, nil
}

type populationSample struct {
	Population float64
	Status     string
}

type exposureMetrics struct {
	Population1KM             float64
	Population5KM             float64
	Population25KM            float64
	TotalPopulation           float64
	Status                    string
	SettlementPresenceScore   float64
	ImpactPriorScore          float64
	LowPopulationContextScore float64
}

func sampleRadiiForAOI(a aoi) []float64 {
	seen := map[int]struct{}{}
	out := make([]float64, 0, len(coreExposureRadiiKM)+1)
	add := func(r float64) {
		if r <= 0 {
			return
		}
		key := int(math.Round(r * 1000))
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	for _, r := range coreExposureRadiiKM {
		add(r)
	}
	add(a.RadiusKM)
	return out
}

func deriveExposureMetrics(samples map[float64]populationSample, totalRadiusKM float64) exposureMetrics {
	popAt := func(radius float64) (float64, string) {
		if s, ok := samples[radius]; ok {
			return s.Population, s.Status
		}
		key := int(math.Round(radius * 1000))
		for r, s := range samples {
			if int(math.Round(r*1000)) == key {
				return s.Population, s.Status
			}
		}
		return 0, ""
	}
	pop1, st1 := popAt(1)
	pop5, st5 := popAt(5)
	pop25, st25 := popAt(25)
	total, stTotal := popAt(totalRadiusKM)
	if total <= 0 {
		total = pop5
		if totalRadiusKM > 10 {
			total = pop25
		}
	}
	status := propx.FirstNonEmpty(stTotal, st5, st25, st1, "sampled")
	settlement := settlementScoreFromPopulation(pop1, pop5)
	return exposureMetrics{
		Population1KM:             pop1,
		Population5KM:             pop5,
		Population25KM:            pop25,
		TotalPopulation:           total,
		Status:                    status,
		SettlementPresenceScore:   settlement,
		ImpactPriorScore:          impactPriorScore(pop25, settlement),
		LowPopulationContextScore: lowPopulationContextScore(pop5, settlement),
	}
}

func populationLog10(pop float64) float64 {
	if pop <= 0 {
		return 0
	}
	return math.Log10(1 + pop)
}

func populationDensity(pop, radiusKM float64) float64 {
	if pop <= 0 || radiusKM <= 0 {
		return 0
	}
	return pop / (math.Pi * radiusKM * radiusKM)
}

func settlementScoreFromPopulation(pop1, pop5 float64) float64 {
	d1 := populationDensity(pop1, 1)
	d5 := populationDensity(pop5, 5)
	score := 0.0
	if d1 > 0 {
		score = math.Max(score, math.Log1p(d1)/math.Log(5000))
	}
	if d5 > 0 {
		score = math.Max(score, math.Log1p(d5)/math.Log(1500))
	}
	return propx.ClampFloat(score*1.5, 0, 1.5)
}

func settlementScoreFromBuildings(buildingCount float64) float64 {
	if buildingCount <= 0 {
		return 0
	}
	return propx.ClampFloat(math.Log1p(buildingCount)/math.Log(250)*1.5, 0, 1.5)
}

func impactPriorScore(pop25, settlement float64) float64 {
	score := populationLog10(pop25)/4.5 + settlement*0.45
	return propx.ClampFloat(score, 0, 3)
}

func lowPopulationContextScore(pop5, settlement float64) float64 {
	if pop5 <= 0 && settlement <= 0 {
		return 1
	}
	if pop5 >= 1000 || settlement >= 0.45 {
		return 0
	}
	return propx.ClampFloat((1000-pop5)/1000, 0, 1)
}

type aoi struct {
	ID       string
	Kind     string
	Label    string
	Priority float64
	Lat      float64
	Lon      float64
	RadiusKM float64
}

func watchConfiguredAOIs(ctx context.Context, pool *pgxpool.Pool, collectorID string, maxAOI int) ([]aoi, error) {
	cfg := collectorutil.SelectAOIsForCollector(ctx, pool, collectorID, maxAOI, 7*24*time.Hour, collectorutil.StrategicAOIs)
	out := make([]aoi, 0, len(cfg))
	for _, a := range cfg {
		radiusKM := a.RadiusM / 1000
		if radiusKM <= 0 {
			radiusKM = 25
		}
		out = append(out, aoi{
			ID:       a.ID,
			Kind:     a.Kind,
			Label:    a.Label,
			Priority: a.Priority,
			Lat:      a.Lat,
			Lon:      a.Lon,
			RadiusKM: radiusKM,
		})
	}
	return out, nil
}

func (c *Collector) population(ctx context.Context, a aoi, radiusKM float64) (float64, string, error) {
	geoJSON, err := geoJSONCircle(a.Lat, a.Lon, radiusKM, 48)
	if err != nil {
		return 0, "", err
	}
	q := url.Values{}
	q.Set("dataset", "wpgppop")
	q.Set("year", strconv.Itoa(c.year))
	q.Set("geojson", geoJSON)
	q.Set("runasync", "false")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, statsURL+"?"+q.Encode(), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios-worldpop/0.1")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	var body worldpopResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, "", err
	}
	if body.Error {
		return 0, body.Status, errors.New(body.ErrorMessage)
	}
	if body.Data.TotalPopulation > 0 {
		return body.Data.TotalPopulation, body.Status, nil
	}
	if body.TaskID == "" {
		return 0, body.Status, errors.New("worldpop returned no total_population")
	}
	for i := 0; i < 4; i++ {
		select {
		case <-ctx.Done():
			return 0, "", ctx.Err()
		case <-time.After(time.Duration(i+1) * time.Second):
		}
		pop, status, ok := c.pollTask(ctx, body.TaskID)
		if ok {
			return pop, status, nil
		}
	}
	return 0, body.Status, errors.New("worldpop task pending")
}

func (c *Collector) pollTask(ctx context.Context, taskID string) (float64, string, bool) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, tasksURL+url.PathEscape(taskID), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios-worldpop/0.1")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, "", false
	}
	defer resp.Body.Close()
	var body worldpopResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, "", false
	}
	if body.Status == "finished" && body.Data.TotalPopulation > 0 {
		return body.Data.TotalPopulation, body.Status, true
	}
	return 0, body.Status, false
}

type worldpopResp struct {
	Status       string `json:"status"`
	StatusCode   int    `json:"status_code"`
	Error        bool   `json:"error"`
	ErrorMessage string `json:"error_message"`
	TaskID       string `json:"taskid"`
	Data         struct {
		TotalPopulation float64 `json:"total_population"`
	} `json:"data"`
}

func geoJSONCircle(lat, lon, radiusKM float64, steps int) (string, error) {
	if !validLatLon(lat, lon) || radiusKM <= 0 {
		return "", errors.New("invalid AOI")
	}
	if steps < 12 {
		steps = 12
	}
	coords := make([][]float64, 0, steps+1)
	latRad := lat * math.Pi / 180
	for i := 0; i <= steps; i++ {
		bearing := 2 * math.Pi * float64(i) / float64(steps)
		dLat := (radiusKM / 111.32) * math.Cos(bearing)
		dLon := (radiusKM / (111.32 * math.Cos(latRad))) * math.Sin(bearing)
		coords = append(coords, []float64{lon + dLon, lat + dLat})
	}
	fc := map[string]any{
		"type": "FeatureCollection",
		"features": []map[string]any{{
			"type":       "Feature",
			"properties": map[string]any{},
			"geometry": map[string]any{
				"type":        "Polygon",
				"coordinates": []any{coords},
			},
		}},
	}
	raw, err := json.Marshal(fc)
	return string(raw), err
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func envInt(name string, def, min, max int) int {
	if s := strings.TrimSpace(os.Getenv(name)); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			if n < min {
				return min
			}
			if n > max {
				return max
			}
			return n
		}
	}
	return def
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}
