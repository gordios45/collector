// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package open_meteo_anomalies computes bounded ERA5/Open-Meteo weather
// anomaly priors over strategic ingestion-owned watch zones.
package open_meteo_anomalies

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const endpoint = "https://archive-api.open-meteo.com/v1/archive"

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_OPEN_METEO_ANOMALIES") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_OPEN_METEO_ANOMALIES=1")
	}
	return &Collector{pool: pool, maxAOIs: envInt("OPEN_METEO_MAX_AOIS", 20, 5, 80)}, nil
}

func (c *Collector) ID() string               { return "open_meteo_anomalies" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := c.watchAOIs(ctx)
	if len(aois) == 0 {
		aois = fallbackZones
	}
	if len(aois) > c.maxAOIs {
		aois = aois[:c.maxAOIs]
	}
	end := time.Now().UTC().AddDate(0, 0, -1)
	start := end.AddDate(0, 0, -44)
	out := []events.Event{}
	for _, batch := range chunkAOIs(aois, 8) {
		payloads, err := fetchBatch(ctx, batch, start, end)
		if err != nil {
			continue
		}
		for i, p := range payloads {
			if i >= len(batch) {
				continue
			}
			if ev, ok := eventForAOI(batch[i], p, start, end); ok {
				out = append(out, ev)
			}
		}
	}
	return out, nil
}

type watchAOI struct {
	ID       string
	Label    string
	Kind     string
	Lat      float64
	Lon      float64
	Priority float64
}

var fallbackZones = []watchAOI{
	{ID: "zone:ukraine", Label: "Ukraine", Kind: "strategic_zone", Lat: 48.4, Lon: 31.2, Priority: 2},
	{ID: "zone:middle_east", Label: "Middle East", Kind: "strategic_zone", Lat: 33.0, Lon: 44.0, Priority: 2},
	{ID: "zone:sahel", Label: "Sahel", Kind: "strategic_zone", Lat: 14.0, Lon: 0.0, Priority: 1.5},
	{ID: "zone:horn_africa", Label: "Horn of Africa", Kind: "strategic_zone", Lat: 8.0, Lon: 42.0, Priority: 1.5},
	{ID: "zone:south_asia", Label: "South Asia", Kind: "strategic_zone", Lat: 25.0, Lon: 78.0, Priority: 1.5},
	{ID: "zone:taiwan_strait", Label: "Taiwan Strait", Kind: "strategic_zone", Lat: 24.0, Lon: 120.0, Priority: 1.5},
	{ID: "zone:caribbean", Label: "Caribbean", Kind: "strategic_zone", Lat: 19.0, Lon: -72.0, Priority: 1.2},
	{ID: "zone:mediterranean", Label: "Mediterranean", Kind: "strategic_zone", Lat: 38.0, Lon: 20.0, Priority: 1.2},
}

func (c *Collector) watchAOIs(ctx context.Context) []watchAOI {
	out := watchAOIsFromConfigured(collectorutil.ConfiguredAOIs(ctx, c.pool, c.ID(), c.maxAOIs))
	out = append(out, fallbackZones...)
	return limitWatchAOIs(out, c.maxAOIs)
}

func watchAOIsFromConfigured(in []collectorutil.AOI) []watchAOI {
	out := make([]watchAOI, 0, len(in))
	for _, a := range in {
		out = append(out, watchAOI{
			ID:       a.ID,
			Label:    a.Label,
			Kind:     a.Kind,
			Lat:      a.Lat,
			Lon:      a.Lon,
			Priority: a.Priority,
		})
	}
	return out
}

func limitWatchAOIs(in []watchAOI, max int) []watchAOI {
	if max <= 0 || max > len(in) {
		max = len(in)
	}
	out := make([]watchAOI, 0, max)
	for _, a := range in {
		if !validLatLon(a.Lat, a.Lon) {
			continue
		}
		out = append(out, a)
		if len(out) >= max {
			break
		}
	}
	return out
}

type meteoPayload struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Daily     struct {
		Time               []string  `json:"time"`
		TemperatureMean    []float64 `json:"temperature_2m_mean"`
		PrecipitationSum   []float64 `json:"precipitation_sum"`
		WindSpeed10mMax    []float64 `json:"wind_speed_10m_max"`
		WindSpeed10mMaxOld []float64 `json:"windspeed_10m_max"`
	} `json:"daily"`
}

func fetchBatch(ctx context.Context, aois []watchAOI, start, end time.Time) ([]meteoPayload, error) {
	q := url.Values{}
	q.Set("latitude", joinFloats(aois, true))
	q.Set("longitude", joinFloats(aois, false))
	q.Set("start_date", start.Format("2006-01-02"))
	q.Set("end_date", end.Format("2006-01-02"))
	q.Set("daily", "temperature_2m_mean,precipitation_sum,wind_speed_10m_max")
	q.Set("timezone", "UTC")
	var raw []meteoPayload
	if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &raw); err == nil {
		return raw, nil
	}
	var one meteoPayload
	if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &one); err != nil {
		return nil, err
	}
	return []meteoPayload{one}, nil
}

func eventForAOI(a watchAOI, p meteoPayload, start, end time.Time) (events.Event, bool) {
	m := summarize(p)
	if !m.OK {
		return events.Event{}, false
	}
	props := map[string]any{
		"watch_aoi_id":          a.ID,
		"watch_aoi_kind":        a.Kind,
		"watch_aoi_label":       a.Label,
		"period_start":          start.Format("2006-01-02"),
		"period_end":            end.Format("2006-01-02"),
		"recent_days":           7,
		"baseline_days":         m.BaselineDays,
		"recent_temp_mean_c":    round(m.RecentTemp, 1),
		"baseline_temp_mean_c":  round(m.BaselineTemp, 1),
		"temp_delta_c":          round(m.TempDelta, 1),
		"recent_precip_mm":      round(m.RecentPrecip, 1),
		"baseline_precip_mm":    round(m.BaselinePrecip, 1),
		"precip_delta_mm":       round(m.PrecipDelta, 1),
		"recent_wind_max_kmh":   round(m.RecentWindMax, 1),
		"baseline_wind_max_kmh": round(m.BaselineWindMax, 1),
		"wind_delta_kmh":        round(m.WindDelta, 1),
		"heat_stress_score":     round(m.HeatStressScore, 2),
		"dryness_score":         round(m.DrynessScore, 2),
		"extreme_precip_score":  round(m.ExtremePrecipScore, 2),
		"wind_anomaly_score":    round(m.WindScore, 2),
		"anomaly_score":         round(m.AnomalyScore, 2),
		"severity":              m.Severity,
		"anomaly_type":          m.Type,
		"source_api_endpoint":   endpoint,
	}
	return events.Event{
		Ts:     end,
		Source: "open_meteo_anomalies",
		ExtID:  fmt.Sprintf("%s:%s", a.ID, end.Format("20060102")),
		Lat:    a.Lat,
		Lon:    a.Lon,
		Props:  props,
	}, true
}

type metric struct {
	OK                 bool
	BaselineDays       int
	RecentTemp         float64
	BaselineTemp       float64
	TempDelta          float64
	RecentPrecip       float64
	BaselinePrecip     float64
	PrecipDelta        float64
	RecentWindMax      float64
	BaselineWindMax    float64
	WindDelta          float64
	HeatStressScore    float64
	DrynessScore       float64
	ExtremePrecipScore float64
	WindScore          float64
	AnomalyScore       float64
	Severity           string
	Type               string
}

func summarize(p meteoPayload) metric {
	times := p.Daily.Time
	if len(times) < 21 {
		return metric{}
	}
	temps := align(p.Daily.TemperatureMean, len(times))
	precips := align(p.Daily.PrecipitationSum, len(times))
	winds := align(firstNonEmptyFloatSlice(p.Daily.WindSpeed10mMax, p.Daily.WindSpeed10mMaxOld), len(times))
	n := len(times)
	recentStart := maxInt(0, n-7)
	baseStart := maxInt(0, n-37)
	baseEnd := recentStart
	if baseEnd-baseStart < 10 {
		return metric{}
	}
	recentTemp := avg(temps[recentStart:n])
	baseTemp := avg(temps[baseStart:baseEnd])
	recentPrecip := sum(precips[recentStart:n])
	basePrecip := avg(rollingSums(precips[baseStart:baseEnd], 7))
	recentWind := max(winds[recentStart:n])
	baseWind := avg(winds[baseStart:baseEnd])
	tempDelta := recentTemp - baseTemp
	precipDelta := recentPrecip - basePrecip
	windDelta := recentWind - baseWind
	heat := propx.ClampFloat((tempDelta-3.0)/2.0, 0, 3)
	dry := propx.ClampFloat((-precipDelta-8.0)/8.0+math.Max(0, tempDelta)/6.0, 0, 3)
	wet := propx.ClampFloat((precipDelta-12.0)/12.0, 0, 3)
	windScore := propx.ClampFloat((windDelta-20.0)/20.0, 0, 3)
	score := math.Max(math.Max(heat, dry), math.Max(wet, windScore))
	typ := "normal"
	switch score {
	case heat:
		typ = "heat"
	case dry:
		typ = "dry"
	case wet:
		typ = "wet"
	case windScore:
		typ = "wind"
	}
	severity := "normal"
	if score >= 2 {
		severity = "extreme"
	} else if score >= 1 {
		severity = "moderate"
	}
	return metric{
		OK: true, BaselineDays: baseEnd - baseStart,
		RecentTemp: recentTemp, BaselineTemp: baseTemp, TempDelta: tempDelta,
		RecentPrecip: recentPrecip, BaselinePrecip: basePrecip, PrecipDelta: precipDelta,
		RecentWindMax: recentWind, BaselineWindMax: baseWind, WindDelta: windDelta,
		HeatStressScore: heat, DrynessScore: dry, ExtremePrecipScore: wet, WindScore: windScore,
		AnomalyScore: score, Severity: severity, Type: typ,
	}
}

func chunkAOIs(in []watchAOI, size int) [][]watchAOI {
	out := [][]watchAOI{}
	for i := 0; i < len(in); i += size {
		end := i + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

func joinFloats(aois []watchAOI, lat bool) string {
	parts := make([]string, 0, len(aois))
	for _, a := range aois {
		v := a.Lon
		if lat {
			v = a.Lat
		}
		parts = append(parts, strconv.FormatFloat(v, 'f', 5, 64))
	}
	return strings.Join(parts, ",")
}

func align(v []float64, n int) []float64 {
	if len(v) >= n {
		return v[:n]
	}
	out := make([]float64, n)
	copy(out, v)
	return out
}

func firstNonEmptyFloatSlice(xs ...[]float64) []float64 {
	for _, x := range xs {
		if len(x) > 0 {
			return x
		}
	}
	return nil
}

func rollingSums(vals []float64, width int) []float64 {
	if len(vals) < width {
		return []float64{sum(vals)}
	}
	out := []float64{}
	for i := 0; i+width <= len(vals); i++ {
		out = append(out, sum(vals[i:i+width]))
	}
	return out
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	return sum(vals) / float64(len(vals))
}

func sum(vals []float64) float64 {
	total := 0.0
	for _, v := range vals {
		total += v
	}
	return total
}

func max(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64{}, vals...)
	sort.Float64s(cp)
	return cp[len(cp)-1]
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
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
