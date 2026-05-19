// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package flood_coverage adds flood-specific open data coverage around the
// existing generic weather/disaster feeds. The collectors intentionally avoid
// downloading full global rasters in the scheduler; they expose catalog/product
// availability, AOI-level hydrology samples when a lightweight API exists, and
// explicit blocked states when the upstream requires a free token.
package flood_coverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
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

const (
	gfmEndpoint          = "https://api.gfm.eodc.eu/v2/"
	glofasEndpoint       = "https://flood-api.open-meteo.com/v1/flood"
	lanceBaseURL         = "https://nrt3.modaps.eosdis.nasa.gov/archive/allData"
	lanceDocsURL         = "https://www.earthdata.nasa.gov/data/instruments/viirs/near-real-time-data/nrt-global-flood-products"
	gfdsBaseURL          = "https://www.gdacs.org/flooddetection/DATA/ALL"
	gfdsDocsURL          = "https://www.gdacs.org/flooddetection/download.aspx"
	imergEndpoint        = "https://pmmpublisher.pps.eosdis.nasa.gov/"
	imergDocsURL         = "https://gpm.nasa.gov/precip-apps/doc"
	hydrologyContextDocs = "https://global-surface-water.appspot.com/"
	cmorphEndpoint       = "https://ftpprd.ncep.noaa.gov/pub/precip/global_CMORPH/daily_025deg/"
	persiannEndpoint     = "https://persiann.eng.uci.edu/CHRSdata/PDIRNow/PDIRNowdaily/"
	directCAPEndpoint    = "https://api.weather.gov/alerts/active?status=actual&severity=Extreme,Severe,Moderate"
)

type watchAOI struct {
	ID       string
	Label    string
	Kind     string
	Lat      float64
	Lon      float64
	Priority float64
}

var fallbackFloodAOIs = []watchAOI{
	{ID: "flood:philippines_luzon", Label: "Luzon / Manila Bay flood watch", Kind: "flood_gap_zone", Lat: 14.8, Lon: 120.75, Priority: 2.0},
	{ID: "flood:indonesia_java", Label: "Java flood watch", Kind: "flood_gap_zone", Lat: -6.2, Lon: 106.8, Priority: 2.0},
	{ID: "flood:bangladesh_delta", Label: "Bangladesh delta flood watch", Kind: "flood_gap_zone", Lat: 23.7, Lon: 90.4, Priority: 2.0},
	{ID: "flood:pakistan_indus", Label: "Pakistan Indus flood watch", Kind: "flood_gap_zone", Lat: 30.2, Lon: 69.3, Priority: 1.8},
	{ID: "flood:india_ganga", Label: "Ganga basin flood watch", Kind: "flood_gap_zone", Lat: 25.4, Lon: 83.0, Priority: 1.8},
	{ID: "flood:yemen", Label: "Yemen flood watch", Kind: "flood_gap_zone", Lat: 15.4, Lon: 44.2, Priority: 1.7},
	{ID: "flood:sierra_leone", Label: "Sierra Leone flood watch", Kind: "flood_gap_zone", Lat: 8.5, Lon: -13.2, Priority: 1.6},
	{ID: "flood:drc", Label: "Congo River flood watch", Kind: "flood_gap_zone", Lat: -4.3, Lon: 15.3, Priority: 1.6},
	{ID: "flood:south_africa_kwazulu", Label: "KwaZulu-Natal flood watch", Kind: "flood_gap_zone", Lat: -29.9, Lon: 31.0, Priority: 1.5},
	{ID: "flood:panama", Label: "Panama flood watch", Kind: "flood_gap_zone", Lat: 9.0, Lon: -79.5, Priority: 1.5},
	{ID: "flood:colombia_magdalena", Label: "Magdalena basin flood watch", Kind: "flood_gap_zone", Lat: 10.9, Lon: -74.8, Priority: 1.5},
	{ID: "flood:iraq_tigris", Label: "Tigris-Euphrates flood watch", Kind: "flood_gap_zone", Lat: 33.3, Lon: 44.4, Priority: 1.5},
	{ID: "flood:mozambique", Label: "Mozambique flood watch", Kind: "flood_gap_zone", Lat: -19.0, Lon: 34.8, Priority: 1.5},
	{ID: "flood:nigeria", Label: "Nigeria Niger-Benue flood watch", Kind: "flood_gap_zone", Lat: 7.8, Lon: 6.7, Priority: 1.5},
	{ID: "flood:myanmar_irrawaddy", Label: "Irrawaddy flood watch", Kind: "flood_gap_zone", Lat: 17.0, Lon: 95.0, Priority: 1.4},
	{ID: "flood:vietnam_mekong", Label: "Mekong delta flood watch", Kind: "flood_gap_zone", Lat: 10.2, Lon: 105.8, Priority: 1.4},
}

type gfmCollector struct {
	pool    *pgxpool.Pool
	token   string
	maxAOIs int
}

func NewCEMSGFM(pool *pgxpool.Pool) (*gfmCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_CEMS_GFM") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_CEMS_GFM=1")
	}
	return &gfmCollector{
		pool:    pool,
		token:   firstEnv("GFM_TOKEN", "CEMS_GFM_TOKEN", "COPERNICUS_GFM_TOKEN"),
		maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 16, 4, 80),
	}, nil
}

func (c *gfmCollector) ID() string               { return "cems_gfm" }
func (c *gfmCollector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *gfmCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	out := make([]events.Event, 0, len(aois))
	state := "blocked_token_required"
	if c.token != "" {
		state = "token_configured_pending_aoi_product_sampling"
	}
	for _, a := range aois {
		out = append(out, events.Event{
			Ts:     now,
			Source: "cems_gfm",
			ExtID:  fmt.Sprintf("%s:%s", stableID(a.ID), now.Format("2006010215")),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props: map[string]any{
				"watch_aoi_id":        a.ID,
				"watch_aoi_kind":      a.Kind,
				"watch_aoi_label":     a.Label,
				"state":               state,
				"integration_state":   state,
				"product":             "Copernicus EMS Global Flood Monitoring",
				"credential_env":      "GFM_TOKEN",
				"source_api_endpoint": gfmEndpoint,
				"docs_url":            "https://global-flood.emergency.copernicus.eu/",
			},
		})
	}
	return out, nil
}

type glofasCollector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func NewGloFAS(pool *pgxpool.Pool) (*glofasCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_GLOFAS_FLOOD_FORECAST") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_GLOFAS_FLOOD_FORECAST=1")
	}
	return &glofasCollector{pool: pool, maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 16, 4, 80)}, nil
}

func (c *glofasCollector) ID() string               { return "glofas_flood_forecast" }
func (c *glofasCollector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *glofasCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	out := make([]events.Event, 0, len(aois))
	for _, a := range aois {
		ev, ok := fetchGloFASAOI(ctx, a)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

type glofasPayload struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Daily     struct {
		Time     []string  `json:"time"`
		Value    []float64 `json:"river_discharge"`
		Mean     []float64 `json:"river_discharge_mean"`
		Median   []float64 `json:"river_discharge_median"`
		Max      []float64 `json:"river_discharge_max"`
		Min      []float64 `json:"river_discharge_min"`
		OldValue []float64 `json:"river_discharge_forecast"`
	} `json:"daily"`
}

func fetchGloFASAOI(ctx context.Context, a watchAOI) (events.Event, bool) {
	q := url.Values{}
	q.Set("latitude", strconv.FormatFloat(a.Lat, 'f', 5, 64))
	q.Set("longitude", strconv.FormatFloat(a.Lon, 'f', 5, 64))
	q.Set("daily", "river_discharge,river_discharge_mean,river_discharge_median,river_discharge_max,river_discharge_min")
	q.Set("forecast_days", "7")
	q.Set("timezone", "UTC")
	var p glofasPayload
	if err := httpx.GetJSON(ctx, glofasEndpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &p); err != nil {
		return events.Event{}, false
	}
	vals := firstNonEmptyFloatSlice(p.Daily.Value, p.Daily.OldValue)
	if len(p.Daily.Time) == 0 || len(vals) == 0 {
		return events.Event{}, false
	}
	m := summarizeDischarge(p.Daily.Time, vals, p.Daily.Mean, p.Daily.Median, p.Daily.Max)
	if !m.OK {
		return events.Event{}, false
	}
	ts := time.Now().UTC()
	state := "normal"
	if m.Score >= 2.0 {
		state = "strong_flood_pressure"
	} else if m.Score >= 1.0 {
		state = "possible_flood_pressure"
	}
	props := map[string]any{
		"watch_aoi_id":                  a.ID,
		"watch_aoi_kind":                a.Kind,
		"watch_aoi_label":               a.Label,
		"forecast_days":                 len(p.Daily.Time),
		"forecast_peak_date":            m.PeakDate,
		"forecast_peak_discharge_m3s":   round(m.Peak, 2),
		"forecast_today_discharge_m3s":  round(vals[0], 2),
		"baseline_mean_discharge_m3s":   round(m.BaselineMean, 2),
		"baseline_median_discharge_m3s": round(m.BaselineMedian, 2),
		"flood_pressure_ratio":          round(m.Ratio, 2),
		"flood_pressure_score":          round(m.Score, 2),
		"state":                         state,
		"integration_state":             "forecast_sampled",
		"source_api_endpoint":           glofasEndpoint,
	}
	return events.Event{
		Ts:     ts,
		Source: "glofas_flood_forecast",
		ExtID:  fmt.Sprintf("%s:%s", stableID(a.ID), p.Daily.Time[0]),
		Lat:    a.Lat,
		Lon:    a.Lon,
		Props:  props,
	}, true
}

type dischargeMetric struct {
	OK             bool
	Peak           float64
	PeakDate       string
	BaselineMean   float64
	BaselineMedian float64
	Ratio          float64
	Score          float64
}

func summarizeDischarge(times []string, values, means, medians, maxima []float64) dischargeMetric {
	values = align(values, len(times))
	means = align(means, len(times))
	medians = align(medians, len(times))
	maxima = align(maxima, len(times))
	if len(times) == 0 || len(values) == 0 {
		return dischargeMetric{}
	}
	peakIdx := 0
	for i, v := range values {
		if v > values[peakIdx] {
			peakIdx = i
		}
	}
	baselineMean := maxFloat(avgPositive(means), avgPositive(medians), avgPositive(maxima)*0.35, 0.01)
	baselineMedian := maxFloat(avgPositive(medians), 0.01)
	ratio := values[peakIdx] / baselineMean
	score := propx.ClampFloat((ratio-1.25)/1.25, 0, 3)
	if values[peakIdx] >= 250 {
		score = maxFloat(score, 1.0)
	}
	if values[peakIdx] >= 1000 {
		score = maxFloat(score, 2.0)
	}
	return dischargeMetric{
		OK:             true,
		Peak:           values[peakIdx],
		PeakDate:       times[peakIdx],
		BaselineMean:   baselineMean,
		BaselineMedian: baselineMedian,
		Ratio:          ratio,
		Score:          score,
	}
}

type lanceCollector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func NewNASALANCEFlood(pool *pgxpool.Pool) (*lanceCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_NASA_LANCE_FLOOD") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_NASA_LANCE_FLOOD=1")
	}
	return &lanceCollector{pool: pool, maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 16, 4, 80)}, nil
}

func (c *lanceCollector) ID() string               { return "nasa_lance_flood" }
func (c *lanceCollector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *lanceCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	now := time.Now().UTC()
	products := []lanceProduct{
		{Family: "MODIS", Collection: "61", Product: "MCDWD_L3_F1_NRT", Version: "061", FilePrefix: "MCDWD_L3_F1_NRT"},
		{Family: "VIIRS", Collection: "5200", Product: "VCDWD_L3_F1_NRT", Version: "002", FilePrefix: "VCDWD_L3_F1_NRT"},
	}
	out := make([]events.Event, 0, len(aois)*len(products))
	for _, p := range products {
		year, doy := latestLANCEDay(ctx, p, now)
		for _, a := range aois {
			tile := floodTile(a.Lat, a.Lon)
			productURL := lanceProductURL(p, year, doy, tile)
			ts := doyTime(year, doy)
			out = append(out, events.Event{
				Ts:     ts,
				Source: "nasa_lance_flood",
				ExtID:  fmt.Sprintf("%s:%s:%04d%03d", p.Product, stableID(a.ID), year, doy),
				Lat:    a.Lat,
				Lon:    a.Lon,
				Props: map[string]any{
					"watch_aoi_id":        a.ID,
					"watch_aoi_kind":      a.Kind,
					"watch_aoi_label":     a.Label,
					"product":             p.Product,
					"product_family":      p.Family,
					"tile":                tile,
					"year":                year,
					"day_of_year":         doy,
					"product_url":         productURL,
					"viewer_url":          "https://go.nasa.gov/globalflood",
					"integration_state":   "catalog_url_ready_download_token_required",
					"source_api_endpoint": fmt.Sprintf("%s/%s/%s", lanceBaseURL, p.Collection, p.Product),
					"docs_url":            lanceDocsURL,
				},
			})
		}
	}
	return out, nil
}

type lanceProduct struct {
	Family     string
	Collection string
	Product    string
	Version    string
	FilePrefix string
}

func latestLANCEDay(ctx context.Context, p lanceProduct, now time.Time) (int, int) {
	year := now.Year()
	url := fmt.Sprintf("%s/%s/%s/%04d/", lanceBaseURL, p.Collection, p.Product, year)
	buf, err := httpx.GetBytes(ctx, url, map[string]string{"Accept": "text/html"})
	if err != nil {
		return year, now.YearDay()
	}
	re := regexp.MustCompile(`data-name="([0-9]{3})"`)
	maxDay := 0
	for _, m := range re.FindAllStringSubmatch(string(buf), -1) {
		n, _ := strconv.Atoi(m[1])
		if n > maxDay {
			maxDay = n
		}
	}
	if maxDay == 0 {
		maxDay = now.YearDay()
	}
	return year, maxDay
}

func lanceProductURL(p lanceProduct, year, doy int, tile string) string {
	return fmt.Sprintf("%s/%s/%s/%04d/%03d/%s.A%04d%03d.%s.%s.tif",
		lanceBaseURL, p.Collection, p.Product, year, doy, p.FilePrefix, year, doy, tile, p.Version)
}

type gfdsCollector struct{}

func NewGDACSGFDS() (*gfdsCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_GDACS_GFDS") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_GDACS_GFDS=1")
	}
	return &gfdsCollector{}, nil
}

func (c *gfdsCollector) ID() string               { return "gdacs_gfds" }
func (c *gfdsCollector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *gfdsCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	for _, p := range []struct {
		Dir     string
		Product string
		Prefix  string
	}{
		{Dir: "MagTiffs", Product: "gfds_magnitude_signal", Prefix: "mag_signal_"},
		{Dir: "SignalTiffs", Product: "gfds_signal", Prefix: "signal_"},
	} {
		if ev, ok := latestGFDSEvent(ctx, p.Dir, p.Product, p.Prefix); ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 {
		now := time.Now().UTC()
		out = append(out, events.Event{
			Ts:     now,
			Source: "gdacs_gfds",
			ExtID:  "catalog:" + now.Format("2006010215"),
			Lat:    0,
			Lon:    0,
			Props: map[string]any{
				"product":             "gfds_catalog",
				"integration_state":   "catalog_probe_failed",
				"source_api_endpoint": gfdsBaseURL,
				"docs_url":            gfdsDocsURL,
			},
		})
	}
	return out, nil
}

func latestGFDSEvent(ctx context.Context, dir, product, prefix string) (events.Event, bool) {
	now := time.Now().UTC()
	monthURL := fmt.Sprintf("%s/%s/%04d/%02d/", gfdsBaseURL, dir, now.Year(), int(now.Month()))
	buf, err := httpx.GetBytes(ctx, monthURL, map[string]string{"Accept": "text/html"})
	if err != nil {
		return events.Event{}, false
	}
	re := regexp.MustCompile(`(?m)([ 0-9/]{8,10})\s+([0-9: APMapm]+)\s+([0-9]+)\s+<A HREF="([^"]+` + regexp.QuoteMeta(prefix) + `([0-9]{8})_ALL\.tif)"`)
	var best gfdsFile
	for _, m := range re.FindAllStringSubmatch(string(buf), -1) {
		dt, err := time.Parse("20060102", m[5])
		if err != nil {
			continue
		}
		size, _ := strconv.ParseInt(m[3], 10, 64)
		if best.Date.IsZero() || dt.After(best.Date) {
			best = gfdsFile{Date: dt, Size: size, URL: absURL("https://www.gdacs.org", m[4])}
		}
	}
	if best.Date.IsZero() {
		return events.Event{}, false
	}
	return events.Event{
		Ts:     best.Date,
		Source: "gdacs_gfds",
		ExtID:  fmt.Sprintf("%s:%s", product, best.Date.Format("20060102")),
		Lat:    0,
		Lon:    0,
		Props: map[string]any{
			"product":             product,
			"date":                best.Date.Format("2006-01-02"),
			"product_url":         best.URL,
			"content_length":      best.Size,
			"integration_state":   "global_tiff_available",
			"source_api_endpoint": monthURL,
			"docs_url":            gfdsDocsURL,
		},
	}, true
}

type gfdsFile struct {
	Date time.Time
	Size int64
	URL  string
}

type imergCollector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func NewIMERGPrecip(pool *pgxpool.Pool) (*imergCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_IMERG_PRECIP") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_IMERG_PRECIP=1")
	}
	return &imergCollector{pool: pool, maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 12, 4, 80)}, nil
}

func (c *imergCollector) ID() string               { return "imerg_precip" }
func (c *imergCollector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *imergCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	out := make([]events.Event, 0, len(aois))
	state := "publisher_catalog_available_pps_download_auth_required"
	if !endpointHEAD(ctx, imergEndpoint) {
		state = "publisher_catalog_unreachable"
	}
	for _, a := range aois {
		out = append(out, events.Event{
			Ts:     now,
			Source: "imerg_precip",
			ExtID:  fmt.Sprintf("%s:%s", stableID(a.ID), now.Format("2006010215")),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props: map[string]any{
				"watch_aoi_id":          a.ID,
				"watch_aoi_kind":        a.Kind,
				"watch_aoi_label":       a.Label,
				"product":               "GPM IMERG precipitation accumulation",
				"precip_pressure_score": 0,
				"state":                 state,
				"integration_state":     state,
				"source_api_endpoint":   imergEndpoint,
				"docs_url":              imergDocsURL,
			},
		})
	}
	return out, nil
}

type hydrologyContextCollector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func NewHydrologyStaticContext(pool *pgxpool.Pool) (*hydrologyContextCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_HYDROLOGY_STATIC_CONTEXT") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_HYDROLOGY_STATIC_CONTEXT=1")
	}
	return &hydrologyContextCollector{pool: pool, maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 16, 4, 80)}, nil
}

func (c *hydrologyContextCollector) ID() string               { return "hydrology_static_context" }
func (c *hydrologyContextCollector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *hydrologyContextCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	out := make([]events.Event, 0, len(aois))
	for _, a := range aois {
		out = append(out, events.Event{
			Ts:     now,
			Source: "hydrology_static_context",
			ExtID:  stableID(a.ID),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props: map[string]any{
				"watch_aoi_id":        a.ID,
				"watch_aoi_kind":      a.Kind,
				"watch_aoi_label":     a.Label,
				"context_kind":        "river_surface_water_static_context",
				"source_family":       "JRC Global Surface Water + HydroSHEDS/HydroRIVERS",
				"recurrence_context":  "use as static floodplain/river-network prior and false-positive suppressor",
				"integration_state":   "static_context_registered_pending_local_extract",
				"source_api_endpoint": hydrologyContextDocs,
				"hydrosheds_url":      "https://www.hydrosheds.org/products/hydrorivers",
				"docs_url":            "https://global-surface-water.appspot.com/download",
			},
		})
	}
	return out, nil
}

type globalPrecipCollector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

func NewGlobalPrecipMonitor(pool *pgxpool.Pool) (*globalPrecipCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_GLOBAL_PRECIP_MONITOR") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_GLOBAL_PRECIP_MONITOR=1")
	}
	return &globalPrecipCollector{pool: pool, maxAOIs: envInt("FLOOD_COVERAGE_MAX_AOIS", 12, 4, 80)}, nil
}

func (c *globalPrecipCollector) ID() string               { return "global_precip_monitor" }
func (c *globalPrecipCollector) PollEvery() time.Duration { return 3 * time.Hour }

func (c *globalPrecipCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	aois := selectWatchAOIs(ctx, c.pool, c.ID(), c.maxAOIs)
	cmorphState := "directory_available"
	if !endpointHEAD(ctx, cmorphEndpoint) {
		cmorphState = "directory_unreachable_or_slow"
	}
	persiannState := "directory_available"
	if !endpointHEAD(ctx, persiannEndpoint) {
		persiannState = "directory_unreachable_or_slow"
	}
	out := make([]events.Event, 0, len(aois))
	for _, a := range aois {
		out = append(out, events.Event{
			Ts:     now,
			Source: "global_precip_monitor",
			ExtID:  fmt.Sprintf("%s:%s", stableID(a.ID), now.Format("2006010215")),
			Lat:    a.Lat,
			Lon:    a.Lon,
			Props: map[string]any{
				"watch_aoi_id":          a.ID,
				"watch_aoi_kind":        a.Kind,
				"watch_aoi_label":       a.Label,
				"cmorph_state":          cmorphState,
				"persiann_state":        persiannState,
				"precip_pressure_score": 0,
				"integration_state":     "product_directories_registered_pending_raster_sampling",
				"source_api_endpoint":   cmorphEndpoint,
				"persiann_endpoint":     persiannEndpoint,
				"docs_url":              "https://www.cpc.ncep.noaa.gov/products/janowiak/cmorph_get_data.html",
			},
		})
	}
	return out, nil
}

type directFloodCAPCollector struct{}

func NewDirectFloodCAP() (*directFloodCAPCollector, error) {
	if os.Getenv("GORDIOS_DISABLE_DIRECT_FLOOD_CAP") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_DIRECT_FLOOD_CAP=1")
	}
	return &directFloodCAPCollector{}, nil
}

func (c *directFloodCAPCollector) ID() string               { return "direct_flood_cap" }
func (c *directFloodCAPCollector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *directFloodCAPCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Features []struct {
			ID         string         `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   map[string]any `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, directCAPEndpoint, map[string]string{"Accept": "application/geo+json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		if f.Geometry == nil || f.Properties == nil {
			continue
		}
		lat, lon, ok := geoJSONCentroid(f.Geometry)
		if !ok {
			continue
		}
		eventName := stringProp(f.Properties, "event")
		if !isFloodEvent(eventName, stringProp(f.Properties, "headline")) {
			continue
		}
		ts := time.Now().UTC()
		if s := stringProp(f.Properties, "sent"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t.UTC()
			}
		}
		props := copyMap(f.Properties)
		props["feed_id"] = "nws_active_flood_alerts"
		props["source_api_endpoint"] = directCAPEndpoint
		props["integration_state"] = "direct_national_warning_active"
		out = append(out, events.Event{
			Ts:     ts,
			Source: "direct_flood_cap",
			ExtID:  f.ID,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out, nil
}

func selectWatchAOIs(ctx context.Context, pool *pgxpool.Pool, collectorID string, maxAOIs int) []watchAOI {
	out := []watchAOI{}
	for _, a := range collectorutil.ConfiguredAOIs(ctx, pool, collectorID, maxAOIs) {
		out = append(out, watchAOI{
			ID:       a.ID,
			Kind:     a.Kind,
			Label:    a.Label,
			Lat:      a.Lat,
			Lon:      a.Lon,
			Priority: a.Priority,
		})
	}
	out = append(out, fallbackFloodAOIs...)
	out = dedupeAOIs(out)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority > out[j].Priority })
	if len(out) > maxAOIs {
		out = out[:maxAOIs]
	}
	return out
}

func dedupeAOIs(in []watchAOI) []watchAOI {
	seen := map[string]bool{}
	out := make([]watchAOI, 0, len(in))
	for _, a := range in {
		key := stableID(a.ID)
		if key == "" || seen[key] || !validLatLon(a.Lat, a.Lon) {
			continue
		}
		seen[key] = true
		out = append(out, a)
	}
	return out
}

func geoJSONCentroid(g map[string]any) (lat, lon float64, ok bool) {
	typ, _ := g["type"].(string)
	coords, _ := g["coordinates"].([]any)
	switch typ {
	case "Point":
		if len(coords) < 2 {
			return 0, 0, false
		}
		x, _ := coords[0].(float64)
		y, _ := coords[1].(float64)
		return y, x, true
	case "Polygon":
		if len(coords) == 0 {
			return 0, 0, false
		}
		ring, _ := coords[0].([]any)
		return ringCentroid(ring)
	case "MultiPolygon":
		if len(coords) == 0 {
			return 0, 0, false
		}
		poly, _ := coords[0].([]any)
		if len(poly) == 0 {
			return 0, 0, false
		}
		ring, _ := poly[0].([]any)
		return ringCentroid(ring)
	}
	return 0, 0, false
}

func ringCentroid(ring []any) (lat, lon float64, ok bool) {
	if len(ring) == 0 {
		return 0, 0, false
	}
	var sx, sy float64
	n := 0
	for _, p := range ring {
		pair, _ := p.([]any)
		if len(pair) < 2 {
			continue
		}
		x, _ := pair[0].(float64)
		y, _ := pair[1].(float64)
		sx += x
		sy += y
		n++
	}
	if n == 0 {
		return 0, 0, false
	}
	return sy / float64(n), sx / float64(n), true
}

func floodTile(lat, lon float64) string {
	h := int(math.Floor((lon + 180.0) / 10.0))
	v := int(math.Floor((90.0 - lat) / 10.0))
	if h < 0 {
		h = 0
	}
	if h > 35 {
		h = 35
	}
	if v < 0 {
		v = 0
	}
	if v > 17 {
		v = 17
	}
	return fmt.Sprintf("h%02dv%02d", h, v)
}

func doyTime(year, doy int) time.Time {
	if doy < 1 {
		doy = 1
	}
	return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, doy-1)
}

func endpointHEAD(ctx context.Context, u string) bool {
	client := &http.Client{Timeout: 6 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	res, err := client.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 400
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if s := strings.TrimSpace(os.Getenv(name)); s != "" {
			return s
		}
	}
	return ""
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

func stableID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.NewReplacer(":", "_", "/", "_", " ", "_", "#", "_").Replace(s)
	return strings.Trim(s, "_")
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func absURL(base, path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if strings.HasPrefix(path, "/") {
		return strings.TrimRight(base, "/") + path
	}
	return strings.TrimRight(base, "/") + "/" + path
}

func stringProp(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func isFloodEvent(vals ...string) bool {
	for _, v := range vals {
		v = strings.ToLower(v)
		if strings.Contains(v, "flood") {
			return true
		}
	}
	return false
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+3)
	for k, v := range in {
		out[k] = v
	}
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

func align(v []float64, n int) []float64 {
	if len(v) >= n {
		return v[:n]
	}
	out := make([]float64, n)
	copy(out, v)
	return out
}

func avgPositive(vals []float64) float64 {
	total := 0.0
	n := 0
	for _, v := range vals {
		if v > 0 {
			total += v
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return total / float64(n)
}

func maxFloat(vals ...float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func marshalPropsForTest(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
