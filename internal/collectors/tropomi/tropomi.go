// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package tropomi builds small, processed Sentinel-5P/TROPOMI watch-grid
// products from Sentinel Hub statistics. It intentionally emits derived
// H3-cell events, not raw global rasters.
package tropomi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/cdse"
	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uber/h3-go/v4"
)

const (
	defaultStatisticsURL = "https://sh.dataspace.copernicus.eu/api/v1/statistics"
	defaultH3Res         = 4
	defaultMaxAOIs       = 4
)

type Gas string

const (
	GasNO2 Gas = "NO2"
	GasSO2 Gas = "SO2"
)

type gasConfig struct {
	Gas         Gas
	Source      string
	DisplayName string
	Unit        string
	MinQA       int
	NoiseFloor  float64
	TypicalMax  float64
}

var gasConfigs = map[Gas]gasConfig{
	GasNO2: {
		Gas:         GasNO2,
		Source:      "tropomi_no2",
		DisplayName: "TROPOMI NO2 tropospheric column",
		Unit:        "mol/m^2",
		MinQA:       75,
		NoiseFloor:  1e-6,
		TypicalMax:  0.0003,
	},
	GasSO2: {
		Gas:         GasSO2,
		Source:      "tropomi_so2",
		DisplayName: "TROPOMI SO2 total column",
		Unit:        "mol/m^2",
		MinQA:       50,
		NoiseFloor:  5e-5,
		TypicalMax:  0.01,
	},
}

type Collector struct {
	pool       *pgxpool.Pool
	cdseClient *cdse.Client
	httpClient *http.Client
	statsURL   string
	cfg        gasConfig
	maxAOIs    int
	h3Res      int
}

func NewNO2(pool *pgxpool.Pool) (*Collector, error) {
	return New(pool, GasNO2)
}

func NewSO2(pool *pgxpool.Pool) (*Collector, error) {
	return New(pool, GasSO2)
}

func New(pool *pgxpool.Pool, gas Gas) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_TROPOMI") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_TROPOMI=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	cfg, ok := gasConfigs[gas]
	if !ok {
		return nil, fmt.Errorf("unsupported TROPOMI gas %q", gas)
	}
	client, err := cdse.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	statsURL := strings.TrimSpace(os.Getenv("SENTINELHUB_STATISTICS_URL"))
	if statsURL == "" {
		statsURL = defaultStatisticsURL
	}
	return &Collector{
		pool:       pool,
		cdseClient: client,
		httpClient: &http.Client{Timeout: 35 * time.Second},
		statsURL:   statsURL,
		cfg:        cfg,
		maxAOIs:    envInt("TROPOMI_MAX_AOIS", defaultMaxAOIs, 1, 40),
		h3Res:      envInt("TROPOMI_H3_RES", defaultH3Res, 3, 6),
	}, nil
}

func (c *Collector) ID() string { return c.cfg.Source }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("TROPOMI_POLL_EVERY_S", 6*3600, 1800, 24*3600)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois, err := c.watchAOIs(ctx)
	if err != nil {
		return nil, err
	}
	if len(aois) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	currentLagDays := envInt("TROPOMI_CURRENT_LAG_DAYS", 2, 1, 10)
	baselineDays := envInt("TROPOMI_BASELINE_DAYS", 30, 7, 90)
	currentStart := now.Truncate(24*time.Hour).AddDate(0, 0, -currentLagDays)
	from := currentStart.AddDate(0, 0, -baselineDays)
	to := currentStart.Add(24 * time.Hour)

	out := make([]events.Event, 0, len(aois))
	for _, aoi := range aois {
		metric, err := c.statsForAOI(ctx, aoi, from, to, currentStart)
		if err != nil {
			return nil, err
		}
		out = append(out, c.eventForMetric(aoi, metric))
	}
	return out, nil
}

type watchAOI struct {
	ID       string
	Kind     string
	Label    string
	Lat      float64
	Lon      float64
	Priority float64
	Sources  []string
	Cell     h3.Cell
	CellID   string
	Geom     map[string]any
	GeomWKT  string
}

func (c *Collector) watchAOIs(ctx context.Context) ([]watchAOI, error) {
	cfg := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs*3, 7*24*time.Hour, collectorutil.StrategicAOIs)
	byCell := map[string]*watchAOI{}
	for _, cfgAOI := range cfg {
		a := watchAOI{
			ID:       cfgAOI.ID,
			Kind:     cfgAOI.Kind,
			Label:    cfgAOI.Label,
			Lat:      cfgAOI.Lat,
			Lon:      cfgAOI.Lon,
			Priority: cfgAOI.Priority,
			Sources:  []string{"ingestion_aoi"},
		}
		if !validLatLon(a.Lat, a.Lon) {
			continue
		}
		cell, err := h3.LatLngToCell(h3.LatLng{Lat: a.Lat, Lng: a.Lon}, c.h3Res)
		if err != nil {
			continue
		}
		geom, wkt, err := pointBoxGeometry(a.Lat, a.Lon, envFloat("TROPOMI_AOI_HALF_DEG", 0.05))
		if err != nil {
			continue
		}
		cellID := cell.String()
		a.Cell = cell
		a.CellID = cellID
		a.Geom = geom
		a.GeomWKT = wkt
		existing := byCell[cellID]
		if existing == nil {
			cp := a
			byCell[cellID] = &cp
			continue
		}
		if a.Priority > existing.Priority {
			existing.ID = a.ID
			existing.Kind = a.Kind
			existing.Label = a.Label
			existing.Priority = a.Priority
			existing.Lat = a.Lat
			existing.Lon = a.Lon
			existing.Geom = a.Geom
			existing.GeomWKT = a.GeomWKT
		}
		existing.Sources = mergeStrings(existing.Sources, a.Sources)
	}

	out := make([]watchAOI, 0, len(byCell))
	for _, a := range byCell {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority == out[j].Priority {
			return out[i].CellID < out[j].CellID
		}
		return out[i].Priority > out[j].Priority
	})
	if len(out) > c.maxAOIs {
		out = out[:c.maxAOIs]
	}
	return out, nil
}

type tropomiMetric struct {
	State                 string
	StateLabel            string
	QualityBlocked        bool
	Current               intervalStats
	BaselineMedianMean    float64
	BaselineStdMean       float64
	BaselineDays          int
	DeltaMean             float64
	AnomalyZ              float64
	AnomalyRatio          float64
	AnomalyScore          float64
	ComputedAt            time.Time
	CurrentLookbackDays   int
	BaselineRequestedDays int
	Note                  string
}

func (c *Collector) statsForAOI(ctx context.Context, aoi watchAOI, from, to, currentStart time.Time) (tropomiMetric, error) {
	auth, err := c.cdseClient.AuthorizationHeader(ctx)
	if err != nil {
		return tropomiMetric{}, err
	}
	body := statsRequest{
		Input: statsInput{
			Bounds: statsBounds{
				Geometry:   aoi.Geom,
				Properties: map[string]string{"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"},
			},
			Data: []statsData{{
				Type: "sentinel-5p-l2",
				DataFilter: statsDataFilter{
					TimeRange:       timeRange{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)},
					MosaickingOrder: "mostRecent",
				},
				Processing: map[string]any{"minQa": c.cfg.MinQA},
			}},
		},
		Aggregation: statsAggregation{
			TimeRange:           timeRange{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)},
			AggregationInterval: map[string]string{"of": "P1D"},
			Evalscript:          evalscript(c.cfg.Gas),
			ResX:                envFloat("TROPOMI_STATS_RES_DEG", 0.05),
			ResY:                envFloat("TROPOMI_STATS_RES_DEG", 0.05),
		},
		Calculations: map[string]any{
			"default": map[string]any{
				"statistics": map[string]any{
					"default": map[string]any{
						"percentiles": map[string]any{"k": []float64{5, 50, 95}},
					},
				},
			},
		},
	}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.statsURL, bytes.NewReader(raw))
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return tropomiMetric{}, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return tropomiMetric{}, fmt.Errorf("%s statistics %d: %s", c.cfg.Source, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out statsResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return tropomiMetric{}, fmt.Errorf("parse %s statistics: %w", c.cfg.Source, err)
	}
	intervals := intervalsFromStats(out, from)
	return summarizeMetric(c.cfg, intervals, currentStart, metricOptions{
		MinBaselineDays:     envInt("TROPOMI_MIN_BASELINE_DAYS", 6, 2, 30),
		MinValidCount:       envInt("TROPOMI_MIN_VALID_PIXELS", 3, 1, 100),
		MinValidPct:         envFloat("TROPOMI_MIN_VALID_PCT", 5.0),
		CurrentLookbackDays: envInt("TROPOMI_CURRENT_LOOKBACK_DAYS", 4, 1, 10),
		BaselineDays:        envInt("TROPOMI_BASELINE_DAYS", 30, 7, 90),
	}), nil
}

func (c *Collector) eventForMetric(aoi watchAOI, m tropomiMetric) events.Event {
	ts := m.Current.From
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	props := map[string]any{
		"source_kind":               "tropomi_processed_cell",
		"display_name":              c.cfg.DisplayName,
		"gas":                       string(c.cfg.Gas),
		"band":                      string(c.cfg.Gas),
		"unit":                      c.cfg.Unit,
		"state":                     m.State,
		"state_label":               m.StateLabel,
		"quality_blocked":           m.QualityBlocked,
		"h3_cell":                   aoi.CellID,
		"h3_res":                    c.h3Res,
		"sample_lat":                aoi.Lat,
		"sample_lon":                aoi.Lon,
		"aoi_geometry_wkt":          aoi.GeomWKT,
		"watch_aoi_id":              aoi.ID,
		"watch_aoi_kind":            aoi.Kind,
		"watch_aoi_label":           aoi.Label,
		"watch_sources":             aoi.Sources,
		"min_qa":                    c.cfg.MinQA,
		"current_interval_from":     timeString(m.Current.From),
		"current_interval_to":       timeString(m.Current.To),
		"current_mean":              m.Current.Mean,
		"current_stdev":             m.Current.StdDev,
		"current_p05":               m.Current.P05,
		"current_p50":               m.Current.P50,
		"current_p95":               m.Current.P95,
		"current_valid_pixel_pct":   round2(m.Current.ValidPct()),
		"current_valid_pixel_count": m.Current.ValidCount(),
		"current_sample_count":      m.Current.SampleCount,
		"geometry_pixel_count":      m.Current.GeometryPixelCount,
		"baseline_mean_median":      m.BaselineMedianMean,
		"baseline_mean_stdev":       m.BaselineStdMean,
		"baseline_days":             m.BaselineDays,
		"delta_mean":                m.DeltaMean,
		"anomaly_z":                 round2(m.AnomalyZ),
		"anomaly_ratio":             round2(m.AnomalyRatio),
		"anomaly_score":             round2(m.AnomalyScore),
		"current_lookback_days":     m.CurrentLookbackDays,
		"baseline_requested_days":   m.BaselineRequestedDays,
		"computed_at":               m.ComputedAt.Format(time.RFC3339),
		"source_api_endpoint":       c.statsURL,
	}
	if m.Note != "" {
		props["note"] = m.Note
	}
	return events.Event{
		Ts:     ts,
		Source: c.cfg.Source,
		ExtID:  fmt.Sprintf("%s:%s", aoi.CellID, ts.Format("2006-01-02")),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Geom:   aoi.GeomWKT,
		Props:  props,
	}
}

type metricOptions struct {
	MinBaselineDays     int
	MinValidCount       int
	MinValidPct         float64
	CurrentLookbackDays int
	BaselineDays        int
}

func summarizeMetric(cfg gasConfig, intervals []intervalStats, targetCurrentStart time.Time, opts metricOptions) tropomiMetric {
	now := time.Now().UTC()
	m := tropomiMetric{
		State:                 "quality_blocked",
		StateLabel:            "QUALITY BLOCKED",
		QualityBlocked:        true,
		ComputedAt:            now,
		CurrentLookbackDays:   opts.CurrentLookbackDays,
		BaselineRequestedDays: opts.BaselineDays,
	}
	if len(intervals) == 0 {
		m.Note = "no Sentinel-5P statistics intervals returned"
		return m
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].From.Before(intervals[j].From) })

	lookbackStart := targetCurrentStart.AddDate(0, 0, -opts.CurrentLookbackDays+1)
	var current *intervalStats
	for i := len(intervals) - 1; i >= 0; i-- {
		it := intervals[i]
		if it.From.Before(lookbackStart) || it.From.After(targetCurrentStart) {
			continue
		}
		if it.hasValid(opts.MinValidCount, opts.MinValidPct) {
			current = &it
			break
		}
		if current == nil && it.ValidCount() > 0 {
			cp := it
			current = &cp
		}
	}
	if current == nil {
		m.Note = "no valid current Sentinel-5P pixels in the requested AOI"
		return m
	}
	m.Current = *current

	means := make([]float64, 0, len(intervals))
	for _, it := range intervals {
		if !it.From.Before(current.From) {
			continue
		}
		if it.hasValid(opts.MinValidCount, opts.MinValidPct) {
			means = append(means, it.Mean)
		}
	}
	m.BaselineDays = len(means)
	if !current.hasValid(opts.MinValidCount, opts.MinValidPct) {
		m.Note = fmt.Sprintf("current valid pixel coverage %.1f%% / %d pixels is below threshold", current.ValidPct(), current.ValidCount())
		return m
	}
	if len(means) < opts.MinBaselineDays {
		m.State = "baseline_pending"
		m.StateLabel = "BASELINE PENDING"
		m.Note = fmt.Sprintf("baseline has %d usable day(s), needs %d", len(means), opts.MinBaselineDays)
		return m
	}

	baseMedian := median(means)
	sigma := robustSigma(means)
	if sigma < cfg.NoiseFloor {
		sigma = math.Max(cfg.NoiseFloor, math.Abs(baseMedian)*0.25)
	}
	delta := current.Mean - baseMedian
	z := 0.0
	if sigma > 0 {
		z = delta / sigma
	}
	if z < 0 {
		z = 0
	}
	ratio := 0.0
	if math.Abs(baseMedian) > cfg.NoiseFloor {
		ratio = current.Mean / baseMedian
	}
	score := propx.ClampFloat(z, 0, 3)
	state, label := "no_significant_change", "NO SIGNIFICANT CHANGE"
	switch {
	case z >= 3.0:
		state, label = "strong_change", "STRONG CHANGE"
	case z >= 2.0:
		state, label = "possible_change", "POSSIBLE CHANGE"
	}
	m.State = state
	m.StateLabel = label
	m.QualityBlocked = false
	m.BaselineMedianMean = baseMedian
	m.BaselineStdMean = sigma
	m.DeltaMean = delta
	m.AnomalyZ = z
	m.AnomalyRatio = ratio
	m.AnomalyScore = score
	return m
}

type intervalStats struct {
	From               time.Time
	To                 time.Time
	Mean               float64
	StdDev             float64
	P05                float64
	P50                float64
	P95                float64
	SampleCount        int
	NoDataCount        int
	GeometryPixelCount int
}

func (s intervalStats) ValidCount() int {
	n := s.SampleCount - s.NoDataCount
	if n < 0 {
		return 0
	}
	return n
}

func (s intervalStats) ValidPct() float64 {
	if s.GeometryPixelCount > 0 {
		return 100 * float64(s.ValidCount()) / float64(s.GeometryPixelCount)
	}
	if s.SampleCount > 0 {
		return 100 * float64(s.ValidCount()) / float64(s.SampleCount)
	}
	return 0
}

func (s intervalStats) hasValid(minCount int, minPct float64) bool {
	return s.ValidCount() >= minCount && s.ValidPct() >= minPct
}

type statsRequest struct {
	Input        statsInput       `json:"input"`
	Aggregation  statsAggregation `json:"aggregation"`
	Calculations map[string]any   `json:"calculations"`
}

type statsInput struct {
	Bounds statsBounds `json:"bounds"`
	Data   []statsData `json:"data"`
}

type statsBounds struct {
	Geometry   map[string]any    `json:"geometry"`
	Properties map[string]string `json:"properties"`
}

type statsData struct {
	Type       string          `json:"type"`
	DataFilter statsDataFilter `json:"dataFilter,omitempty"`
	Processing map[string]any  `json:"processing,omitempty"`
}

type statsDataFilter struct {
	TimeRange       timeRange `json:"timeRange,omitempty"`
	MosaickingOrder string    `json:"mosaickingOrder,omitempty"`
}

type statsAggregation struct {
	TimeRange           timeRange         `json:"timeRange"`
	AggregationInterval map[string]string `json:"aggregationInterval"`
	Evalscript          string            `json:"evalscript"`
	ResX                float64           `json:"resx"`
	ResY                float64           `json:"resy"`
}

type timeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type statsResponse struct {
	Data               []statsDatum `json:"data"`
	GeometryPixelCount int          `json:"geometryPixelCount"`
}

type statsDatum struct {
	Interval struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"interval"`
	Outputs struct {
		Default struct {
			Bands map[string]statsBand `json:"bands"`
		} `json:"default"`
	} `json:"outputs"`
}

type statsBand struct {
	Stats struct {
		Mean        statFloat `json:"mean"`
		StdDev      statFloat `json:"stDev"`
		SampleCount int       `json:"sampleCount"`
		NoDataCount int       `json:"noDataCount"`
		Percentiles struct {
			P05 statFloat `json:"5.0"`
			P50 statFloat `json:"50.0"`
			P95 statFloat `json:"95.0"`
		} `json:"percentiles"`
	} `json:"stats"`
}

type statFloat float64

func (f *statFloat) UnmarshalJSON(raw []byte) error {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		var unquoted string
		if err := json.Unmarshal(raw, &unquoted); err != nil {
			return err
		}
		s = strings.TrimSpace(unquoted)
	}
	switch strings.ToLower(s) {
	case "", "nan", "+nan", "-nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity":
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		*f = 0
		return nil
	}
	*f = statFloat(v)
	return nil
}

func (f statFloat) Float64() float64 { return float64(f) }

func intervalsFromStats(out statsResponse, fallbackStart time.Time) []intervalStats {
	intervals := make([]intervalStats, 0, len(out.Data))
	for i, d := range out.Data {
		band, ok := d.Outputs.Default.Bands["B0"]
		if !ok {
			continue
		}
		from, _ := time.Parse(time.RFC3339, d.Interval.From)
		to, _ := time.Parse(time.RFC3339, d.Interval.To)
		if from.IsZero() {
			from = fallbackStart.AddDate(0, 0, i)
		}
		if to.IsZero() {
			to = from.Add(24 * time.Hour)
		}
		intervals = append(intervals, intervalStats{
			From:               from.UTC(),
			To:                 to.UTC(),
			Mean:               band.Stats.Mean.Float64(),
			StdDev:             band.Stats.StdDev.Float64(),
			P05:                band.Stats.Percentiles.P05.Float64(),
			P50:                band.Stats.Percentiles.P50.Float64(),
			P95:                band.Stats.Percentiles.P95.Float64(),
			SampleCount:        band.Stats.SampleCount,
			NoDataCount:        band.Stats.NoDataCount,
			GeometryPixelCount: out.GeometryPixelCount,
		})
	}
	return intervals
}

func evalscript(gas Gas) string {
	band := string(gas)
	return fmt.Sprintf(`//VERSION=3
function setup() {
  return {
    input: ["%s", "dataMask"],
    output: [{ id: "default", bands: 1, sampleType: "FLOAT32" }, { id: "dataMask", bands: 1 }]
  };
}
function evaluatePixel(s) {
  var v = s.%s;
  var ok = s.dataMask && isFinite(v);
  return { default: [ok ? v : 0], dataMask: [ok ? 1 : 0] };
}`, band, band)
}

func cellGeometry(cell h3.Cell) (map[string]any, string, error) {
	boundary, err := h3.CellToBoundary(cell)
	if err != nil {
		return nil, "", err
	}
	if len(boundary) < 3 {
		return nil, "", errors.New("h3 boundary has fewer than three vertices")
	}
	ring := make([][]float64, 0, len(boundary)+1)
	parts := make([]string, 0, len(boundary)+1)
	for _, p := range boundary {
		ring = append(ring, []float64{p.Lng, p.Lat})
		parts = append(parts, fmt.Sprintf("%f %f", p.Lng, p.Lat))
	}
	first := boundary[0]
	ring = append(ring, []float64{first.Lng, first.Lat})
	parts = append(parts, fmt.Sprintf("%f %f", first.Lng, first.Lat))
	geom := map[string]any{
		"type":        "Polygon",
		"coordinates": []any{ring},
	}
	return geom, "POLYGON((" + strings.Join(parts, ",") + "))", nil
}

func pointBoxGeometry(lat, lon, halfDeg float64) (map[string]any, string, error) {
	if !validLatLon(lat, lon) {
		return nil, "", errors.New("invalid point")
	}
	if halfDeg <= 0 {
		halfDeg = 0.05
	}
	if halfDeg > 0.5 {
		halfDeg = 0.5
	}
	minLat := math.Max(-89.999999, lat-halfDeg)
	maxLat := math.Min(89.999999, lat+halfDeg)
	minLon := math.Max(-179.999999, lon-halfDeg)
	maxLon := math.Min(179.999999, lon+halfDeg)
	ring := [][]float64{
		{minLon, minLat},
		{maxLon, minLat},
		{maxLon, maxLat},
		{minLon, maxLat},
		{minLon, minLat},
	}
	parts := make([]string, 0, len(ring))
	for _, p := range ring {
		parts = append(parts, fmt.Sprintf("%f %f", p[0], p[1]))
	}
	geom := map[string]any{
		"type":        "Polygon",
		"coordinates": []any{ring},
	}
	return geom, "POLYGON((" + strings.Join(parts, ",") + "))", nil
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	m := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[m]
	}
	return (cp[m-1] + cp[m]) / 2
}

func robustSigma(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	med := median(xs)
	dev := make([]float64, len(xs))
	for i, x := range xs {
		dev[i] = math.Abs(x - med)
	}
	mad := median(dev) * 1.4826
	if mad > 0 {
		return mad
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	return math.Sqrt(ss / float64(len(xs)-1))
}

func mergeStrings(a, b []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(a)+len(b))
	for _, xs := range [][]string{a, b} {
		for _, x := range xs {
			x = strings.TrimSpace(x)
			if x == "" {
				continue
			}
			if _, ok := seen[x]; ok {
				continue
			}
			seen[x] = struct{}{}
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !math.IsNaN(lat) && !math.IsNaN(lon)
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func envInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func envFloat(key string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def
	}
	return v
}

func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}
