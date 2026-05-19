// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package blackmarble builds small processed NASA Black Marble / VIIRS
// night-light watch cells from public NASA GIBS rendered daily tiles. It emits
// derived AOI metrics, not raw HDF science products.
package blackmarble

import (
	"context"
	"errors"
	"fmt"
	"image"
	"image/png"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uber/h3-go/v4"
)

const (
	sourceID        = "black_marble_nightlights"
	defaultLayerID  = "VIIRS_SNPP_GapFilled_BRDF_Corrected_DayNightBand_Radiance"
	defaultGIBSBase = "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best"
	defaultH3Res    = 4
	defaultMatrix   = 7
)

var matrixDims500m = map[int][2]int{
	0: {2, 1},
	1: {3, 2},
	2: {5, 3},
	3: {10, 5},
	4: {20, 10},
	5: {40, 20},
	6: {80, 40},
	7: {160, 80},
}

type Collector struct {
	pool       *pgxpool.Pool
	httpClient *http.Client
	baseURL    string
	layerID    string
	maxAOIs    int
	h3Res      int
	matrix     int
	radiusPx   int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_BLACK_MARBLE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_BLACK_MARBLE=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("BLACK_MARBLE_GIBS_BASE_URL")), "/")
	if base == "" {
		base = defaultGIBSBase
	}
	layer := strings.TrimSpace(os.Getenv("BLACK_MARBLE_LAYER_ID"))
	if layer == "" {
		layer = defaultLayerID
	}
	matrix := envInt("BLACK_MARBLE_TILE_MATRIX", defaultMatrix, 3, 7)
	if _, ok := matrixDims500m[matrix]; !ok {
		matrix = defaultMatrix
	}
	return &Collector{
		pool:       pool,
		httpClient: &http.Client{Timeout: 18 * time.Second},
		baseURL:    base,
		layerID:    layer,
		maxAOIs:    envInt("BLACK_MARBLE_MAX_AOIS", 4, 1, 30),
		h3Res:      envInt("BLACK_MARBLE_H3_RES", defaultH3Res, 3, 6),
		matrix:     matrix,
		radiusPx:   envInt("BLACK_MARBLE_SAMPLE_RADIUS_PX", 8, 1, 30),
	}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("BLACK_MARBLE_POLL_EVERY_S", 12*3600, 3600, 48*3600)) * time.Second
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
	currentDate := now.Truncate(24*time.Hour).AddDate(0, 0, -envInt("BLACK_MARBLE_CURRENT_LAG_DAYS", 2, 1, 10))
	baselineDays := envInt("BLACK_MARBLE_BASELINE_DAYS", 28, 7, 90)
	baselineGap := envInt("BLACK_MARBLE_BASELINE_GAP_DAYS", 7, 2, 30)
	sampleEvery := envInt("BLACK_MARBLE_BASELINE_SAMPLE_EVERY_DAYS", 4, 1, 14)

	cache := map[string]image.Image{}
	out := make([]events.Event, 0, len(aois))
	for _, aoi := range aois {
		metric := c.metricForAOI(ctx, cache, aoi, currentDate, baselineDays, baselineGap, sampleEvery)
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
		geom, wkt, err := cellGeometry(cell)
		if err != nil {
			continue
		}
		a.Cell = cell
		a.CellID = cell.String()
		a.Geom = geom
		a.GeomWKT = wkt
		existing := byCell[a.CellID]
		if existing == nil {
			cp := a
			byCell[a.CellID] = &cp
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

type sample struct {
	Date                   time.Time
	Mean                   float64
	P50                    float64
	P95                    float64
	ValidCount             int
	TotalCount             int
	LocalMidnightUTC       time.Time
	SolarElevationMidnight float64
	NightExpected          bool
	OK                     bool
}

func (s sample) ValidPct() float64 {
	if s.TotalCount <= 0 {
		return 0
	}
	return float64(s.ValidCount) * 100 / float64(s.TotalCount)
}

type metric struct {
	State              string
	StateLabel         string
	QualityBlocked     bool
	Current            sample
	Previous           sample
	BaselineMedian     float64
	BaselineStd        float64
	BaselineSampleDays int
	DeltaBrightness    float64
	DeltaPct           float64
	AnomalyZ           float64
	BlackoutScore      float64
	RecoveryScore      float64
	RadianceSpikeScore float64
	ComputedAt         time.Time
	LayerID            string
	Note               string
}

func (c *Collector) metricForAOI(ctx context.Context, cache map[string]image.Image, aoi watchAOI, currentDate time.Time, baselineDays, baselineGap, sampleEvery int) metric {
	m := metric{
		State:          "quality_blocked",
		StateLabel:     "QUALITY BLOCKED",
		QualityBlocked: true,
		ComputedAt:     time.Now().UTC(),
		LayerID:        c.layerID,
	}
	current, err := c.sampleCurrent(ctx, cache, aoi, currentDate)
	if err != nil || !current.OK {
		m.Note = "no current NASA GIBS night-light tile sample"
		return m
	}
	m.Current = current
	if !current.NightExpected {
		m.State = "night_unavailable"
		m.StateLabel = "NIGHT UNAVAILABLE"
		m.Note = "local solar midnight is too bright for a reliable night-light comparison"
		return m
	}

	var baseline []sample
	for d := current.Date.AddDate(0, 0, -baselineDays); d.Before(current.Date.AddDate(0, 0, -baselineGap+1)); d = d.AddDate(0, 0, sampleEvery) {
		s, err := c.sampleDate(ctx, cache, aoi, d)
		if err == nil && s.OK && s.NightExpected {
			baseline = append(baseline, s)
		}
	}
	prev, _ := c.sampleDate(ctx, cache, aoi, current.Date.AddDate(0, 0, -sampleEvery))
	m.Previous = prev
	if len(baseline) < envInt("BLACK_MARBLE_MIN_BASELINE_SAMPLES", 4, 2, 20) {
		m.State = "baseline_pending"
		m.StateLabel = "BASELINE PENDING"
		m.Note = "too few baseline night-light samples"
		return m
	}
	if current.ValidCount < envInt("BLACK_MARBLE_MIN_VALID_PIXELS", 20, 1, 1000) || current.ValidPct() < envFloat("BLACK_MARBLE_MIN_VALID_PCT", 40.0) {
		m.Note = "too few valid rendered night-light pixels"
		return m
	}

	means := make([]float64, 0, len(baseline))
	for _, s := range baseline {
		means = append(means, s.Mean)
	}
	m.BaselineMedian = median(means)
	m.BaselineStd = robustSigma(means)
	if m.BaselineStd < 2 {
		m.BaselineStd = 2
	}
	m.BaselineSampleDays = len(baseline)
	m.DeltaBrightness = current.Mean - m.BaselineMedian
	if m.BaselineMedian > 2 {
		m.DeltaPct = m.DeltaBrightness / m.BaselineMedian
	}
	m.AnomalyZ = m.DeltaBrightness / m.BaselineStd

	blackoutThreshold := -math.Abs(envFloat("BLACK_MARBLE_BLACKOUT_DELTA_PCT", 0.30))
	spikeThreshold := math.Abs(envFloat("BLACK_MARBLE_SPIKE_DELTA_PCT", 0.45))
	if m.DeltaPct <= blackoutThreshold || m.AnomalyZ <= -2.0 {
		m.BlackoutScore = propx.ClampFloat((math.Abs(m.DeltaPct)-math.Abs(blackoutThreshold))*4+math.Abs(m.AnomalyZ)/2, 0, 3)
	}
	if m.DeltaPct >= spikeThreshold || m.AnomalyZ >= 2.0 {
		m.RadianceSpikeScore = propx.ClampFloat((m.DeltaPct-spikeThreshold)*3+m.AnomalyZ/2, 0, 3)
	}
	if prev.OK && m.BaselineMedian > 2 {
		prevPct := (prev.Mean - m.BaselineMedian) / m.BaselineMedian
		if prevPct <= -0.25 && m.DeltaPct > -0.10 && current.Mean > prev.Mean*1.15 {
			m.RecoveryScore = propx.ClampFloat((m.DeltaPct-prevPct)*3, 0, 3)
		}
	}

	m.QualityBlocked = false
	switch {
	case m.BlackoutScore >= 1.0:
		m.State = "possible_blackout"
		m.StateLabel = "POSSIBLE BLACKOUT"
	case m.RadianceSpikeScore >= 1.0:
		m.State = "radiance_spike"
		m.StateLabel = "RADIANCE SPIKE"
	case m.RecoveryScore >= 1.0:
		m.State = "recovery"
		m.StateLabel = "RECOVERY"
	default:
		m.State = "no_significant_change"
		m.StateLabel = "NO SIGNIFICANT CHANGE"
	}
	return m
}

func (c *Collector) sampleCurrent(ctx context.Context, cache map[string]image.Image, aoi watchAOI, date time.Time) (sample, error) {
	var lastErr error
	for i := 0; i <= envInt("BLACK_MARBLE_CURRENT_FALLBACK_DAYS", 5, 0, 14); i++ {
		s, err := c.sampleDate(ctx, cache, aoi, date.AddDate(0, 0, -i))
		if err == nil && s.OK {
			return s, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no current sample")
	}
	return sample{}, lastErr
}

func (c *Collector) sampleDate(ctx context.Context, cache map[string]image.Image, aoi watchAOI, date time.Time) (sample, error) {
	tile, px, py, err := c.tileFor(aoi.Lat, aoi.Lon)
	if err != nil {
		return sample{}, err
	}
	img, err := c.fetchTile(ctx, cache, date, tile.row, tile.col)
	if err != nil {
		return sample{}, err
	}
	return sampleImage(img, date, px, py, c.radiusPx, aoi.Lat, aoi.Lon), nil
}

type tileRef struct {
	row int
	col int
}

func (c *Collector) tileFor(lat, lon float64) (tileRef, int, int, error) {
	dims, ok := matrixDims500m[c.matrix]
	if !ok {
		return tileRef{}, 0, 0, fmt.Errorf("unsupported 500m tile matrix %d", c.matrix)
	}
	w, h := dims[0], dims[1]
	lon = math.Max(-179.999999, math.Min(179.999999, lon))
	lat = math.Max(-89.999999, math.Min(89.999999, lat))
	tileW := 360.0 / float64(w)
	tileH := 180.0 / float64(h)
	col := int(math.Floor((lon + 180.0) / tileW))
	row := int(math.Floor((90.0 - lat) / tileH))
	col = maxInt(0, minInt(col, w-1))
	row = maxInt(0, minInt(row, h-1))
	x := int(math.Floor(((lon + 180.0) - float64(col)*tileW) / tileW * 512.0))
	y := int(math.Floor(((90.0 - lat) - float64(row)*tileH) / tileH * 512.0))
	x = maxInt(0, minInt(x, 511))
	y = maxInt(0, minInt(y, 511))
	return tileRef{row: row, col: col}, x, y, nil
}

func (c *Collector) fetchTile(ctx context.Context, cache map[string]image.Image, date time.Time, row, col int) (image.Image, error) {
	key := fmt.Sprintf("%s/%d/%d/%d", date.Format("2006-01-02"), c.matrix, row, col)
	if img := cache[key]; img != nil {
		return img, nil
	}
	url := fmt.Sprintf("%s/%s/default/%s/500m/%d/%d/%d.png",
		c.baseURL, c.layerID, date.Format("2006-01-02"), c.matrix, row, col)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "gordios-black-marble/1.0")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("black marble tile %s: %s", key, resp.Status)
	}
	img, err := png.Decode(resp.Body)
	if err != nil {
		return nil, err
	}
	cache[key] = img
	return img, nil
}

func sampleImage(img image.Image, date time.Time, cx, cy, radius int, lat, lon float64) sample {
	b := img.Bounds()
	vals := []float64{}
	total := 0
	localMidnightUTC := localSolarMidnight(date, lon)
	solarElev := solarElevationDeg(lat, lon, localMidnightUTC)
	nightExpected := solarElev <= envFloat("BLACK_MARBLE_NIGHT_MAX_SOLAR_ELEV_DEG", -6.0)
	for y := cy - radius; y <= cy+radius; y++ {
		for x := cx - radius; x <= cx+radius; x++ {
			if x < b.Min.X || x >= b.Max.X || y < b.Min.Y || y >= b.Max.Y {
				continue
			}
			total++
			r, g, bl, a := img.At(x, y).RGBA()
			if a == 0 {
				continue
			}
			// GIBS tiles are rendered imagery, not raw radiance. Luma is a
			// stable relative brightness proxy for blackout/spike detection.
			luma := (0.2126*float64(r) + 0.7152*float64(g) + 0.0722*float64(bl)) / 257.0
			vals = append(vals, luma)
		}
	}
	if total == 0 || len(vals) == 0 {
		return sample{
			Date:                   date,
			TotalCount:             total,
			LocalMidnightUTC:       localMidnightUTC,
			SolarElevationMidnight: solarElev,
			NightExpected:          nightExpected,
		}
	}
	sort.Float64s(vals)
	var sum float64
	for _, v := range vals {
		sum += v
	}
	p95Idx := int(math.Ceil(float64(len(vals))*0.95)) - 1
	p95Idx = maxInt(0, minInt(p95Idx, len(vals)-1))
	return sample{
		Date:                   date,
		Mean:                   sum / float64(len(vals)),
		P50:                    median(vals),
		P95:                    vals[p95Idx],
		ValidCount:             len(vals),
		TotalCount:             total,
		LocalMidnightUTC:       localMidnightUTC,
		SolarElevationMidnight: solarElev,
		NightExpected:          nightExpected,
		OK:                     true,
	}
}

func (c *Collector) eventForMetric(aoi watchAOI, m metric) events.Event {
	props := map[string]any{
		"source_kind":               "processed_black_marble_cell",
		"display_name":              "NASA Black Marble night-light anomaly",
		"layer_id":                  m.LayerID,
		"state":                     m.State,
		"state_label":               m.StateLabel,
		"quality_blocked":           m.QualityBlocked,
		"h3_cell":                   aoi.CellID,
		"h3_res":                    c.h3Res,
		"h3_cell_geometry":          aoi.Geom,
		"h3_cell_wkt":               aoi.GeomWKT,
		"sample_lat":                aoi.Lat,
		"sample_lon":                aoi.Lon,
		"watch_aoi_id":              aoi.ID,
		"watch_aoi_kind":            aoi.Kind,
		"watch_aoi_label":           aoi.Label,
		"watch_sources":             aoi.Sources,
		"current_date":              dateString(m.Current.Date),
		"current_night_expected":    m.Current.NightExpected,
		"local_midnight_utc":        timeString(m.Current.LocalMidnightUTC),
		"solar_elevation_midnight":  round2(m.Current.SolarElevationMidnight),
		"current_brightness_mean":   round2(m.Current.Mean),
		"current_brightness_p50":    round2(m.Current.P50),
		"current_brightness_p95":    round2(m.Current.P95),
		"current_valid_pixel_pct":   round2(m.Current.ValidPct()),
		"current_valid_pixel_count": m.Current.ValidCount,
		"previous_date":             dateString(m.Previous.Date),
		"previous_brightness_mean":  round2(m.Previous.Mean),
		"baseline_brightness_p50":   round2(m.BaselineMedian),
		"baseline_brightness_stdev": round2(m.BaselineStd),
		"baseline_sample_days":      m.BaselineSampleDays,
		"delta_brightness":          round2(m.DeltaBrightness),
		"delta_pct":                 round2(m.DeltaPct * 100),
		"anomaly_z":                 round2(m.AnomalyZ),
		"blackout_score":            round2(m.BlackoutScore),
		"recovery_score":            round2(m.RecoveryScore),
		"radiance_spike_score":      round2(m.RadianceSpikeScore),
		"tile_matrix":               c.matrix,
		"sample_radius_px":          c.radiusPx,
		"computed_at":               m.ComputedAt.Format(time.RFC3339),
		"source_api_endpoint":       c.baseURL + "/" + c.layerID,
	}
	if m.Note != "" {
		props["note"] = m.Note
	}
	ts := m.Current.Date
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  fmt.Sprintf("%s:%s", aoi.CellID, ts.Format("2006-01-02")),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Props:  props,
	}
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
	return map[string]any{"type": "Polygon", "coordinates": []any{ring}}, "POLYGON((" + strings.Join(parts, ",") + "))", nil
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

func dateString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02")
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func localSolarMidnight(date time.Time, lon float64) time.Time {
	day := date.UTC().Truncate(24 * time.Hour)
	offsetHours := lon / 15.0
	return day.Add(time.Duration(-offsetHours * float64(time.Hour)))
}

func solarElevationDeg(lat, lon float64, at time.Time) float64 {
	t := at.UTC()
	day := float64(t.YearDay())
	hour := float64(t.Hour()) + float64(t.Minute())/60.0 + float64(t.Second())/3600.0
	gamma := 2 * math.Pi / 365.0 * (day - 1 + (hour-12)/24.0)
	decl := 0.006918 -
		0.399912*math.Cos(gamma) + 0.070257*math.Sin(gamma) -
		0.006758*math.Cos(2*gamma) + 0.000907*math.Sin(2*gamma) -
		0.002697*math.Cos(3*gamma) + 0.00148*math.Sin(3*gamma)
	eqTime := 229.18 * (0.000075 +
		0.001868*math.Cos(gamma) - 0.032077*math.Sin(gamma) -
		0.014615*math.Cos(2*gamma) - 0.040849*math.Sin(2*gamma))
	trueSolarMinutes := math.Mod(hour*60.0+eqTime+4.0*lon+1440.0, 1440.0)
	hourAngle := trueSolarMinutes/4.0 - 180.0
	if hourAngle < -180 {
		hourAngle += 360
	}
	latRad := lat * math.Pi / 180.0
	haRad := hourAngle * math.Pi / 180.0
	elev := math.Asin(math.Sin(latRad)*math.Sin(decl) + math.Cos(latRad)*math.Cos(decl)*math.Cos(haRad))
	return elev * 180.0 / math.Pi
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
