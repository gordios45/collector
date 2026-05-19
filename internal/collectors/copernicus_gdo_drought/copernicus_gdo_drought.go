// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package copernicus_gdo_drought samples the Copernicus Global Drought
// Observatory Combined Drought Indicator over active AOIs. It is context only:
// a drought-prone AOI should influence weather/fire/food-security hypotheses,
// not create a fresh incident by itself.
package copernicus_gdo_drought

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"math"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/image/tiff"
)

const (
	sourceID = "copernicus_gdo_drought"
	wcsURL   = "https://drought.emergency.copernicus.eu/api/wcs"
	wmsURL   = "https://drought.emergency.copernicus.eu/api/wms"
)

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

var httpClient = collectorutil.HTTPClient(120 * time.Second)

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_COPERNICUS_GDO_DROUGHT") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_COPERNICUS_GDO_DROUGHT=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:    pool,
		maxAOIs: collectorutil.EnvInt("COPERNICUS_GDO_MAX_AOIS", 24, 4, 80),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 12 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	img, productDate, endpoint, err := fetchLatestCDI(ctx, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 14*24*time.Hour, collectorutil.StrategicAOIs)
	out := make([]events.Event, 0, len(aois))
	for _, aoi := range aois {
		value, valid := sampleWorldImage(img, aoi.Lat, aoi.Lon)
		if !valid {
			continue
		}
		score := cdiScore(value)
		props := map[string]any{
			"source_provider":         "Copernicus Emergency Management Service - Global Drought Observatory",
			"source_api_endpoint":     endpoint,
			"source_public_url":       wmsURL,
			"source_provider_kind":    "drought_indicator_model",
			"watch_aoi_id":            aoi.ID,
			"watch_aoi_kind":          aoi.Kind,
			"watch_aoi_label":         aoi.Label,
			"product":                 "Combined Drought Indicator",
			"coverage_id":             "cdiad",
			"product_date":            productDate.Format("2006-01-02"),
			"cdi_pixel_value":         round(value, 2),
			"cdi_alert_score":         round(score, 2),
			"drought_context_score":   round(score, 2),
			"drought_context_only":    true,
			"source_payload_validity": validity(productDate, productDate.Add(35*24*time.Hour), "copernicus_gdo_cdi_product_window"),
		}
		out = append(out, events.Event{
			Ts:     productDate,
			Source: sourceID,
			ExtID:  fmt.Sprintf("cdi:%s:%s", productDate.Format("20060102"), collectorutil.StableID(aoi.ID)),
			Lat:    aoi.Lat,
			Lon:    aoi.Lon,
			Props:  props,
		})
	}
	return out, nil
}

func fetchLatestCDI(ctx context.Context, now time.Time) (image.Image, time.Time, string, error) {
	var firstErr error
	for _, d := range candidateDates(now) {
		endpoint := cdiCoverageURL(d)
		buf, err := httpx.GetBytesWithClient(ctx, httpClient, endpoint, map[string]string{"Accept": "image/tiff,*/*"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		img, err := tiff.Decode(bytes.NewReader(buf))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return img, d, endpoint, nil
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("no candidate GDO CDI dates")
	}
	return nil, time.Time{}, "", firstErr
}

func cdiCoverageURL(d time.Time) string {
	q := url.Values{}
	q.Set("map", "DO_WCS")
	q.Set("SERVICE", "WCS")
	q.Set("VERSION", "2.0.0")
	q.Set("REQUEST", "GetCoverage")
	q.Set("coverageID", "cdiad")
	q.Set("CRS", "EPSG:4326")
	q.Set("format", "GEOTIFF")
	q.Set("TIME", d.Format("2006-01-02"))
	return wcsURL + "?" + q.Encode()
}

func candidateDates(now time.Time) []time.Time {
	dates := []time.Time{}
	cutoff := now.Add(-14 * 24 * time.Hour)
	for monthBack := 0; monthBack < 5; monthBack++ {
		base := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -monthBack, 0)
		for _, day := range []int{1, 11, 21} {
			d := time.Date(base.Year(), base.Month(), day, 0, 0, 0, 0, time.UTC)
			if d.After(cutoff) {
				continue
			}
			dates = append(dates, d)
		}
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i].After(dates[j]) })
	return dates
}

func sampleWorldImage(img image.Image, lat, lon float64) (float64, bool) {
	if img == nil || !collectorutil.ValidLatLon(lat, lon) {
		return 0, false
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	if w <= 0 || h <= 0 {
		return 0, false
	}
	x := int(math.Floor((lon + 180.0) / 360.0 * float64(w)))
	y := int(math.Floor((90.0 - lat) / 180.0 * float64(h)))
	if x < 0 {
		x = 0
	}
	if x >= w {
		x = w - 1
	}
	if y < 0 {
		y = 0
	}
	if y >= h {
		y = h - 1
	}
	r, g, b2, _ := img.At(b.Min.X+x, b.Min.Y+y).RGBA()
	gray := float64(r+g+b2) / 3.0 / 257.0
	if gray >= 250 {
		return 0, false
	}
	return gray, true
}

func cdiScore(value float64) float64 {
	switch {
	case value <= 0:
		return 0
	case value <= 4:
		return propx.ClampFloat(value, 0, 3)
	default:
		return propx.ClampFloat(value/85.0, 0, 3)
	}
}

func validity(start, end time.Time, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}
