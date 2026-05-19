// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package gwis_fire_danger samples the Copernicus/JRC Global Wildfire
// Information System Fire Weather Index map over active AOIs. The WMS endpoint
// is keyless; the resulting signal is environmental context, not fire
// confirmation.
package gwis_fire_danger

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/png"
	"math"
	"net/url"
	"os"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceID = "gwis_fire_danger"
	wmsURL   = "https://maps.effis.emergency.copernicus.eu/gwis"
)

type Collector struct {
	pool    *pgxpool.Pool
	maxAOIs int
}

var httpClient = collectorutil.HTTPClient(90 * time.Second)

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_GWIS_FIRE_DANGER") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_GWIS_FIRE_DANGER=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:    pool,
		maxAOIs: collectorutil.EnvInt("GWIS_FIRE_DANGER_MAX_AOIS", 24, 4, 80),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	endpoint := fireDangerMapURL()
	buf, err := httpx.GetBytesWithClient(ctx, httpClient, endpoint, map[string]string{"Accept": "image/png"})
	if err != nil {
		return nil, err
	}
	img, err := png.Decode(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 7*24*time.Hour, collectorutil.StrategicAOIs)
	now := time.Now().UTC().Truncate(time.Hour)
	out := make([]events.Event, 0, len(aois))
	for _, aoi := range aois {
		r, g, b, ok := sampleRGB(img, aoi.Lat, aoi.Lon)
		if !ok {
			continue
		}
		score := fireDangerScore(r, g, b)
		props := map[string]any{
			"source_provider":           "Copernicus/JRC Global Wildfire Information System",
			"source_api_endpoint":       endpoint,
			"source_public_url":         "https://gwis.jrc.ec.europa.eu/apps/gwis_current_situation/",
			"source_provider_kind":      "fire_weather_model",
			"watch_aoi_id":              aoi.ID,
			"watch_aoi_kind":            aoi.Kind,
			"watch_aoi_label":           aoi.Label,
			"layer":                     "ecmwf.fwi",
			"fire_danger_display_r":     r,
			"fire_danger_display_g":     g,
			"fire_danger_display_b":     b,
			"fwi_score":                 round(score, 2),
			"fire_danger_context_score": round(score, 2),
			"fire_danger_context_only":  true,
			"source_payload_validity":   validity(now, now.Add(48*time.Hour), "gwis_ecmwf_fwi_display_window"),
		}
		out = append(out, events.Event{
			Ts:     now,
			Source: sourceID,
			ExtID:  fmt.Sprintf("ecmwf-fwi:%s:%s", now.Format("20060102T15"), collectorutil.StableID(aoi.ID)),
			Lat:    aoi.Lat,
			Lon:    aoi.Lon,
			Props:  props,
		})
	}
	return out, nil
}

func fireDangerMapURL() string {
	q := url.Values{}
	q.Set("SERVICE", "WMS")
	q.Set("VERSION", "1.3.0")
	q.Set("REQUEST", "GetMap")
	q.Set("LAYERS", "ecmwf.fwi")
	q.Set("STYLES", "")
	q.Set("CRS", "EPSG:4326")
	q.Set("BBOX", "-90,-180,90,180")
	q.Set("WIDTH", "720")
	q.Set("HEIGHT", "360")
	q.Set("FORMAT", "image/png")
	q.Set("TRANSPARENT", "false")
	return wmsURL + "?" + q.Encode()
}

func sampleRGB(img image.Image, lat, lon float64) (int, int, int, bool) {
	if img == nil || !collectorutil.ValidLatLon(lat, lon) {
		return 0, 0, 0, false
	}
	bounds := img.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	if w <= 0 || h <= 0 {
		return 0, 0, 0, false
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
	rr, gg, bb, aa := img.At(bounds.Min.X+x, bounds.Min.Y+y).RGBA()
	if aa == 0 {
		return 0, 0, 0, false
	}
	return int(rr / 257), int(gg / 257), int(bb / 257), true
}

func fireDangerScore(r, g, b int) float64 {
	if r > 238 && g > 238 && b > 238 {
		return 0
	}
	if r < 20 && g < 20 && b < 20 {
		return 0
	}
	score := 0.0
	switch {
	case r >= 180 && g < 90 && b >= 120:
		score = 2.8 // purple/extreme display classes.
	case r >= 210 && g < 120 && b < 120:
		score = 2.4 // red/high display classes.
	case r >= 220 && g >= 110 && g < 200 && b < 120:
		score = 1.7 // orange.
	case r >= 220 && g >= 190 && b < 150:
		score = 1.1 // yellow.
	case r >= 140 && g < 140 && b < 140:
		score = 1.0
	}
	redness := float64(r-maxInt(g, b)) / 120.0
	if redness > 0 {
		score = math.Max(score, redness)
	}
	return propx.ClampFloat(score, 0, 3)
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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
