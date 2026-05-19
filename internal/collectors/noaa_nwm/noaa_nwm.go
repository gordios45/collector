// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA National Water Model product availability probe.
//
// This is intentionally a lightweight layer: it confirms recent public NWM
// NetCDF products on NOMADS and exposes run/product metadata. Pixel/feature
// extraction from NetCDF can build on these stable product URLs later.
package noaa_nwm

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const (
	productBaseURL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod"
	docsURL        = "https://www.nco.ncep.noaa.gov/pmb/products/nwm/"
)

type Collector struct {
	client *http.Client
}

func New() (*Collector, error) {
	return &Collector{client: &http.Client{Timeout: 10 * time.Second}}, nil
}

func (c *Collector) ID() string               { return "noaa_nwm" }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

type productDef struct {
	Product      string
	Domain       string
	Path         string
	FileTemplate string
	Lat          float64
	Lon          float64
}

var products = []productDef{
	{
		Product:      "analysis_assim_channel_rt",
		Domain:       "conus",
		Path:         "analysis_assim",
		FileTemplate: "nwm.t%sz.analysis_assim.channel_rt.tm00.conus.nc",
		Lat:          39.5,
		Lon:          -98.35,
	},
	{
		Product:      "short_range_channel_rt_f001",
		Domain:       "conus",
		Path:         "short_range",
		FileTemplate: "nwm.t%sz.short_range.channel_rt.f001.conus.nc",
		Lat:          39.5,
		Lon:          -98.35,
	},
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(products))
	for _, p := range products {
		ev, ok := c.latestAvailable(ctx, p, now)
		if ok {
			out = append(out, ev)
		}
	}
	if len(out) == 0 {
		out = append(out, metadataFallback(now))
	}
	return out, nil
}

func (c *Collector) latestAvailable(ctx context.Context, p productDef, now time.Time) (events.Event, bool) {
	for i := 0; i < 18; i++ {
		cycle := now.Add(-time.Duration(i) * time.Hour).Truncate(time.Hour)
		productURL := productURL(p, cycle)
		ok, size, lm := c.headOK(ctx, productURL)
		if !ok {
			continue
		}
		props := map[string]any{
			"product":             p.Product,
			"domain":              p.Domain,
			"cycle":               cycle.Format("2006010215"),
			"cycle_time":          cycle.Format(time.RFC3339),
			"path":                p.Path,
			"product_url":         productURL,
			"content_length":      size,
			"last_modified":       lm,
			"integration_state":   "product_available",
			"hydrology_analysis":  "pending_netcdf_sampling",
			"source_api_endpoint": productBaseURL,
			"docs_url":            docsURL,
		}
		return events.Event{
			Ts:     cycle,
			Source: "noaa_nwm",
			ExtID:  fmt.Sprintf("%s:%s:%s", p.Product, p.Domain, cycle.Format("2006010215")),
			Lat:    p.Lat,
			Lon:    p.Lon,
			Props:  props,
		}, true
	}
	return events.Event{}, false
}

func (c *Collector) headOK(ctx context.Context, url string) (bool, int64, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, 0, ""
	}
	req.Header.Set("User-Agent", httpxUserAgent())
	res, err := c.client.Do(req)
	if err != nil {
		return false, 0, ""
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return false, 0, ""
	}
	return true, res.ContentLength, res.Header.Get("Last-Modified")
}

func productURL(p productDef, cycle time.Time) string {
	return fmt.Sprintf("%s/nwm.%s/%s/%s",
		productBaseURL,
		cycle.Format("20060102"),
		p.Path,
		fmt.Sprintf(p.FileTemplate, cycle.Format("15")),
	)
}

func metadataFallback(now time.Time) events.Event {
	props := map[string]any{
		"product":             "nwm_product_catalog",
		"domain":              "conus",
		"cycle":               now.Truncate(time.Hour).Format("2006010215"),
		"product_url":         docsURL,
		"integration_state":   "product_catalog_only",
		"hydrology_analysis":  "pending_netcdf_sampling",
		"source_api_endpoint": docsURL,
	}
	return events.Event{
		Ts:     now,
		Source: "noaa_nwm",
		ExtID:  "nwm_product_catalog:" + now.Truncate(time.Hour).Format("2006010215"),
		Lat:    39.5,
		Lon:    -98.35,
		Props:  props,
	}
}

func httpxUserAgent() string {
	return "gordios/0.1 (+https://github.com/gordios)"
}
