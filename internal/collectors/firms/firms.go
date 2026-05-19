// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NASA FIRMS — active fire detections, last 24h.
//
// This collector uses the keyless FIRMS active_fire CSV mirrors for the main
// polar-orbiting thermal products. The API-keyed FIRMS service can expose
// additional geostationary products, but these keyless feeds keep the primary
// strike/thermal path alive without requiring credentials.
package firms

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

type feed struct {
	ID         string
	URL        string
	Platform   string
	Instrument string
	Product    string
}

var feeds = []feed{
	{
		ID:         "VIIRS_SNPP_NRT",
		URL:        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv",
		Platform:   "Suomi-NPP",
		Instrument: "VIIRS",
		Product:    "suomi-npp-viirs-c2",
	},
	{
		ID:         "VIIRS_NOAA20_NRT",
		URL:        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv",
		Platform:   "NOAA-20",
		Instrument: "VIIRS",
		Product:    "noaa-20-viirs-c2",
	},
	{
		ID:         "VIIRS_NOAA21_NRT",
		URL:        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-21-viirs-c2/csv/J2_VIIRS_C2_Global_24h.csv",
		Platform:   "NOAA-21",
		Instrument: "VIIRS",
		Product:    "noaa-21-viirs-c2",
	},
	{
		ID:         "MODIS_NRT",
		URL:        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv",
		Platform:   "Terra/Aqua",
		Instrument: "MODIS",
		Product:    "modis-c6.1",
	},
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "firms" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	var errs []error
	now := time.Now().UTC()
	for _, f := range feeds {
		buf, err := httpx.GetBytes(ctx, f.URL, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.ID, err))
			continue
		}
		evs, err := parseFIRMSCSV(f, buf, now)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.ID, err))
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

func parseFIRMSCSV(f feed, buf []byte, fallbackTS time.Time) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	hdr, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := make(map[string]int, len(hdr))
	for i, h := range hdr {
		idx[strings.TrimSpace(h)] = i
	}
	need := []string{"latitude", "longitude", "acq_date", "acq_time"}
	for _, k := range need {
		if _, ok := idx[k]; !ok {
			return nil, errors.New("firms: missing column " + k)
		}
	}

	out := make([]events.Event, 0, 1024)
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		lat, _ := strconv.ParseFloat(row[idx["latitude"]], 64)
		lon, _ := strconv.ParseFloat(row[idx["longitude"]], 64)
		if lat == 0 && lon == 0 {
			continue
		}
		tsStr := row[idx["acq_date"]] + " " + row[idx["acq_time"]]
		ts, err := time.Parse("2006-01-02 1504", tsStr)
		if err != nil {
			ts = fallbackTS
		}
		props := map[string]any{}
		for k, i := range idx {
			if i < len(row) {
				props[k] = row[i]
			}
		}
		passID := thermalPassID(f, ts, row[idx["latitude"]], row[idx["longitude"]])
		props["data_id"] = f.ID
		props["platform"] = f.Platform
		props["instrument"] = f.Instrument
		props["product"] = f.Product
		props["thermal_pass_id"] = passID
		props["observed_start"] = ts.UTC().Format(time.RFC3339)
		props["observed_end"] = ts.UTC().Format(time.RFC3339)
		props["source_payload_validity"] = map[string]any{
			"valid_start":    ts.UTC().Format(time.RFC3339),
			"valid_end":      ts.UTC().Add(6 * time.Hour).Format(time.RFC3339),
			"validity_basis": "firms_nrt_active_fire_detection",
		}
		props["source_api_endpoint"] = f.URL
		addThermalScores(props, idx, row)
		// Natural key: lat+lon+time should be close to unique.
		ext := passID
		out = append(out, events.Event{
			Ts: ts.UTC(), Source: "firms", ExtID: ext,
			Lat: lat, Lon: lon, Props: props,
		})
	}
	return out, nil
}

func addThermalScores(props map[string]any, idx map[string]int, row []string) {
	if frp, ok := columnFloat(idx, row, "frp"); ok {
		props["frp_score"] = collectorutil.FIRMSFRPScore(frp)
	}
	if brightness, ok := columnFloat(idx, row, "bright_ti4", "brightness"); ok {
		props["brightness_score"] = collectorutil.ThermalBrightnessScore(brightness, 20)
	}
}

func columnFloat(idx map[string]int, row []string, keys ...string) (float64, bool) {
	for _, key := range keys {
		i, ok := idx[key]
		if !ok || i >= len(row) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(row[i]), 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func thermalPassID(f feed, ts time.Time, lat, lon string) string {
	return strings.Join([]string{
		f.ID,
		ts.UTC().Format("20060102T1504Z"),
		strings.TrimSpace(lat),
		strings.TrimSpace(lon),
	}, "_")
}
