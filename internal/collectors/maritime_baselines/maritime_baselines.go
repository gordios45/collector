// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package maritime_baselines monitors public maritime baseline products:
// MarineCadastre historical AIS file catalogs and EMODnet vessel-density
// metadata. Heavy source files are not downloaded by the scheduled collector.
package maritime_baselines

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	marineCadastreSourceID = "marinecadastre_ais_catalog"
	emodnetSourceID        = "emodnet_vessel_density"
	emodnetInfoURL         = "https://erddap.emodnet.eu/erddap/info/humanactivities_e929_c26d_18a2/index.json"
	emodnetGridURL         = "https://erddap.emodnet.eu/erddap/griddap/humanactivities_e929_c26d_18a2.html"
	emodnetWMSURL          = "https://erddap.emodnet.eu/erddap/wms/humanactivities_e929_c26d_18a2/index.html"
)

type MarineCadastreAISCatalog struct {
	year     int
	maxFiles int
}

func NewMarineCadastreAISCatalog() (*MarineCadastreAISCatalog, error) {
	year := envInt("MARINECADASTRE_AIS_YEAR", time.Now().UTC().Year()-1, 2009, time.Now().UTC().Year())
	return &MarineCadastreAISCatalog{
		year:     year,
		maxFiles: envInt("MARINECADASTRE_AIS_MAX_FILES", 30, 1, 366),
	}, nil
}

func (c *MarineCadastreAISCatalog) ID() string               { return marineCadastreSourceID }
func (c *MarineCadastreAISCatalog) PollEvery() time.Duration { return 24 * time.Hour }

func (c *MarineCadastreAISCatalog) Fetch(ctx context.Context) ([]events.Event, error) {
	indexURL := marineCadastreIndexURL(c.year)
	buf, err := httpx.GetBytes(ctx, indexURL, map[string]string{"Accept": "text/html,*/*"})
	if err != nil {
		return nil, err
	}
	files := parseMarineCadastreIndex(indexURL, string(buf))
	sort.Slice(files, func(i, j int) bool { return files[i].Date.After(files[j].Date) })
	if len(files) > c.maxFiles {
		files = files[:c.maxFiles]
	}
	out := make([]events.Event, 0, len(files))
	for _, f := range files {
		props := map[string]any{
			"source_provider":         "NOAA Office for Coastal Management / MarineCadastre.gov",
			"product":                 "Nationwide AIS CSV daily file",
			"coverage":                "U.S. coastal and inland waters",
			"file_name":               f.Name,
			"file_url":                f.URL,
			"compressed_size":         f.Size,
			"last_modified":           f.LastModified,
			"index_url":               indexURL,
			"download_policy":         "catalog_only_no_bulk_download",
			"source_payload_validity": validity(f.Date, 400*24*time.Hour, "marinecadastre_daily_ais_file_date"),
		}
		out = append(out, events.Event{
			Ts:     f.Date,
			Source: marineCadastreSourceID,
			ExtID:  f.Name,
			Props:  props,
		})
	}
	return out, nil
}

type marineCadastreFile struct {
	Name         string
	URL          string
	Date         time.Time
	LastModified string
	Size         string
}

var aisFileRe = regexp.MustCompile(`href="(ais-([0-9]{4})-([0-9]{2})-([0-9]{2})\.csv\.zst)">[^<]+</a></td><td class="date">([^<]+)</td><td class="size">([^<]+)</td>`)

func parseMarineCadastreIndex(indexURL, html string) []marineCadastreFile {
	var out []marineCadastreFile
	for _, m := range aisFileRe.FindAllStringSubmatch(html, -1) {
		ts, err := time.Parse("2006-01-02", fmt.Sprintf("%s-%s-%s", m[2], m[3], m[4]))
		if err != nil {
			continue
		}
		out = append(out, marineCadastreFile{
			Name:         m[1],
			URL:          strings.TrimRight(indexURL, "/") + "/" + m[1],
			Date:         ts.UTC(),
			LastModified: strings.TrimSpace(m[5]),
			Size:         strings.TrimSpace(m[6]),
		})
	}
	return out
}

func marineCadastreIndexURL(year int) string {
	return fmt.Sprintf("https://noaaocm.blob.core.windows.net/ais/csv2/csv%d/index.html", year)
}

type EMODnetVesselDensity struct{}

func NewEMODnetVesselDensity() (*EMODnetVesselDensity, error) { return &EMODnetVesselDensity{}, nil }

func (c *EMODnetVesselDensity) ID() string               { return emodnetSourceID }
func (c *EMODnetVesselDensity) PollEvery() time.Duration { return 24 * time.Hour }

func (c *EMODnetVesselDensity) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw erddapInfo
	if err := httpx.GetJSON(ctx, emodnetInfoURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil, err
	}
	ev, ok := eventFromEMODnetInfo(raw, time.Now().UTC())
	if !ok {
		return nil, nil
	}
	return []events.Event{ev}, nil
}

type erddapInfo struct {
	Table struct {
		Rows [][]string `json:"rows"`
	} `json:"table"`
}

func eventFromEMODnetInfo(raw erddapInfo, now time.Time) (events.Event, bool) {
	attrs := map[string]string{}
	for _, row := range raw.Table.Rows {
		if len(row) < 5 {
			continue
		}
		key := row[1] + "." + row[2]
		attrs[key] = row[4]
	}
	if len(attrs) == 0 {
		return events.Event{}, false
	}
	props := map[string]any{
		"source_provider":        "EMODnet Human Activities / ERDDAP",
		"dataset_id":             "humanactivities_e929_c26d_18a2",
		"product":                "Vessel Density",
		"institution":            attrs["NC_GLOBAL.institution"],
		"license":                attrs["NC_GLOBAL.license"],
		"time_actual_range":      attrs["time.actual_range"],
		"latitude_actual_range":  attrs["latitude.actual_range"],
		"longitude_actual_range": attrs["longitude.actual_range"],
		"variable":               "vd",
		"variable_units":         attrs["vd.units"],
		"info_url":               emodnetInfoURL,
		"griddap_url":            emodnetGridURL,
		"wms_url":                emodnetWMSURL,
		"download_policy":        "catalog_only_no_raster_download",
		"source_payload_validity": map[string]any{
			"valid_start":    now.Format(time.RFC3339),
			"valid_end":      now.Add(7 * 24 * time.Hour).Format(time.RFC3339),
			"validity_basis": "emodnet_erddap_metadata_refresh",
		},
	}
	return events.Event{
		Ts:     now,
		Source: emodnetSourceID,
		ExtID:  "humanactivities_e929_c26d_18a2",
		Props:  props,
	}, true
}

func envInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func validity(start time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      start.Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func decodeERDDAPInfo(buf []byte) (erddapInfo, error) {
	var raw erddapInfo
	err := json.Unmarshal(buf, &raw)
	return raw, err
}
