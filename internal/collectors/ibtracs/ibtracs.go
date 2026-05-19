// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// IBTrACS — International Best Track Archive for Climate Stewardship.
//
// Normalized, RSMC-merged tropical-cyclone record across all basins with
// stable schema and unified storm IDs (SID = ssssYYYYBBnnn). Sits alongside
// `nhc` (NHC Atlantic + East Pacific) and `jtwc` (JTWC NW Pacific + Indian +
// South Hemisphere) as the global-coverage backstop: when an agency-specific
// feed lags, IBTrACS still ticks once NCEI's near-real-time export refreshes
// (multiple times per day during active periods).
//
// Source: NCEI v04r01 NRT subset
//
//	https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.last3years.list.v04r01.csv
//
// CSV layout: row 1 is column headers, row 2 is units (we skip), rows 3+
// are observations. We restrict to fixes within the last 14 days — older
// rows are present but already fused into per-storm tracks downstream and
// don't benefit from re-upsert.
//
// Per-fix coordinates and ISO_TIME are required; WMO_WIND/WMO_PRES are the
// preferred intensity fields, with USA_WIND/USA_PRES as a fallback for
// basins where no WMO RSMC submitted to the archive yet.
package ibtracs

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
)

const url = "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r01/access/csv/ibtracs.last3years.list.v04r01.csv"

const (
	fixWindow      = 14 * 24 * time.Hour
	requestTimeout = 90 * time.Second
)

type Collector struct {
	client *http.Client
}

func New() (*Collector, error) {
	return &Collector{client: &http.Client{Timeout: requestTimeout}}, nil
}

func (c *Collector) ID() string               { return "ibtracs" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept", "text/csv")
	r, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 400))
		return nil, fmt.Errorf("ibtracs %d: %s", r.StatusCode, string(body))
	}
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return parseCSV(buf, time.Now().UTC())
}

func parseCSV(buf []byte, now time.Time) ([]events.Event, error) {
	rd := csv.NewReader(bytes.NewReader(buf))
	rd.FieldsPerRecord = -1
	rd.ReuseRecord = false
	header, err := rd.Read()
	if err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	if _, err := rd.Read(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("units row: %w", err)
	}
	cols := indexHeader(header)
	if cols.sid < 0 || cols.iso < 0 || cols.lat < 0 || cols.lon < 0 {
		return nil, fmt.Errorf("required IBTrACS columns missing (have %v)", header)
	}
	cutoff := now.Add(-fixWindow)
	out := make([]events.Event, 0, 256)
	for {
		row, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(row) <= cols.lon {
			continue
		}
		ts, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(row[cols.iso]))
		if err != nil {
			continue
		}
		ts = ts.UTC()
		if ts.Before(cutoff) {
			continue
		}
		lat, err := strconv.ParseFloat(strings.TrimSpace(row[cols.lat]), 64)
		if err != nil {
			continue
		}
		lon, err := strconv.ParseFloat(strings.TrimSpace(row[cols.lon]), 64)
		if err != nil {
			continue
		}
		sid := strings.TrimSpace(row[cols.sid])
		if sid == "" {
			continue
		}
		wind := pickInt(row, cols.wmoWind, cols.usaWind)
		pres := pickInt(row, cols.wmoPres, cols.usaPres)
		props := map[string]any{
			"sid":      sid,
			"season":   field(row, cols.season),
			"basin":    field(row, cols.basin),
			"subbasin": field(row, cols.subbasin),
			"name":     field(row, cols.name),
			"nature":   field(row, cols.nature),
			"iso_time": ts.Format(time.RFC3339),
			"wind_kt":  wind,
			"pres_mb":  pres,
			"agency":   field(row, cols.agency),
		}
		collectorutil.AddTropicalCycloneScores(props, false)
		out = append(out, events.Event{
			Ts:     ts,
			Source: "ibtracs",
			ExtID:  sid + ":" + ts.Format("20060102T1504"),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out, nil
}

type colIdx struct {
	sid, season, basin, subbasin, name, iso, nature int
	lat, lon, wmoWind, wmoPres, usaWind, usaPres    int
	agency                                          int
}

func indexHeader(header []string) colIdx {
	idx := func(name string) int {
		for i, h := range header {
			if strings.EqualFold(strings.TrimSpace(h), name) {
				return i
			}
		}
		return -1
	}
	return colIdx{
		sid:      idx("SID"),
		season:   idx("SEASON"),
		basin:    idx("BASIN"),
		subbasin: idx("SUBBASIN"),
		name:     idx("NAME"),
		iso:      idx("ISO_TIME"),
		nature:   idx("NATURE"),
		lat:      idx("LAT"),
		lon:      idx("LON"),
		wmoWind:  idx("WMO_WIND"),
		wmoPres:  idx("WMO_PRES"),
		usaWind:  idx("USA_WIND"),
		usaPres:  idx("USA_PRES"),
		agency:   idx("WMO_AGENCY"),
	}
}

func field(row []string, i int) string {
	if i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

// pickInt returns the first non-empty integer value across the given indices.
// IBTrACS often has WMO_WIND filled while WMO_PRES is blank (or vice versa),
// with the USA_* column filled in either case.
func pickInt(row []string, idxs ...int) int {
	for _, i := range idxs {
		s := field(row, i)
		if s == "" {
			continue
		}
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int(f)
		}
	}
	return 0
}
