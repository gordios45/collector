// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// GDELT 2.0 events via the 15-minute public CSV dumps — no auth, no billing.
//
// Flow:
//  1. GET http://data.gdeltproject.org/gdeltv2/lastupdate.txt  (3 lines:
//     size, sha1, URL — for export, mentions, gkg respectively).
//  2. Download the first URL (*.export.CSV.zip).
//  3. Unzip in memory; parse the tab-separated CSV (61 fixed columns).
//  4. Emit one event per row with an ActionGeo lat/lon.
//
// Column schema per the GDELT 2.0 Event Codebook. Key indexes (0-based):
//
//	0  GlobalEventID   1  SQLDATE      6  Actor1Name   16 Actor2Name
//	26 EventCode      27 EventBaseCode  28 EventRootCode  29 QuadClass
//	30 GoldsteinScale 31 NumMentions   34 AvgTone
//	52 ActionGeo_FullName  53 ActionGeo_CountryCode
//	56 ActionGeo_Lat  57 ActionGeo_Long  59 DATEADDED  60 SOURCEURL
package gdelt

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const lastUpdateURL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

// Column indexes (0-based) into the GDELT 2.0 export CSV.
const (
	colGlobalEventID  = 0
	colSQLDate        = 1
	colActor1Name     = 6
	colActor2Name     = 16
	colEventCode      = 26
	colEventBaseCode  = 27
	colEventRootCode  = 28
	colQuadClass      = 29
	colGoldsteinScale = 30
	colNumMentions    = 31
	colNumSources     = 32
	colNumArticles    = 33
	colAvgTone        = 34
	colActionGeoFull  = 52
	colActionGeoCCode = 53
	colActionGeoLat   = 56
	colActionGeoLong  = 57
	colDateAdded      = 59
	colSourceURL      = 60
	expectedCols      = 61
)

// minMentions — ignore noise. Multi-mention events are the interesting ones.
const minMentions = 3

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "gdelt" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	// Step 1: resolve latest export ZIP URL.
	upd, err := httpx.GetBytes(ctx, lastUpdateURL, nil)
	if err != nil {
		return nil, fmt.Errorf("lastupdate: %w", err)
	}
	exportURL := ""
	for _, line := range strings.Split(strings.TrimSpace(string(upd)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		u := fields[2]
		if strings.HasSuffix(u, ".export.CSV.zip") {
			exportURL = u
			break
		}
	}
	if exportURL == "" {
		return nil, fmt.Errorf("lastupdate: no export URL")
	}

	// Step 2: download ZIP.
	zipBuf, err := httpx.GetBytes(ctx, exportURL, nil)
	if err != nil {
		return nil, fmt.Errorf("export zip: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(zipBuf), int64(len(zipBuf)))
	if err != nil {
		return nil, fmt.Errorf("zip open: %w", err)
	}
	if len(zr.File) == 0 {
		return nil, fmt.Errorf("empty zip")
	}

	// Step 3: unzip + parse CSV (one file expected).
	f, err := zr.File[0].Open()
	if err != nil {
		return nil, fmt.Errorf("zip entry: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.FieldsPerRecord = -1 // tolerate trailing blanks
	r.LazyQuotes = true

	out := []events.Event{}
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		if len(row) < expectedCols {
			continue
		}
		latStr := strings.TrimSpace(row[colActionGeoLat])
		lonStr := strings.TrimSpace(row[colActionGeoLong])
		if latStr == "" || lonStr == "" {
			continue
		}
		lat, err1 := strconv.ParseFloat(latStr, 64)
		lon, err2 := strconv.ParseFloat(lonStr, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		mentions, _ := strconv.Atoi(strings.TrimSpace(row[colNumMentions]))
		if mentions < minMentions {
			continue
		}

		id := strings.TrimSpace(row[colGlobalEventID])
		if id == "" {
			continue
		}
		ts := parseDateAdded(row[colDateAdded])
		goldstein, _ := strconv.ParseFloat(strings.TrimSpace(row[colGoldsteinScale]), 64)
		avgTone, _ := strconv.ParseFloat(strings.TrimSpace(row[colAvgTone]), 64)

		props := map[string]any{
			"global_event_id":    id,
			"sql_date":           row[colSQLDate],
			"actor1_name":        row[colActor1Name],
			"actor2_name":        row[colActor2Name],
			"event_code":         row[colEventCode],
			"event_base_code":    row[colEventBaseCode],
			"event_root_code":    row[colEventRootCode],
			"quad_class":         row[colQuadClass],
			"goldstein":          goldstein,
			"num_mentions":       mentions,
			"avg_tone":           avgTone,
			"actiongeo_fullname": row[colActionGeoFull],
			"country":            row[colActionGeoCCode],
			"source_url":         row[colSourceURL],
			"date_added":         row[colDateAdded],
		}
		out = append(out, events.Event{
			Ts: ts, Source: "gdelt", ExtID: id,
			Lat: lat, Lon: lon, Props: props,
		})
	}
	return out, nil
}

func parseDateAdded(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().UTC()
	}
	if t, err := time.Parse("20060102150405", s); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}
