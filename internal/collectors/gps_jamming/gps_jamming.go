// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// GPSJAM — daily maps of possible GPS interference, grouped into H3 cells.
//
// Data URL pattern (discovered by inspecting gpsjam.org/views/index.ejs):
//
//	https://gpsjam.org/data/{YYYY-MM-DD}-h3_4.csv
//
// CSV columns: hex, count_good_aircraft, count_bad_aircraft
// We decode each H3 index (resolution 4 → ~288 km across) to a centroid
// lat/lon and emit one event per cell that has bad-aircraft hits.
package gps_jamming

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/uber/h3-go/v4"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "gps_jamming" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	// gpsjam publishes daily with some lag. Walk the last 7 days and ingest
	// every available file. Fetching only the newest file can permanently miss
	// an intermediate day when GPSJam publishes with an irregular delay.
	out := []events.Event{}
	var found int
	for i := 1; i <= 7; i++ {
		d := time.Now().UTC().Add(time.Duration(-i*24) * time.Hour).Format("2006-01-02")
		url := fmt.Sprintf("https://gpsjam.org/data/%s-h3_4.csv", d)
		b, err := httpx.GetBytes(ctx, url, nil)
		if err != nil || len(b) == 0 {
			continue
		}
		evs, err := parseGPSJamCSV(b, d)
		if err != nil {
			return nil, err
		}
		found++
		out = append(out, evs...)
	}
	if found == 0 {
		return nil, fmt.Errorf("no gpsjam CSV found in last 7 days")
	}
	return out, nil
}

func parseGPSJamCSV(buf []byte, dateStr string) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	hdr, err := r.Read()
	if err != nil {
		return nil, err
	}
	col := map[string]int{}
	for i, h := range hdr {
		col[h] = i
	}
	hexI, okHex := col["hex"]
	goodI := col["count_good_aircraft"]
	badI := col["count_bad_aircraft"]
	if !okHex {
		return nil, fmt.Errorf("gpsjam csv missing 'hex' column")
	}

	ts, _ := time.Parse("2006-01-02", dateStr)
	out := []events.Event{}
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		hexStr := row[hexI]
		good, _ := strconv.Atoi(row[goodI])
		bad, _ := strconv.Atoi(row[badI])
		if bad == 0 {
			continue // only cells with jamming signals are worth storing
		}
		raw := h3.IndexFromString(hexStr)
		if raw == 0 {
			continue
		}
		latlng, err := h3.CellToLatLng(h3.Cell(raw))
		if err != nil {
			continue
		}
		// Compute a rough "intensity" = bad / (good + bad).
		total := good + bad
		intensity := 0.0
		if total > 0 {
			intensity = float64(bad) / float64(total)
		}
		out = append(out, events.Event{
			Ts: ts.UTC(), Source: "gps_jamming", ExtID: dateStr + "_" + hexStr,
			Lat: latlng.Lat, Lon: latlng.Lng,
			Props: map[string]any{
				"h3_cell":             hexStr,
				"h3_resolution":       4,
				"count_good_aircraft": good,
				"count_bad_aircraft":  bad,
				"intensity":           intensity,
				"intensity_score":     collectorutil.GPSJammingIntensityScore(intensity, bad),
				"date":                dateStr,
			},
		})
	}
	return out, nil
}
