// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA/NCEI Severe Weather Data Inventory radar-derived signatures.
package swdi_radar_signatures

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const baseURL = "https://www.ncdc.noaa.gov/swdiws/csv"

const (
	maxRowsPerDataset = 750
	recentWindow      = 24 * time.Hour
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "swdi_radar_signatures" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

type datasetDef struct {
	Name string
}

var datasets = []datasetDef{
	{Name: "nx3tvs"},
	{Name: "nx3hail"},
	{Name: "nx3meso"},
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	start := now.AddDate(0, 0, -1).Format("20060102")
	end := now.Format("20060102")
	out := []events.Event{}
	for _, ds := range datasets {
		url := fmt.Sprintf("%s/%s/%s:%s?limit=1000", baseURL, ds.Name, start, end)
		buf, err := httpx.GetBytes(ctx, url, nil)
		if err != nil {
			return nil, err
		}
		rows, err := eventsFromCSV(ds.Name, url, buf)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ds.Name, err)
		}
		out = append(out, recentRows(rows, now.Add(-recentWindow), maxRowsPerDataset)...)
	}
	return out, nil
}

func eventsFromCSV(dataset, sourceURL string, buf []byte) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	r.FieldsPerRecord = -1
	header := []string{}
	out := []events.Event{}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if len(rec) == 0 {
			continue
		}
		if len(header) == 0 {
			if strings.EqualFold(strings.TrimSpace(rec[0]), "summary") {
				return out, nil
			}
			header = normalizeHeader(rec)
			continue
		}
		row := mapRow(header, rec)
		ev, ok := eventFromRow(dataset, sourceURL, row)
		if ok {
			out = append(out, ev)
		}
	}
}

func eventFromRow(dataset, sourceURL string, row map[string]string) (events.Event, bool) {
	lat, ok1 := parseFloat(row["lat"])
	lon, ok2 := parseFloat(row["lon"])
	if !ok1 || !ok2 || (lat == 0 && lon == 0) {
		return events.Event{}, false
	}
	ts := parseTime(row["ztime"])
	if ts.IsZero() {
		return events.Event{}, false
	}
	wsr := row["wsr_id"]
	cell := row["cell_id"]
	if wsr == "" || cell == "" {
		return events.Event{}, false
	}
	props := map[string]any{
		"dataset":             dataset,
		"ztime":               row["ztime"],
		"wsr_id":              wsr,
		"cell_id":             cell,
		"lat":                 lat,
		"lon":                 lon,
		"source_api_endpoint": sourceURL,
	}
	for k, v := range row {
		if _, exists := props[k]; exists || v == "" {
			continue
		}
		if f, ok := parseFloat(v); ok {
			props[k] = f
		} else {
			props[k] = v
		}
	}
	return events.Event{
		Ts:     ts,
		Source: "swdi_radar_signatures",
		ExtID:  fmt.Sprintf("%s:%s:%s:%s", dataset, row["ztime"], wsr, cell),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func recentRows(rows []events.Event, since time.Time, limit int) []events.Event {
	if limit <= 0 {
		return nil
	}
	recent := make([]events.Event, 0, len(rows))
	for _, ev := range rows {
		if ev.Ts.Before(since) {
			continue
		}
		recent = append(recent, ev)
	}
	if len(recent) <= limit {
		return recent
	}
	return recent[len(recent)-limit:]
}

func normalizeHeader(rec []string) []string {
	out := make([]string, len(rec))
	for i, v := range rec {
		out[i] = strings.ToLower(strings.TrimSpace(v))
	}
	return out
}

func mapRow(header, rec []string) map[string]string {
	out := map[string]string{}
	for i, key := range header {
		if i < len(rec) {
			out[key] = strings.TrimSpace(rec[i])
		}
	}
	return out
}

func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(s)); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

func parseFloat(s string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v, err == nil
}
