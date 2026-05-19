// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package eia_wpsr ingests selected machine-readable CSV tables from EIA's
// Weekly Petroleum Status Report.
package eia_wpsr

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const sourceID = "eia_wpsr"

type tableSpec struct {
	ID   string
	Name string
	URL  string
}

var defaultTables = []tableSpec{
	{
		ID:   "table1",
		Name: "U.S. Petroleum Balance Sheet",
		URL:  "https://ir.eia.gov/wpsr/table1.csv",
	},
	{
		ID:   "table9",
		Name: "U.S. and PAD District Weekly Estimates",
		URL:  "https://ir.eia.gov/wpsr/table9.csv",
	},
}

type Collector struct {
	tables []tableSpec
}

func New() (*Collector, error) {
	return &Collector{tables: defaultTables}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, spec := range c.tables {
		buf, err := httpx.GetBytes(ctx, spec.URL, map[string]string{"Accept": "text/csv,*/*"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		evs, err := eventsFromCSV(spec, buf)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func eventsFromCSV(spec tableSpec, buf []byte) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(bytes.Trim(buf, "\x1a\r\n\t ")))
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s csv: %w", spec.ID, err)
	}
	out := []events.Event{}
	var header []string
	for _, row := range rows {
		row = trimRow(row)
		if len(row) == 0 {
			continue
		}
		if isStubHeader(row) {
			header = row
			continue
		}
		ev, ok := eventFromRow(spec, header, row)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromRow(spec tableSpec, header, row []string) (events.Event, bool) {
	currentIdx, reportWeek, ok := currentValueColumn(header)
	if !ok || len(row) <= currentIdx {
		return events.Event{}, false
	}
	value, ok := parseNumber(row[currentIdx])
	if !ok {
		return events.Event{}, false
	}
	descriptors := rowDescriptors(row[:currentIdx])
	if len(descriptors) == 0 {
		return events.Event{}, false
	}
	metric := descriptors[len(descriptors)-1]
	category := ""
	if len(descriptors) > 1 {
		category = strings.Join(descriptors[:len(descriptors)-1], " / ")
	}
	props := map[string]any{
		"table":                   spec.ID,
		"table_name":              spec.Name,
		"category":                category,
		"metric":                  metric,
		"descriptors":             descriptors,
		"report_week":             reportWeek.Format("2006-01-02"),
		"value":                   value,
		"unit":                    inferUnit(spec, descriptors),
		"comparisons":             comparisonValues(header, row, currentIdx),
		"source_api_endpoint":     spec.URL,
		"source_payload_validity": validity(reportWeek),
	}
	return events.Event{
		Ts:     reportWeek,
		Source: sourceID,
		ExtID:  stableID(spec.ID + ":" + strings.Join(descriptors, ":") + ":" + reportWeek.Format("2006-01-02")),
		Lat:    39.5,
		Lon:    -98.35,
		Props:  props,
	}, true
}

func trimRow(row []string) []string {
	for len(row) > 0 && strings.TrimSpace(strings.Trim(row[len(row)-1], "\x1a")) == "" {
		row = row[:len(row)-1]
	}
	for i := range row {
		row[i] = strings.TrimSpace(strings.Trim(row[i], "\x1a"))
	}
	return row
}

func isStubHeader(row []string) bool {
	return len(row) > 0 && strings.EqualFold(strings.TrimSpace(row[0]), "STUB_1")
}

func currentValueColumn(header []string) (int, time.Time, bool) {
	for i, h := range header {
		if t, ok := parseWPSRDate(h); ok {
			return i, t, true
		}
	}
	return 0, time.Time{}, false
}

func rowDescriptors(cells []string) []string {
	out := []string{}
	for _, cell := range cells {
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		cell = rowNumberPrefix.ReplaceAllString(cell, "")
		cell = strings.Join(strings.Fields(cell), " ")
		if cell != "" {
			out = append(out, cell)
		}
	}
	return out
}

var rowNumberPrefix = regexp.MustCompile(`^\(\d+\)\s*`)

func comparisonValues(header, row []string, currentIdx int) map[string]any {
	out := map[string]any{}
	seen := map[string]int{}
	for i := currentIdx + 1; i < len(header) && i < len(row); i++ {
		label := strings.TrimSpace(header[i])
		if label == "" || strings.HasPrefix(strings.ToUpper(label), "STUB_") {
			continue
		}
		key := comparisonKey(label)
		seen[key]++
		if seen[key] > 1 {
			key = fmt.Sprintf("%s_%d", key, seen[key])
		}
		if v, ok := parseNumber(row[i]); ok {
			out[key] = v
			continue
		}
		if s := strings.TrimSpace(row[i]); s != "" && s != "-" {
			out[key] = s
		}
	}
	return out
}

func comparisonKey(label string) string {
	if t, ok := parseWPSRDate(label); ok {
		return "value_" + t.Format("2006_01_02")
	}
	label = strings.ToLower(strings.TrimSpace(label))
	label = strings.NewReplacer("%", "percent", "/", "_", "-", "_").Replace(label)
	label = nonKeyChars.ReplaceAllString(label, "_")
	label = strings.Trim(label, "_")
	if label == "" {
		return "column"
	}
	return label
}

var nonKeyChars = regexp.MustCompile(`[^a-z0-9]+`)

func parseNumber(s string) (float64, bool) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" || s == "-" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

func parseWPSRDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	parts := strings.Fields(s)
	if len(parts) > 1 {
		s = parts[len(parts)-1]
	}
	for _, layout := range []string{"1/2/06", "1/2/2006"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func inferUnit(spec tableSpec, descriptors []string) string {
	text := strings.ToLower(strings.Join(descriptors, " "))
	switch {
	case strings.Contains(text, "percent utilization"):
		return "percent"
	case spec.ID == "table1" && len(descriptors) == 1:
		return "million_barrels"
	case spec.ID == "table9":
		return "wpsr_table_units"
	default:
		return "wpsr_table_units"
	}
}

func validity(reportWeek time.Time) map[string]any {
	return map[string]any{
		"valid_start":    reportWeek.Format(time.RFC3339),
		"valid_end":      reportWeek.Add(14 * 24 * time.Hour).Format(time.RFC3339),
		"validity_basis": "eia_wpsr_week_ending",
	}
}

func stableID(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])[:20]
}
