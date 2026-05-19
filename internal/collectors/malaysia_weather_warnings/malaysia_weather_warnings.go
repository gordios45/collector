// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package malaysia_weather_warnings ingests official METMalaysia warnings from
// Malaysia's public open-data API.
package malaysia_weather_warnings

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	sourceID   = "malaysia_weather_warnings"
	defaultURL = "https://api.data.gov.my/weather/warning/"
)

type Collector struct {
	endpoint string
}

func New() (*Collector, error) {
	endpoint := strings.TrimSpace(os.Getenv("MALAYSIA_WEATHER_WARNINGS_URL"))
	if endpoint == "" {
		endpoint = defaultURL
	}
	return &Collector{endpoint: endpoint}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("MALAYSIA_WEATHER_WARNINGS_POLL_EVERY_S", 600, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var rows []warningRow
	if err := httpx.GetJSON(ctx, c.endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil, err
	}
	return eventsFromRows(c.endpoint, rows), nil
}

type warningRow struct {
	WarningIssue struct {
		Issued  string `json:"issued"`
		TitleBM string `json:"title_bm"`
		TitleEN string `json:"title_en"`
	} `json:"warning_issue"`
	ValidFrom     *string `json:"valid_from"`
	ValidTo       *string `json:"valid_to"`
	HeadingEN     string  `json:"heading_en"`
	TextEN        string  `json:"text_en"`
	InstructionEN *string `json:"instruction_en"`
	HeadingBM     string  `json:"heading_bm"`
	TextBM        string  `json:"text_bm"`
	InstructionBM *string `json:"instruction_bm"`
}

func eventsFromRows(endpoint string, rows []warningRow) []events.Event {
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		ts := parseMalaysiaTime(row.WarningIssue.Issued)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		heading := firstNonEmpty(row.HeadingEN, row.WarningIssue.TitleEN, row.HeadingBM, row.WarningIssue.TitleBM)
		text := firstNonEmpty(row.TextEN, row.TextBM)
		props := map[string]any{
			"source_provider":     "METMalaysia / data.gov.my",
			"source_api_endpoint": endpoint,
			"country":             "Malaysia",
			"country_code":        "MY",
			"issued":              row.WarningIssue.Issued,
			"title_en":            row.WarningIssue.TitleEN,
			"title_bm":            row.WarningIssue.TitleBM,
			"heading_en":          row.HeadingEN,
			"text_en":             row.TextEN,
			"heading_bm":          row.HeadingBM,
			"text_bm":             row.TextBM,
			"severity":            severity(heading),
			"hazard_type":         hazardType(heading + " " + text),
			"active_advisory":     !strings.EqualFold(strings.TrimSpace(heading), "No Advisory"),
		}
		if row.ValidFrom != nil {
			props["valid_from"] = *row.ValidFrom
		}
		if row.ValidTo != nil {
			props["valid_to"] = *row.ValidTo
		}
		if row.InstructionEN != nil {
			props["instruction_en"] = *row.InstructionEN
		}
		if row.InstructionBM != nil {
			props["instruction_bm"] = *row.InstructionBM
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s", row.WarningIssue.Issued, heading, text)),
			Lat:    4.2105,
			Lon:    101.9758,
			Props:  props,
		})
	}
	return out
}

func parseMalaysiaTime(raw string) time.Time {
	loc := time.FixedZone("MYT", 8*3600)
	for _, layout := range []string{"2006-01-02T15:04:05", time.RFC3339} {
		if t, err := time.ParseInLocation(layout, strings.TrimSpace(raw), loc); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func severity(text string) string {
	l := strings.ToLower(text)
	switch {
	case strings.Contains(l, "third category"):
		return "high"
	case strings.Contains(l, "second category"):
		return "moderate"
	case strings.Contains(l, "first category"):
		return "low"
	case strings.Contains(l, "no advisory"):
		return "none"
	default:
		return "unknown"
	}
}

func hazardType(text string) string {
	l := strings.ToLower(text)
	switch {
	case strings.Contains(l, "cyclone"):
		return "tropical_cyclone"
	case strings.Contains(l, "rain"):
		return "heavy_rain"
	case strings.Contains(l, "wind") || strings.Contains(l, "wave") || strings.Contains(l, "rough sea"):
		return "marine_weather"
	default:
		return "weather"
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}
