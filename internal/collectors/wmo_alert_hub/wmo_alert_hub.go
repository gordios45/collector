// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// WMO Alert Hub current CAP alert summary.
//
// The feed exposes alert summaries plus member metadata. We keep one point at
// the official member centroid and preserve the CAP XML URL for drill-down.
package wmo_alert_hub

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	alertsURL  = "https://severeweather.wmo.int/json/wmo_all.json"
	membersURL = "https://severeweather.wmo.int/json/wmo_member.json?20240904"
	capBaseURL = "https://severeweather.wmo.int/v2/cap-alerts/"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "wmo_alert_hub" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

type alertsResponse struct {
	ItemCount   int         `json:"itemCount"`
	LastUpdated string      `json:"lastUpdated"`
	Items       []alertItem `json:"items"`
}

type alertItem struct {
	ID        string `json:"id"`
	Event     string `json:"event"`
	Headline  string `json:"headline"`
	Sent      string `json:"sent"`
	Expires   string `json:"expires"`
	AreaDesc  string `json:"areaDesc"`
	MID       string `json:"mid"`
	Region    string `json:"ra"`
	Severity  int    `json:"s"`
	Urgency   int    `json:"u"`
	Certainty int    `json:"c"`
	URL       string `json:"url"`
	Effective string `json:"effective"`
}

type memberRegion struct {
	Region  int      `json:"ra"`
	Members []member `json:"members"`
}

type member struct {
	MID  string  `json:"mid"`
	Name string  `json:"name"`
	Dept string  `json:"dept"`
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
	URL  string  `json:"url"`
	Reg  int     `json:"reg"`
	Code string  `json:"code"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	members, err := fetchMembers(ctx)
	if err != nil {
		return nil, err
	}
	var raw alertsResponse
	if err := httpx.GetJSON(ctx, alertsURL, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Items))
	for _, item := range raw.Items {
		ev, ok := eventFromItem(item, members, raw.LastUpdated)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func fetchMembers(ctx context.Context) (map[string]member, error) {
	var raw []memberRegion
	if err := httpx.GetJSON(ctx, membersURL, nil, &raw); err != nil {
		return nil, err
	}
	out := map[string]member{}
	for _, region := range raw {
		for _, m := range region.Members {
			if m.MID != "" {
				out[m.MID] = m
			}
		}
	}
	return out, nil
}

func eventFromItem(item alertItem, members map[string]member, lastUpdated string) (events.Event, bool) {
	if item.ID == "" || item.URL == "" {
		return events.Event{}, false
	}
	m, ok := members[item.MID]
	if !ok || (m.Lat == 0 && m.Lng == 0) {
		return events.Event{}, false
	}
	ts := parseTime(item.Sent)
	if ts.IsZero() {
		ts = parseTime(item.Effective)
	}
	if ts.IsZero() {
		ts = parseTime(lastUpdated)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	props := map[string]any{
		"id":                  item.ID,
		"event":               item.Event,
		"headline":            item.Headline,
		"sent":                item.Sent,
		"effective":           item.Effective,
		"expires":             item.Expires,
		"areaDesc":            item.AreaDesc,
		"severity_code":       item.Severity,
		"urgency_code":        item.Urgency,
		"certainty_code":      item.Certainty,
		"severity":            severityLabel(item.Severity),
		"urgency":             urgencyLabel(item.Urgency),
		"certainty":           certaintyLabel(item.Certainty),
		"member_id":           item.MID,
		"member_country":      m.Name,
		"member_code":         m.Code,
		"member_department":   m.Dept,
		"wmo_region":          firstNonEmpty(item.Region, fmt.Sprintf("%d", m.Reg)),
		"cap_url":             capBaseURL + item.URL,
		"source_api_endpoint": alertsURL,
		"lastUpdated":         lastUpdated,
	}
	collectorutil.AddAlertScores(props)
	collectorutil.AddWMOHazardScores(props)
	return events.Event{
		Ts:     ts,
		Source: "wmo_alert_hub",
		ExtID:  item.ID,
		Lat:    m.Lat,
		Lon:    m.Lng,
		Props:  props,
	}, true
}

func parseTime(s string) time.Time {
	for _, layout := range []string{"2006-01-02 15:04:05", time.RFC3339Nano, time.RFC3339} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func severityLabel(v int) string {
	switch v {
	case 1:
		return "Extreme"
	case 2:
		return "Severe"
	case 3:
		return "Moderate"
	case 4:
		return "Minor"
	}
	return strconv.Itoa(v)
}

func urgencyLabel(v int) string {
	switch v {
	case 1:
		return "Immediate"
	case 2:
		return "Expected"
	case 3:
		return "Future"
	case 4:
		return "Past"
	}
	return strconv.Itoa(v)
}

func certaintyLabel(v int) string {
	switch v {
	case 1:
		return "Observed"
	case 2:
		return "Likely"
	case 3:
		return "Possible"
	case 4:
		return "Unlikely"
	}
	return strconv.Itoa(v)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
