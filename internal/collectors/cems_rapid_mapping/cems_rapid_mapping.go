// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Copernicus EMS Rapid Mapping public activations.
package cems_rapid_mapping

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://rapidmapping.emergency.copernicus.eu/backend/dashboard-api/public-activations-info/?limit=200&ordering=-lastUpdate"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "cems_rapid_mapping" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type response struct {
	Results []activation `json:"results"`
}

type activation struct {
	Code           string   `json:"code"`
	Countries      []string `json:"countries"`
	EventTime      string   `json:"eventTime"`
	Name           string   `json:"name"`
	Centroid       string   `json:"centroid"`
	ActivationTime string   `json:"activationTime"`
	Category       string   `json:"category"`
	LastUpdate     string   `json:"lastUpdate"`
	Closed         bool     `json:"closed"`
	GDACSID        string   `json:"gdacsId"`
	NAOIs          int      `json:"n_aois"`
	NProducts      int      `json:"n_products"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw response
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Results))
	for _, a := range raw.Results {
		ev, ok := eventFromActivation(a)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventFromActivation(a activation) (events.Event, bool) {
	if a.Code == "" {
		return events.Event{}, false
	}
	lon, lat, ok := pointWKT(a.Centroid)
	if !ok {
		return events.Event{}, false
	}
	ts := parseTime(a.LastUpdate)
	if ts.IsZero() {
		ts = parseTime(a.ActivationTime)
	}
	if ts.IsZero() {
		ts = parseTime(a.EventTime)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	props := map[string]any{
		"code":                a.Code,
		"name":                a.Name,
		"category":            a.Category,
		"countries":           a.Countries,
		"eventTime":           a.EventTime,
		"activationTime":      a.ActivationTime,
		"lastUpdate":          a.LastUpdate,
		"closed":              a.Closed,
		"gdacsId":             a.GDACSID,
		"n_aois":              a.NAOIs,
		"n_products":          a.NProducts,
		"centroid":            a.Centroid,
		"activation_url":      fmt.Sprintf("https://rapidmapping.emergency.copernicus.eu/EMSR%s", trimEMSR(a.Code)),
		"source_api_endpoint": url,
	}
	return events.Event{
		Ts:     ts,
		Source: "cems_rapid_mapping",
		ExtID:  a.Code,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

var pointRe = regexp.MustCompile(`(?i)^POINT\s*\(\s*([-0-9.]+)\s+([-0-9.]+)\s*\)$`)

func pointWKT(s string) (lon, lat float64, ok bool) {
	m := pointRe.FindStringSubmatch(s)
	if len(m) != 3 {
		return 0, 0, false
	}
	lon, err1 := strconv.ParseFloat(m[1], 64)
	lat, err2 := strconv.ParseFloat(m[2], 64)
	return lon, lat, err1 == nil && err2 == nil
}

func parseTime(s string) time.Time {
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func trimEMSR(code string) string {
	if len(code) > 4 && code[:4] == "EMSR" {
		return code[4:]
	}
	return code
}
