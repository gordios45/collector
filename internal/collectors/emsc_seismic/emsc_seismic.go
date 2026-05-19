// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// EMSC SeismicPortal real-time earthquake feed.
package emsc_seismic

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://www.seismicportal.eu/fdsnws/event/1/query?format=json&limit=500&orderby=time"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "emsc_seismic" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

type featureCollection struct {
	Features []feature `json:"features"`
}

type feature struct {
	ID         string     `json:"id"`
	Geometry   geometry   `json:"geometry"`
	Properties properties `json:"properties"`
}

type geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type properties struct {
	SourceID      string  `json:"source_id"`
	SourceCatalog string  `json:"source_catalog"`
	LastUpdate    string  `json:"lastupdate"`
	Time          string  `json:"time"`
	FlynnRegion   string  `json:"flynn_region"`
	Lat           float64 `json:"lat"`
	Lon           float64 `json:"lon"`
	Depth         float64 `json:"depth"`
	EvType        string  `json:"evtype"`
	Auth          string  `json:"auth"`
	Mag           float64 `json:"mag"`
	MagType       string  `json:"magtype"`
	Unid          string  `json:"unid"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var fc featureCollection
	if err := httpx.GetJSON(ctx, endpoint, nil, &fc); err != nil {
		return nil, err
	}
	return eventsFromFeatures(fc.Features, time.Now().UTC()), nil
}

func eventsFromFeatures(features []feature, fallback time.Time) []events.Event {
	out := make([]events.Event, 0, len(features))
	for _, f := range features {
		ev, ok := eventFromFeature(f, fallback)
		if ok {
			out = append(out, ev)
		}
	}
	return out
}

func eventFromFeature(f feature, fallback time.Time) (events.Event, bool) {
	if f.Geometry.Type != "" && f.Geometry.Type != "Point" {
		return events.Event{}, false
	}
	lon, lat := f.Properties.Lon, f.Properties.Lat
	if len(f.Geometry.Coordinates) >= 2 {
		lon, lat = f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
	}
	if lat == 0 && lon == 0 {
		return events.Event{}, false
	}
	ext := firstNonEmpty(f.Properties.Unid, f.ID, f.Properties.SourceID)
	if ext == "" {
		return events.Event{}, false
	}
	ts := parseTime(f.Properties.Time)
	if ts.IsZero() {
		ts = parseTime(f.Properties.LastUpdate)
	}
	if ts.IsZero() {
		ts = fallback
	}
	props := map[string]any{
		"unid":                f.Properties.Unid,
		"id":                  f.ID,
		"source_id":           f.Properties.SourceID,
		"source_catalog":      f.Properties.SourceCatalog,
		"lastupdate":          f.Properties.LastUpdate,
		"time":                f.Properties.Time,
		"flynn_region":        f.Properties.FlynnRegion,
		"lat":                 lat,
		"lon":                 lon,
		"depth":               f.Properties.Depth,
		"evtype":              f.Properties.EvType,
		"auth":                f.Properties.Auth,
		"mag":                 f.Properties.Mag,
		"magtype":             f.Properties.MagType,
		"source_api_endpoint": endpoint,
	}
	collectorutil.AddEMSCSeismicScores(props)
	return events.Event{
		Ts:     ts,
		Source: "emsc_seismic",
		ExtID:  fmt.Sprintf("%s", ext),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func parseTime(s string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
