// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package portwatch_disruptions ingests IMF PortWatch disruption events from
// the public ArcGIS FeatureServer.
package portwatch_disruptions

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/portwatch_disruptions_database/FeatureServer/0/query"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "portwatch_disruptions" }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	since := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02 15:04:05")
	q := url.Values{}
	q.Set("where", fmt.Sprintf("todate > timestamp '%s' OR todate IS NULL", since))
	q.Set("outFields", "eventid,eventtype,eventname,alertlevel,country,fromdate,todate,severitytext,lat,long,affectedports,n_affectedports")
	q.Set("orderByFields", "fromdate DESC")
	q.Set("resultRecordCount", "2000")
	q.Set("outSR", "4326")
	q.Set("f", "json")

	var body arcResponse
	if err := httpx.GetJSON(ctx, endpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &body); err != nil {
		return nil, err
	}
	if body.Error.Message != "" {
		return nil, fmt.Errorf("portwatch arcgis: %s", body.Error.Message)
	}
	return eventsFromArc(body, endpoint), nil
}

type arcResponse struct {
	Features []struct {
		Attributes map[string]any `json:"attributes"`
	} `json:"features"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func eventsFromArc(body arcResponse, sourceURL string) []events.Event {
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(body.Features))
	for _, f := range body.Features {
		a := f.Attributes
		id := text(a["eventid"])
		if id == "" {
			continue
		}
		lat, okLat := number(a["lat"])
		lon, okLon := number(a["long"])
		if !okLat || !okLon || math.Abs(lat) > 90 || math.Abs(lon) > 180 {
			continue
		}
		from := arcTime(a["fromdate"])
		to := arcTime(a["todate"])
		active := to.IsZero() || to.After(now)
		ts := from
		if active {
			ts = now
		}
		if ts.IsZero() {
			ts = now
		}
		alert := strings.ToUpper(text(a["alertlevel"]))
		score := disruptionScore(alert, intFrom(a["n_affectedports"]))
		props := map[string]any{
			"eventid":             id,
			"eventtype":           text(a["eventtype"]),
			"eventname":           text(a["eventname"]),
			"alertlevel":          alert,
			"country":             text(a["country"]),
			"fromdate":            timeString(from),
			"todate":              timeString(to),
			"active":              active,
			"severitytext":        text(a["severitytext"]),
			"affectedports":       splitPorts(text(a["affectedports"])),
			"n_affectedports":     intFrom(a["n_affectedports"]),
			"disruption_score":    score,
			"source_api_endpoint": sourceURL,
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "portwatch_disruptions",
			ExtID:  "portwatch:" + id,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out
}

func disruptionScore(alert string, ports int) float64 {
	score := 0.5
	switch strings.ToUpper(alert) {
	case "RED":
		score = 3.0
	case "ORANGE":
		score = 2.0
	case "YELLOW":
		score = 1.0
	}
	if ports >= 5 {
		score += 0.4
	}
	if ports >= 15 {
		score += 0.6
	}
	return math.Min(score, 4)
}

func splitPorts(s string) []string {
	parts := strings.Split(s, ",")
	out := []string{}
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func text(v any) string {
	return strings.TrimSpace(fmt.Sprint(v))
}

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func intFrom(v any) int {
	f, ok := number(v)
	if !ok {
		return 0
	}
	return int(f)
}

func arcTime(v any) time.Time {
	switch x := v.(type) {
	case float64:
		if x <= 0 {
			return time.Time{}
		}
		return time.UnixMilli(int64(x)).UTC()
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return time.Time{}
		}
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil && ms > 10000000000 {
			return time.UnixMilli(ms).UTC()
		}
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Time{}
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
