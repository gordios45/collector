// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA SPC preliminary local storm reports.
package spc_storm_reports

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	todayURL     = "https://www.spc.noaa.gov/climo/reports/today_filtered.csv"
	yesterdayURL = "https://www.spc.noaa.gov/climo/reports/yesterday_filtered.csv"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "spc_storm_reports" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	out := []events.Event{}
	for _, src := range []struct {
		url string
		day time.Time
		tag string
	}{
		{todayURL, utcDay(now), "today"},
		{yesterdayURL, utcDay(now).AddDate(0, 0, -1), "yesterday"},
	} {
		buf, err := httpx.GetBytes(ctx, src.url, nil)
		if err != nil {
			return nil, err
		}
		rows, err := parseCSV(buf, src.day, src.tag, now)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", src.tag, err)
		}
		out = append(out, rows...)
	}
	return out, nil
}

func parseCSV(buf []byte, day time.Time, dayTag string, now time.Time) ([]events.Event, error) {
	r := csv.NewReader(bytes.NewReader(buf))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	var typ string
	out := []events.Event{}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 8 {
			continue
		}
		if strings.EqualFold(rec[0], "Time") {
			typ = typeFromHeader(rec)
			continue
		}
		if typ == "" {
			continue
		}
		ev, ok := eventFromRecord(rec, typ, day, dayTag, now)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func typeFromHeader(rec []string) string {
	if len(rec) < 2 {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(rec[1])) {
	case "f_scale":
		return "tornado"
	case "speed":
		return "wind"
	case "size":
		return "hail"
	}
	return ""
}

func eventFromRecord(rec []string, typ string, day time.Time, dayTag string, now time.Time) (events.Event, bool) {
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(rec[5]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(rec[6]), 64)
	if err1 != nil || err2 != nil || (lat == 0 && lon == 0) {
		return events.Event{}, false
	}
	ts, ok := parseReportTime(day, rec[0])
	if !ok {
		return events.Event{}, false
	}
	if dayTag == "today" && ts.After(now.Add(2*time.Hour)) {
		ts = ts.AddDate(0, 0, -1)
	}
	mag := strings.TrimSpace(rec[1])
	location := strings.TrimSpace(rec[2])
	county := strings.TrimSpace(rec[3])
	state := strings.TrimSpace(rec[4])
	comments := strings.TrimSpace(rec[7])
	props := map[string]any{
		"type":                typ,
		"time":                strings.TrimSpace(rec[0]),
		"magnitude":           mag,
		"location":            location,
		"county":              county,
		"state":               state,
		"lat":                 lat,
		"lon":                 lon,
		"comments":            comments,
		"day":                 dayTag,
		"source_api_endpoint": endpointForDay(dayTag),
	}
	collectorutil.AddSPCStormReportScores(props)
	return events.Event{
		Ts:     ts,
		Source: "spc_storm_reports",
		ExtID:  fmt.Sprintf("%s:%s:%s:%s:%s", day.Format("20060102"), typ, strings.TrimSpace(rec[0]), state, shortHash(strings.Join(rec, "|"))),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func parseReportTime(day time.Time, raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, false
	}
	for len(s) < 4 {
		s = "0" + s
	}
	h, err1 := strconv.Atoi(s[:2])
	m, err2 := strconv.Atoi(s[2:4])
	if err1 != nil || err2 != nil || h > 23 || m > 59 {
		return time.Time{}, false
	}
	return time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, time.UTC), true
}

func utcDay(t time.Time) time.Time {
	return time.Date(t.UTC().Year(), t.UTC().Month(), t.UTC().Day(), 0, 0, 0, 0, time.UTC)
}

func endpointForDay(tag string) string {
	if tag == "yesterday" {
		return yesterdayURL
	}
	return todayURL
}

func shortHash(s string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return strconv.FormatUint(uint64(h.Sum32()), 36)
}
