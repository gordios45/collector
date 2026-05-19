// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package bgpstream_broker tracks CAIDA BGPStream broker metadata so Gordios
// can see whether public RouteViews/RIPE RIS update archives are fresh.
package bgpstream_broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	sourceName = "bgpstream_broker"
	baseURL    = "https://broker.bgpstream.caida.org/v2/meta/projects/"
	docsURL    = "https://bgpstream.caida.org/docs/api/broker"
)

type Collector struct {
	maxCollectors int
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_BGPSTREAM_BROKER") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_BGPSTREAM_BROKER=1")
	}
	return &Collector{maxCollectors: collectorutil.EnvInt("BGPSTREAM_MAX_COLLECTORS", 30, 2, 100)}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, project := range []string{"ris", "routeviews"} {
		rows, err := fetchProject(ctx, project)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, rows...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Props["update_lag_seconds"].(int64) > out[j].Props["update_lag_seconds"].(int64)
	})
	if len(out) > c.maxCollectors {
		out = out[:c.maxCollectors]
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type metaResp struct {
	Time int64 `json:"time"`
	Data struct {
		Projects map[string]project `json:"projects"`
	} `json:"data"`
}

type project struct {
	Collectors map[string]collectorMeta `json:"collectors"`
}

type collectorMeta struct {
	Project   string `json:"project"`
	DataTypes struct {
		Updates dumpMeta `json:"updates"`
		RIBs    dumpMeta `json:"ribs"`
	} `json:"dataTypes"`
}

type dumpMeta struct {
	DumpPeriod     int    `json:"dumpPeriod"`
	DumpDuration   int    `json:"dumpDuration"`
	LatestDumpTime string `json:"latestDumpTime"`
	OldestDumpTime string `json:"oldestDumpTime"`
}

func fetchProject(ctx context.Context, projectName string) ([]events.Event, error) {
	var r metaResp
	if err := httpx.GetJSON(ctx, baseURL+projectName, map[string]string{"Accept": "application/json"}, &r); err != nil {
		return nil, err
	}
	p, ok := r.Data.Projects[projectName]
	if !ok {
		return nil, fmt.Errorf("BGPStream broker returned no %s project", projectName)
	}
	now := time.Now().UTC()
	out := []events.Event{}
	for name, meta := range p.Collectors {
		ev, ok := eventForCollector(projectName, name, meta, now)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func eventForCollector(projectName, name string, meta collectorMeta, now time.Time) (events.Event, bool) {
	latest, err := strconv.ParseInt(meta.DataTypes.Updates.LatestDumpTime, 10, 64)
	if err != nil || latest <= 0 {
		return events.Event{}, false
	}
	ts := time.Unix(latest, 0).UTC()
	lag := now.Sub(ts)
	if lag < 0 {
		lag = 0
	}
	period := meta.DataTypes.Updates.DumpPeriod
	if period <= 0 {
		period = 900
	}
	lagSeconds := int64(lag.Seconds())
	lagScore := float64(lagSeconds-int64(period)) / float64(period)
	if lagScore < 0 {
		lagScore = 0
	}
	if lagScore > 3 {
		lagScore = 3
	}
	lat, lon := projectCentroid(projectName)
	bucket := now.Truncate(30 * time.Minute)
	props := map[string]any{
		"project":                    projectName,
		"collector":                  name,
		"latest_update_time":         ts.Format(time.RFC3339),
		"update_dump_period_seconds": period,
		"update_lag_seconds":         lagSeconds,
		"update_stream_lag_score":    collectorutil.Round(lagScore, 2),
		"source_api_endpoint":        baseURL + projectName,
		"docs_url":                   docsURL,
		"integration_state":          "broker_metadata_freshness_only",
	}
	return events.Event{
		Ts:     bucket,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s:%s", projectName, name, bucket.Format("20060102T1504")),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func projectCentroid(project string) (float64, float64) {
	switch project {
	case "ris":
		return 52.37, 4.90
	default:
		return 44.05, -123.09
	}
}
