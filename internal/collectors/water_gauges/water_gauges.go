// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// USGS Water Services — US stream/river gauges, stage (gage-height, 00065).
// Backend polls CONUS in coarse tiles (bbox ≤ 10° × 10°, USGS 400 limit) and
// stores the full per-site measurement. Frontend can pull a global snapshot
// without having to know the viewport.
package water_gauges

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

// USGS rejects bbox wider than ~5° in either dimension. We sweep CONUS
// in a coarse 5° grid. Format: {w, s, e, n}.
var tiles = func() [][4]float64 {
	var out [][4]float64
	for w := -125.0; w < -65.0; w += 5.0 {
		for s := 25.0; s < 50.0; s += 5.0 {
			out = append(out, [4]float64{w, s, w + 5, s + 5})
		}
	}
	return out
}()

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "water_gauges" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	seen := map[string]bool{}
	out := []events.Event{}

	for _, t := range tiles {
		url := fmt.Sprintf(
			"https://waterservices.usgs.gov/nwis/iv/?format=json&parameterCd=00065&siteStatus=active&period=PT2H&siteType=ST&bBox=%.2f,%.2f,%.2f,%.2f",
			t[0], t[1], t[2], t[3])
		var raw struct {
			Value struct {
				TimeSeries []map[string]any `json:"timeSeries"`
			} `json:"value"`
		}
		if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
			continue // a single tile failing shouldn't kill the batch
		}
		for _, ts := range raw.Value.TimeSeries {
			info, _ := ts["sourceInfo"].(map[string]any)
			geo, _ := info["geoLocation"].(map[string]any)
			pt, _ := geo["geogLocation"].(map[string]any)
			lat, _ := pt["latitude"].(float64)
			lon, _ := pt["longitude"].(float64)
			if lat == 0 && lon == 0 {
				continue
			}
			siteCode := ""
			if arr, ok := info["siteCode"].([]any); ok && len(arr) > 0 {
				if m, ok := arr[0].(map[string]any); ok {
					siteCode, _ = m["value"].(string)
				}
			}
			if siteCode == "" || seen[siteCode] {
				continue
			}
			seen[siteCode] = true

			// Latest value from timeSeries[0].values[0].value[last].
			var lastVal float64
			if vals, ok := ts["values"].([]any); ok && len(vals) > 0 {
				if m, ok := vals[0].(map[string]any); ok {
					if arr, ok := m["value"].([]any); ok && len(arr) > 0 {
						if rec, ok := arr[len(arr)-1].(map[string]any); ok {
							if s, _ := rec["value"].(string); s != "" {
								if f, err := strconv.ParseFloat(s, 64); err == nil {
									lastVal = f
								}
							}
						}
					}
				}
			}
			props := map[string]any{
				"site_code":         siteCode,
				"site_name":         info["siteName"],
				"gage_height_ft":    lastVal,
				"timeseries_source": ts,
			}
			out = append(out, events.Event{
				Ts: now, Source: "water_gauges", ExtID: siteCode,
				Lat: lat, Lon: lon, Props: props,
			})
		}
	}
	return out, nil
}
