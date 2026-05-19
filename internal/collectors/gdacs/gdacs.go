// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// GDACS — Global Disaster Alert and Coordination System.
// https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH?...
//
// GDACS returns a FeatureCollection whose properties contain the full
// event metadata. We preserve everything in props for the intel panel.
package gdacs

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const urlTmpl = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH?alertlevel=Green;Orange;Red&eventlist=&fromDate=%s&toDate=%s&limit=500"

func buildURL() string {
	now := time.Now().UTC()
	from := now.Add(-30 * 24 * time.Hour).Format("2006-01-02")
	to := now.Add(24 * time.Hour).Format("2006-01-02")
	return fmt.Sprintf(urlTmpl, from, to)
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "gdacs" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		Features []struct {
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string `json:"type"`
				Coordinates any    `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, buildURL(), nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, ok := pointFromGeo(f.Geometry.Type, f.Geometry.Coordinates)
		if !ok {
			continue
		}
		id, _ := f.Properties["eventid"].(float64)
		ext := ""
		if id != 0 {
			ext = stringFromFloat(id)
		}
		if ext == "" {
			if s, ok := f.Properties["eventid"].(string); ok {
				ext = s
			}
		}

		ts := time.Now().UTC()
		if s, ok := f.Properties["fromdate"].(string); ok {
			if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "gdacs", ExtID: ext,
			Lat: lat, Lon: lon, Props: f.Properties,
		})
	}
	return out, nil
}

func pointFromGeo(typ string, coords any) (lat, lon float64, ok bool) {
	arr, _ := coords.([]any)
	if len(arr) < 2 {
		return 0, 0, false
	}
	x, ok1 := arr[0].(float64)
	y, ok2 := arr[1].(float64)
	if !ok1 || !ok2 {
		return 0, 0, false
	}
	_ = typ
	return y, x, true
}

func stringFromFloat(f float64) string {
	n := int64(f)
	if float64(n) != f {
		return ""
	}
	// avoid "fmt" import; simple itoa
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
