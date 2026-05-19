// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NGA broadcast warnings (maritime / hydrographic).
// https://msi.nga.mil/api/publications/broadcast-warn?output=json
package nga_warnings

import (
	"context"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://msi.nga.mil/api/publications/broadcast-warn?output=json"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nga_warnings" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw struct {
		BroadcastWarn []map[string]any `json:"broadcast-warn"`
	}
	if err := httpx.GetJSON(ctx, url, nil, &raw); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(raw.BroadcastWarn))
	for _, w := range raw.BroadcastWarn {
		msgNum := fmt.Sprintf("%v", w["msgNumber"])
		year := fmt.Sprintf("%v", w["msgYear"])
		broadcastID := fmt.Sprintf("%v_%s_%s", w["navArea"], year, msgNum)
		// NGA warnings have a "subregion" but many don't include explicit lat/lon.
		// When available they embed lat/lon in the "text" field; we stash those
		// for a later enrichment pass. For now, drop entries without a usable
		// point. (Phase 7 will add polygon/centroid enrichment.)
		lat, ok1 := w["lat"].(float64)
		lon, ok2 := w["lon"].(float64)
		if !ok1 || !ok2 {
			continue
		}
		out = append(out, events.Event{
			Ts: now, Source: "nga_warnings", ExtID: broadcastID,
			Lat: lat, Lon: lon, Props: w,
		})
	}
	return out, nil
}
