// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Event is the unified shape collectors produce and the ingester writes
// into `events`. Props is the raw record for the intel panel so we don't
// hard-code every field.
package events

import "time"

type Event struct {
	Ts     time.Time      // event occurrence time (source-reported if available)
	Source string         // 'martrack', 'flights', …
	ExtID  string         // feed-native id (vesselId, icao24, alert uuid…)
	Lat    float64        // WGS84; for polygons, centroid or zero
	Lon    float64
	Geom   string         // optional WKT (e.g. "POLYGON(...)"); when empty, Lat/Lon → POINT
	Props  map[string]any // raw record — rendered by intel panel directly
}

// HasPoint reports whether Lat/Lon are meaningful. Collectors that have
// only polygon geometry set Geom and leave Lat/Lon zero.
func (e Event) HasPoint() bool { return e.Lat != 0 || e.Lon != 0 }
