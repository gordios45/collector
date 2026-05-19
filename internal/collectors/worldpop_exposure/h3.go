// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package worldpop_exposure

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uber/h3-go/v4"
)

type H3Collector struct {
	pool   *pgxpool.Pool
	maxRow int
}

func NewPopulationH3Exposure(pool *pgxpool.Pool) (*H3Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_POPULATION_H3_EXPOSURE") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_POPULATION_H3_EXPOSURE=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	return &H3Collector{
		pool:   pool,
		maxRow: envInt("POPULATION_H3_MAX_ROWS", 200, 1, 2000),
	}, nil
}

func (c *H3Collector) ID() string               { return "population_h3_exposure" }
func (c *H3Collector) PollEvery() time.Duration { return 12 * time.Hour }

func (c *H3Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT DISTINCT ON (props->>'watch_aoi_id')
		       ts,
		       ST_Y(ST_Centroid(geom::geometry)) AS lat,
		       ST_X(ST_Centroid(geom::geometry)) AS lon,
		       props
		  FROM events
		 WHERE source = 'worldpop_exposure'
		   AND ts > now() - interval '30 days'
		   AND geom IS NOT NULL
		   AND props ? 'population_1km'
		   AND props ? 'population_5km'
		   AND props ? 'population_25km'
		 ORDER BY props->>'watch_aoi_id', ts DESC
		 LIMIT $1`, c.maxRow)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	out := []events.Event{}
	for rows.Next() {
		var ts time.Time
		var lat, lon float64
		var props map[string]any
		if err := rows.Scan(&ts, &lat, &lon, &props); err != nil {
			return nil, err
		}
		if !validLatLon(lat, lon) {
			continue
		}
		cell, err := h3.LatLngToCell(h3.LatLng{Lat: lat, Lng: lon}, 4)
		if err != nil {
			continue
		}
		aoiID := stringFromAny(props["watch_aoi_id"])
		year := stringFromAny(props["population_year"])
		if aoiID == "" {
			continue
		}
		h3Props := copyProps(props)
		h3Props["h3_cell"] = cell.String()
		h3Props["h3_res"] = 4
		h3Props["derived_from_source"] = "worldpop_exposure"
		h3Props["derived_from_ts"] = ts.Format(time.RFC3339)
		h3Props["integration_state"] = "h3_preaggregated"
		h3Props["source_priority"] = "h3_exposure_context"

		out = append(out, events.Event{
			Ts:     now,
			Source: "population_h3_exposure",
			ExtID:  fmt.Sprintf("%s:%s:%s", aoiID, year, cell.String()),
			Lat:    lat,
			Lon:    lon,
			Props:  h3Props,
		})
	}
	return out, rows.Err()
}

func copyProps(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+6)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func stringFromAny(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		if math.Trunc(x) == x {
			return fmt.Sprintf("%.0f", x)
		}
		return fmt.Sprintf("%f", x)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}
