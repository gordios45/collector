// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package sources

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gordios45/collector/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uber/h3-go/v4"
)

const eventAggregateBinSize = 15 * time.Minute

type h3BinAgg struct {
	source   string
	h3Res    int
	h3Cell   string
	binStart time.Time
	count    int
	lat      float64
	lon      float64
}

// upsertEventH3Bins maintains the compact source/H3/bin event projection used
// by local signal baselines. It is called only for rows that were actually
// inserted into events, so retransmits handled by ON CONFLICT DO NOTHING do
// not inflate counts.
func upsertEventH3Bins(ctx context.Context, tx pgx.Tx, evs []events.Event, res int) error {
	if len(evs) == 0 {
		return nil
	}
	agg := make(map[string]h3BinAgg, len(evs))
	for _, e := range evs {
		if e.Source == "" || e.Ts.IsZero() || !e.HasPoint() {
			continue
		}
		cell, err := h3.LatLngToCell(h3.LatLng{Lat: e.Lat, Lng: e.Lon}, res)
		if err != nil {
			continue
		}
		binStart := e.Ts.UTC().Truncate(eventAggregateBinSize)
		cellID := cell.String()
		key := fmt.Sprintf("%s|%d|%s|%s", e.Source, res, cellID, binStart.Format(time.RFC3339))
		row := agg[key]
		if row.count == 0 {
			row = h3BinAgg{
				source:   e.Source,
				h3Res:    res,
				h3Cell:   cellID,
				binStart: binStart,
				lat:      e.Lat,
				lon:      e.Lon,
			}
		}
		row.count++
		agg[key] = row
	}
	if len(agg) == 0 {
		return nil
	}

	sources := make([]string, 0, len(agg))
	h3Res := make([]int32, 0, len(agg))
	h3Cells := make([]string, 0, len(agg))
	binStarts := make([]time.Time, 0, len(agg))
	counts := make([]int32, 0, len(agg))
	lats := make([]float64, 0, len(agg))
	lons := make([]float64, 0, len(agg))
	for _, row := range agg {
		sources = append(sources, row.source)
		h3Res = append(h3Res, int32(row.h3Res))
		h3Cells = append(h3Cells, row.h3Cell)
		binStarts = append(binStarts, row.binStart)
		counts = append(counts, int32(row.count))
		lats = append(lats, row.lat)
		lons = append(lons, row.lon)
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO event_h3_bins
		  (source, h3_res, h3_cell, bin_start, events_count, sample_lat, sample_lon)
		SELECT source, h3_res, h3_cell, bin_start, events_count, sample_lat, sample_lon
		  FROM unnest($1::text[], $2::int[], $3::text[], $4::timestamptz[],
		              $5::int[], $6::float8[], $7::float8[])
		       AS t(source, h3_res, h3_cell, bin_start, events_count, sample_lat, sample_lon)
		ON CONFLICT (source, h3_res, h3_cell, bin_start) DO UPDATE
		   SET events_count = event_h3_bins.events_count + EXCLUDED.events_count,
		       sample_lat   = COALESCE(event_h3_bins.sample_lat, EXCLUDED.sample_lat),
		       sample_lon   = COALESCE(event_h3_bins.sample_lon, EXCLUDED.sample_lon),
		       updated_at   = now()`,
		sources, h3Res, h3Cells, binStarts, counts, lats, lons)
	if err != nil {
		if sourcePgErrIsUndefinedTable(err) {
			return nil
		}
		return fmt.Errorf("upsert event h3 bins: %w", err)
	}
	return nil
}

func sourcePgErrIsUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func sourcePgErrIsUndefinedColumn(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42703"
}
