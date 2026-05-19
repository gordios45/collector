// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package sources

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"

	"github.com/jackc/pgx/v5/pgxpool"
)

func recordSourceRun(ctx context.Context, pool *pgxpool.Pool, src string, started, finished time.Time, ok bool, rowsFetched, rowsInserted int, payloadBytes int64, errIn error) {
	if pool == nil || src == "" {
		return
	}
	durationMS := int(finished.Sub(started).Milliseconds())
	if durationMS < 0 {
		durationMS = 0
	}
	var errText any
	if errIn != nil {
		errText = errIn.Error()
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO source_ingest_runs
		  (source_id, started_at, finished_at, ok, rows_fetched, rows_inserted, payload_bytes, duration_ms, error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		src, started, finished, ok, rowsFetched, rowsInserted, payloadBytes, durationMS, errText)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !sourcePgErrIsUndefinedTable(err) {
			log.Printf("[%s] record source run: %v", src, err)
		}
		return
	}
	_, err = pool.Exec(ctx,
		`DELETE FROM source_ingest_runs WHERE source_id=$1 AND started_at < now() - interval '30 days'`, src)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[%s] prune source runs: %v", src, err)
	}
}

func estimatedEventPayloadBytes(evs []events.Event) int64 {
	var total int64
	for _, e := range evs {
		raw, _ := json.Marshal(e.Props)
		total += int64(len(raw) + len(e.Source) + len(e.ExtID) + len(e.Geom) + 32)
	}
	return total
}

func estimatedFeaturePayloadBytes(feats []features.Feature) int64 {
	var total int64
	for _, f := range feats {
		raw, _ := json.Marshal(f.Props)
		total += int64(len(raw) + len(f.ExtID) + len(f.GeomWKT) + 32)
	}
	return total
}
