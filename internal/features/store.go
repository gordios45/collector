// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Upsert helper for the `features` table. Feature is source-agnostic:
// { ExtID, Props, GeomWKT }.
package features

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Feature struct {
	ExtID   string
	GeomWKT string         // WKT: "POINT(lon lat)", "POLYGON((...))", "LINESTRING(...)"
	Props   map[string]any // rendered by the intel panel directly
}

// UpsertBatch does a per-row upsert on (source, ext_id) — for streaming
// sources like maritime AIS where each tick updates a subset of rows in
// place. Unlike Upsert (which replaces the whole source), rows omitted
// from `feats` are left untouched.
func UpsertBatch(ctx context.Context, pool *pgxpool.Pool, source string, feats []Feature) (int, error) {
	if len(feats) == 0 {
		return 0, nil
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const stmt = `INSERT INTO features (source, ext_id, geom, props, updated_at)
	              VALUES ($1, $2, ST_GeogFromText($3), $4::jsonb, now())
	              ON CONFLICT (source, ext_id) DO UPDATE
	                SET geom = EXCLUDED.geom,
	                    props = EXCLUDED.props,
	                    updated_at = now()`

	for _, f := range feats {
		raw, err := json.Marshal(f.Props)
		if err != nil {
			return 0, fmt.Errorf("marshal props: %w", err)
		}
		if _, err := tx.Exec(ctx, stmt, source, f.ExtID, f.GeomWKT, string(raw)); err != nil {
			return 0, fmt.Errorf("upsert %s/%s: %w", source, f.ExtID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(feats), nil
}

// Upsert replaces all features for the given source in a single transaction.
// That semantic fits "re-seed everything" — the caller pulls a fresh copy
// from upstream, then we atomically swap the set. (For the tiny sizes we're
// dealing with — thousands of rows — this is cheaper than diffing.)
func Upsert(ctx context.Context, pool *pgxpool.Pool, source string, feats []Feature) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `DELETE FROM features WHERE source = $1`, source); err != nil {
		return 0, fmt.Errorf("delete old: %w", err)
	}

	const stmt = `INSERT INTO features (source, ext_id, geom, props, updated_at)
	              VALUES ($1, $2, ST_GeogFromText($3), $4::jsonb, now())`
	for _, f := range feats {
		raw, err := json.Marshal(f.Props)
		if err != nil {
			return 0, fmt.Errorf("marshal props: %w", err)
		}
		if _, err := tx.Exec(ctx, stmt, source, f.ExtID, f.GeomWKT, string(raw)); err != nil {
			return 0, fmt.Errorf("insert %s/%s: %w", source, f.ExtID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(feats), nil
}
