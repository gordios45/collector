// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open reads DATABASE_URL, opens a pool, and pings. Caller must Close.
func Open(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxPoolConns()
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// The local Timescale/PostGIS workload can otherwise choose parallel
		// plans that resize dynamic shared memory beyond the small dev shm
		// allocation. Serial plans are more predictable for the ingester and
		// gateway's bounded API queries.
		_, err := conn.Exec(ctx, `
			SET max_parallel_workers_per_gather = 0;
			SET work_mem = '4MB';
		`)
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func maxPoolConns() int32 {
	const def = int32(2)
	raw := firstEnv("GORDIOS_DB_MAX_CONNS", "WV_DB_MAX_CONNS")
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 20 {
		return def
	}
	return int32(n)
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return ""
}
