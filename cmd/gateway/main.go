// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Gateway: raw ingestion HTTP + WebSocket server.
// Endpoints:
//
//	GET  /healthz               — db + timescale liveness
//	GET  /readyz                — process liveness only
//	GET  /api/sources           — list feeds + health
//	GET  /api/latest?source=X   — latest row per ext_id for a source
//	GET  /stream?source=X,Y     — WebSocket refresh-signal stream
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/gateway"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	db.LoadDotEnv("../.env", ".env")
	defaultDatabaseURL("RAW_GATEWAY_DB_URL")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()
	log.Println("[gateway] db pool ready")

	addr := os.Getenv("GATEWAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/healthz", healthz(pool))

	rest := &gateway.RestHandler{Pool: pool}
	rest.RegisterRaw(mux)

	hub := gateway.NewHub(pool)
	go hub.Run(ctx)

	ws := &gateway.WSHandler{Hub: hub}
	ws.Register(mux)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[gateway] listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[gateway] shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func defaultDatabaseURL(key string) {
	if os.Getenv("DATABASE_URL") != "" {
		return
	}
	if v := os.Getenv(key); v != "" {
		_ = os.Setenv("DATABASE_URL", v)
	}
}

func healthz(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()

		w.Header().Set("content-type", "application/json")
		statusCode := http.StatusOK
		resp := map[string]any{"ok": true}
		var version string
		if err := pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname='timescaledb'").Scan(&version); err != nil {
			resp["ok"] = false
			resp["db_err"] = err.Error()
			statusCode = http.StatusServiceUnavailable
		} else {
			resp["timescale"] = version
		}
		sourceFreshness, requiredViolations, err := sourceFreshnessHealth(ctx, pool)
		if err != nil {
			resp["ok"] = false
			resp["source_freshness_err"] = err.Error()
			statusCode = http.StatusServiceUnavailable
		} else {
			resp["source_freshness"] = sourceFreshness
			if intValue(sourceFreshness["violation_count"]) > 0 {
				if requiredViolations > 0 {
					resp["ok"] = false
					resp["source_freshness_required_violations"] = requiredViolations
					statusCode = http.StatusServiceUnavailable
				} else {
					resp["source_freshness_degraded_violations"] = intValue(sourceFreshness["violation_count"])
				}
			}
		}
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func sourceFreshnessHealth(ctx context.Context, pool *pgxpool.Pool) (map[string]any, int, error) {
	rows, err := pool.Query(ctx, `
		WITH contract AS (
			SELECT s.id,
			       COALESCE(s.freshness_contract_severity, 'degraded') AS severity,
			       COALESCE(s.expected_min_rows_per_window, 0)::int AS min_rows,
			       COALESCE(s.expected_window, interval '24 hours') AS effective_window,
			       s.expected_window,
			       s.expected_max_lag,
			       s.last_ok_at,
			       GREATEST(COALESCE(s.poll_every_s, 0) * 4, 3600)::int AS collector_liveness_s
			  FROM sources s
			 WHERE s.enabled
			   AND COALESCE(s.freshness_contract_enabled, false)
		),
		lookback AS (
			SELECT COALESCE(
			         MAX(GREATEST(effective_window, COALESCE(expected_max_lag, effective_window))),
			         interval '24 hours'
			       ) AS max_window
			  FROM contract
		),
		run_rollup AS (
			SELECT r.source_id AS id,
			       COALESCE(SUM(GREATEST(r.rows_inserted, 0)) FILTER (
			         WHERE r.started_at >= now() - c.effective_window
			       ), 0)::int AS rows_in_window,
			       MAX(r.finished_at) FILTER (WHERE r.rows_inserted > 0) AS last_inserted_at
			  FROM source_ingest_runs r
			  JOIN contract c ON c.id = r.source_id
			 WHERE r.started_at >= now() - (SELECT max_window FROM lookback)
			 GROUP BY r.source_id
		),
		contract_status AS (
			SELECT c.id,
			       c.severity,
			       c.min_rows,
			       c.expected_window,
			       c.expected_max_lag,
			       c.last_ok_at,
			       c.collector_liveness_s,
			       COALESCE(r.rows_in_window, 0)::int AS rows_in_window,
			       r.last_inserted_at,
			       CASE
			         WHEN c.collector_liveness_s > 0
			          AND (
			            c.last_ok_at IS NULL
			            OR now() - c.last_ok_at > (c.collector_liveness_s * interval '1 second')
			          )
			           THEN 'collector_liveness_stale'
			         WHEN c.min_rows > 0
			          AND COALESCE(r.rows_in_window, 0) < c.min_rows
			           THEN 'row_count_below_contract'
			         WHEN c.expected_max_lag IS NOT NULL
			          AND (c.min_rows > 0 OR r.last_inserted_at IS NOT NULL)
			          AND (r.last_inserted_at IS NULL OR now() - r.last_inserted_at > c.expected_max_lag)
			           THEN 'max_lag_exceeded'
			         ELSE ''
			       END AS reason
			  FROM contract c
			  LEFT JOIN run_rollup r ON r.id = c.id
		)
		SELECT id, severity, min_rows, EXTRACT(EPOCH FROM expected_window)::int,
		       EXTRACT(EPOCH FROM expected_max_lag)::int, rows_in_window,
		       last_inserted_at, last_ok_at, collector_liveness_s, reason
		  FROM contract_status
		 WHERE reason <> ''
		 ORDER BY CASE severity WHEN 'required' THEN 0 ELSE 1 END, id
		 LIMIT 50`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	violations := []map[string]any{}
	required := 0
	for rows.Next() {
		var id, severity, reason string
		var minRows, rowsInWindow, collectorLivenessSec int
		var expectedWindowSec, expectedMaxLagSec *int
		var lastInserted, lastOKAt *time.Time
		if err := rows.Scan(
			&id, &severity, &minRows, &expectedWindowSec, &expectedMaxLagSec,
			&rowsInWindow, &lastInserted, &lastOKAt, &collectorLivenessSec, &reason,
		); err != nil {
			return nil, 0, err
		}
		if severity == "required" {
			required++
		}
		row := map[string]any{
			"source":                       id,
			"severity":                     severity,
			"reason":                       reason,
			"expected_min_rows_per_window": minRows,
			"rows_in_window":               rowsInWindow,
		}
		if expectedWindowSec != nil {
			row["expected_window_s"] = *expectedWindowSec
		}
		if expectedMaxLagSec != nil {
			row["expected_max_lag_s"] = *expectedMaxLagSec
		}
		if lastInserted != nil {
			row["last_inserted_at"] = lastInserted.UTC()
		}
		if lastOKAt != nil {
			row["last_ok_at"] = lastOKAt.UTC()
		}
		row["collector_liveness_s"] = collectorLivenessSec
		violations = append(violations, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	status := "ok"
	if len(violations) > 0 {
		status = "degraded"
	}
	return map[string]any{
		"status":          status,
		"violation_count": len(violations),
		"violations":      violations,
	}, required, nil
}

func intValue(v any) int {
	if n, ok := v.(int); ok {
		return n
	}
	return 0
}
