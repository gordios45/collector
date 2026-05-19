// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package sources

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type sourceFreshnessViolation struct {
	Source       string
	Severity     string
	RowsInWindow int
	MinRows      int
	Window       time.Duration
	MaxLag       time.Duration
	LastInserted *time.Time
}

func (v sourceFreshnessViolation) Error() string {
	last := "never"
	if v.LastInserted != nil {
		last = v.LastInserted.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf(
		"freshness_contract_violated: source=%s severity=%s rows_in_window=%d min_rows=%d window=%s max_lag=%s last_inserted_at=%s",
		v.Source, v.Severity, v.RowsInWindow, v.MinRows, v.Window.Round(time.Second), v.MaxLag.Round(time.Second), last,
	)
}

type sourceFreshnessContract struct {
	Enabled  bool
	MinRows  int
	Window   time.Duration
	MaxLag   time.Duration
	Severity string
}

func evaluateSourceFreshnessContract(ctx context.Context, pool *pgxpool.Pool, source string, finished time.Time) (*sourceFreshnessViolation, error) {
	if pool == nil || source == "" {
		return nil, nil
	}
	contract, ok, err := loadSourceFreshnessContract(ctx, pool, source)
	if err != nil || !ok || !contract.Enabled {
		return nil, err
	}
	if contract.Window <= 0 {
		contract.Window = 24 * time.Hour
	}
	if contract.Severity == "" {
		contract.Severity = "degraded"
	}

	var rowsInWindow int
	var lastInserted *time.Time
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)::int,
		       MAX(ingested_at)
		  FROM events
		 WHERE source = $1
		   AND ingested_at >= $2::timestamptz`,
		source, finished.UTC().Add(-contract.Window)).Scan(&rowsInWindow, &lastInserted)
	if err != nil {
		if sourcePgErrIsUndefinedTable(err) {
			return nil, nil
		}
		return nil, err
	}

	violation := sourceFreshnessViolationFor(source, contract, rowsInWindow, lastInserted, finished)
	if violation == nil {
		return nil, nil
	}
	return violation, nil
}

func sourceFreshnessViolationFor(source string, contract sourceFreshnessContract, rowsInWindow int, lastInserted *time.Time, finished time.Time) *sourceFreshnessViolation {
	if contract.Window <= 0 {
		contract.Window = 24 * time.Hour
	}
	if contract.Severity == "" {
		contract.Severity = "degraded"
	}

	violated := contract.MinRows > 0 && rowsInWindow < contract.MinRows

	// Max-lag is meaningful for row-producing contracts. Sparse event-driven
	// feeds can legitimately have no rows in a quiet window; their health is
	// covered by scheduler liveness rather than by inventing heartbeat events.
	if contract.MaxLag > 0 && (contract.MinRows > 0 || lastInserted != nil) {
		if lastInserted == nil || finished.UTC().Sub(lastInserted.UTC()) > contract.MaxLag {
			violated = true
		}
	}
	if !violated {
		return nil
	}
	return &sourceFreshnessViolation{
		Source:       source,
		Severity:     contract.Severity,
		RowsInWindow: rowsInWindow,
		MinRows:      contract.MinRows,
		Window:       contract.Window,
		MaxLag:       contract.MaxLag,
		LastInserted: lastInserted,
	}
}

func loadSourceFreshnessContract(ctx context.Context, pool *pgxpool.Pool, source string) (sourceFreshnessContract, bool, error) {
	var c sourceFreshnessContract
	var windowSec, maxLagSec *float64
	err := pool.QueryRow(ctx, `
		SELECT freshness_contract_enabled,
		       expected_min_rows_per_window,
		       EXTRACT(EPOCH FROM expected_window)::float8,
		       EXTRACT(EPOCH FROM expected_max_lag)::float8,
		       freshness_contract_severity
		  FROM sources
		 WHERE id = $1`,
		source).Scan(&c.Enabled, &c.MinRows, &windowSec, &maxLagSec, &c.Severity)
	if err != nil {
		if sourcePgErrIsUndefinedTable(err) || sourcePgErrIsUndefinedColumn(err) {
			return sourceFreshnessContract{}, false, nil
		}
		return sourceFreshnessContract{}, false, err
	}
	if windowSec != nil && *windowSec > 0 {
		c.Window = time.Duration(*windowSec * float64(time.Second))
	}
	if maxLagSec != nil && *maxLagSec > 0 {
		c.MaxLag = time.Duration(*maxLagSec * float64(time.Second))
	}
	return c, true, nil
}
