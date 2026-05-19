// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"testing"
	"time"
)

func TestSourceDisplayName(t *testing.T) {
	if got := sourceDisplayName("globalping_measurements", nil); got != "Globalping Measurements" {
		t.Fatalf("display name = %q", got)
	}
	if got := sourceDisplayName("ignored", map[string]any{"name": "Custom Feed"}); got != "Custom Feed" {
		t.Fatalf("config display name = %q", got)
	}
}

func TestSourceStatusValue(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	poll := 600
	okAt := now.Add(-10 * time.Minute)
	staleAt := now.Add(-2 * time.Hour)
	errText := "upstream 500"

	status, stale, staleAfter := sourceStatusValue("firms", true, &poll, &okAt, nil, 0, nil, now)
	if status != "ok" || stale || staleAfter == nil || *staleAfter != 3600 {
		t.Fatalf("ok status = %s stale=%v staleAfter=%v", status, stale, staleAfter)
	}

	status, stale, _ = sourceStatusValue("firms", true, &poll, &staleAt, nil, 0, nil, now)
	if status != "stale" || !stale {
		t.Fatalf("stale status = %s stale=%v", status, stale)
	}

	status, stale, _ = sourceStatusValue("firms", true, &poll, &okAt, &errText, 0, nil, now)
	if status != "error" || stale {
		t.Fatalf("error status = %s stale=%v", status, stale)
	}

	freshnessErr := "freshness_contract_violated: source=netblocks_rss rows_in_window=0"
	status, stale, _ = sourceStatusValue("netblocks_rss", true, &poll, &okAt, &freshnessErr, 1, nil, now)
	if status != "freshness_contract_violated" || !stale {
		t.Fatalf("freshness status = %s stale=%v", status, stale)
	}

	status, stale, _ = sourceStatusValue("firms", false, &poll, &okAt, nil, 0, nil, now)
	if status != "disabled" || stale {
		t.Fatalf("disabled status = %s stale=%v", status, stale)
	}

	status, stale, staleAfter = sourceStatusValue("static_context", true, nil, nil, nil, 0, nil, now)
	if status != "static" || stale || staleAfter != nil {
		t.Fatalf("static status = %s stale=%v staleAfter=%v", status, stale, staleAfter)
	}

	runs := []sourceIngestRun{
		{OK: true, RowsInserted: 0},
		{OK: true, RowsInserted: 0},
		{OK: true, RowsInserted: 0},
	}
	status, stale, _ = sourceStatusValue("space_weather", true, &poll, &okAt, nil, 1, runs, now)
	if status != "stale_success" || !stale {
		t.Fatalf("stale-success status = %s stale=%v", status, stale)
	}

	status, stale, _ = sourceStatusValue("usgs_shakemap", true, &poll, &okAt, nil, 0, runs, now)
	if status != "ok" || stale {
		t.Fatalf("sparse quiet-source status = %s stale=%v", status, stale)
	}
}
