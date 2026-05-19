// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package sources

import (
	"testing"
	"time"
)

func TestSourceFreshnessViolationForSparseQuietSource(t *testing.T) {
	finished := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	contract := sourceFreshnessContract{
		Enabled: true,
		MinRows: 0,
		Window:  7 * 24 * time.Hour,
		MaxLag:  7 * 24 * time.Hour,
	}

	if got := sourceFreshnessViolationFor("usgs_shakemap", contract, 0, nil, finished); got != nil {
		t.Fatalf("quiet sparse source violation = %#v", got)
	}
}

func TestSourceFreshnessViolationForRowProducingSource(t *testing.T) {
	finished := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	contract := sourceFreshnessContract{
		Enabled: true,
		MinRows: 1,
		Window:  24 * time.Hour,
		MaxLag:  24 * time.Hour,
	}

	if got := sourceFreshnessViolationFor("tor_metrics", contract, 0, nil, finished); got == nil {
		t.Fatal("row-producing source without rows did not violate contract")
	}

	lastInserted := finished.Add(-25 * time.Hour)
	if got := sourceFreshnessViolationFor("tor_metrics", contract, 1, &lastInserted, finished); got == nil {
		t.Fatal("row-producing source beyond max lag did not violate contract")
	}
}
