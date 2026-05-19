// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package copernicus_gdo_drought

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveCoverageFetch(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_COLLECTOR_TESTS") != "1" {
		t.Skip("set GORDIOS_LIVE_COLLECTOR_TESTS=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	img, productDate, endpoint, err := fetchLatestCDI(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if img == nil || productDate.IsZero() || endpoint == "" {
		t.Fatal("expected decoded GDO image, product date, and endpoint")
	}
}
