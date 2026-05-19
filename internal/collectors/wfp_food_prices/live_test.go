// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package wfp_food_prices

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveFetch(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_COLLECTOR_TESTS") != "1" {
		t.Skip("set GORDIOS_LIVE_COLLECTOR_TESTS=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	evs, err := c.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) == 0 {
		t.Fatal("expected live WFP food price stress events")
	}
}
