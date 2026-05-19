// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package ifrc_go

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveFetch(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_COLLECTOR_SMOKE") != "1" {
		t.Skip("set GORDIOS_LIVE_COLLECTOR_SMOKE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, _ := New()
	events, err := c.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected live IFRC GO events")
	}
}
