// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package road_incidents

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gordios45/collector/internal/events"
)

func TestLiveGlobalRoadFeeds(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_COLLECTOR_SMOKE") != "1" {
		t.Skip("set GORDIOS_LIVE_COLLECTOR_SMOKE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, _ := New()
	checks := []struct {
		name string
		rows []events.Event
	}{
		{"autobahn", c.fetchAutobahn(ctx)},
		{"digitraffic", c.fetchDigitraffic(ctx)},
		{"ndw", c.fetchNDW(ctx)},
		{"bison_fute", c.fetchBisonFute(ctx)},
		{"national_highways", c.fetchRSS(ctx, rssFeeds[len(rssFeeds)-2])},
	}
	for _, check := range checks {
		if len(check.rows) == 0 {
			t.Fatalf("expected live rows from %s", check.name)
		}
	}
}
