// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package utility_outages

import (
	"testing"
	"time"
)

func TestEventAddsOutageSubtypeScores(t *testing.T) {
	props := baseProps("Test utility", "https://example.test", "https://example.test", "outage-1", 6000)
	props["outage_score"] = outageScore(6000, "substation", "crew onsite")

	ev := event(time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC), "outage-1", 33.7, -84.4, "", props)
	if ev.Props["affected_customers_score"] == nil || ev.Props["major_outage_score"] == nil {
		t.Fatalf("missing outage subtype scores: %+v", ev.Props)
	}
}
