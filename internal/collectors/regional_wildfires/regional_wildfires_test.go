// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package regional_wildfires

import (
	"testing"
	"time"
)

func TestEventAddsWildfireSubtypeScores(t *testing.T) {
	props := baseProps("Test fire feed", "https://example.test", "https://example.test", "fire-1", "Emergency Warning Fire")
	props["area_acres"] = 2500.0
	props["percent_contained"] = 10.0
	props["status"] = "Emergency warning, out of control"
	props["wildfire_context_score"] = wildfireScore(props)

	ev := event(time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC), "fire-1", -34.9, 138.6, props)
	for _, key := range []string{"large_fire_score", "low_containment_score", "uncontrolled_fire_score"} {
		if ev.Props[key] == nil {
			t.Fatalf("missing %s: %+v", key, ev.Props)
		}
	}
}
