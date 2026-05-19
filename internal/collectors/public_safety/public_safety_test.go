// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package public_safety

import (
	"testing"
	"time"
)

func TestEventAddsSubtypeScores(t *testing.T) {
	labels := incidentLabels("Brush Fire")
	props := baseProps("Test CAD", "https://example.test", "https://example.test", "1", "Brush Fire", labels)
	props["incident_score"] = incidentScore("Brush Fire", labels)

	ev := event(time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC), "1", 32.7, -117.1, props)
	if ev.Props["vegetation_fire_report_score"] == nil {
		t.Fatalf("missing vegetation fire score: %+v", ev.Props)
	}
	if ev.Props["structure_fire_report_score"] != nil {
		t.Fatalf("brush fire should not score as structure fire: %+v", ev.Props)
	}
}
