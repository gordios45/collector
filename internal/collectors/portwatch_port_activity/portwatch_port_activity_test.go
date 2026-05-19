// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package portwatch_port_activity

import "testing"

func TestActivityScore(t *testing.T) {
	score := activityScore(activityRow{TankerCalls: 100, ImportTanker: 1000, ExportTanker: 500})
	if score <= 0 {
		t.Fatalf("activityScore = %v", score)
	}
}
