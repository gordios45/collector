// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package thailand_tmd_alerts

import (
	"strings"
	"testing"
)

func TestParseBulletins(t *testing.T) {
	html := `<div class="link-list-content">
<div class="link-list-title"><a href="/en/warning-and-events/warning-storm/test">Heavy to very heavy rains in Thailand</a></div>
<div class="link-list-description"><a href="/en/warning-and-events/warning-storm/test">People should beware of flash flood.</a></div>
<div class="link-list-caption caption d-flex mt-2"><div>Date:</div><div>18 May 2026</div><div>|</div></div>
</div>`
	rows, err := parseBulletins(strings.NewReader(html), defaultURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows", len(rows))
	}
	if rows[0].Title != "Heavy to very heavy rains in Thailand" || rows[0].Date != "18 May 2026" {
		t.Fatalf("bad row: %+v", rows[0])
	}
	evs := eventsFromBulletins(defaultURL, rows)
	if len(evs) != 1 || evs[0].Props["hazard_type"] != "heavy_rain" {
		t.Fatalf("bad event: %+v", evs)
	}
}
