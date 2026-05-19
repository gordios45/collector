// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package health_outbreaks

import "testing"

func TestOutbreakEventGeocodesCountry(t *testing.T) {
	ev := outbreakEvent("WHO Disease Outbreak News", "Cholera outbreak - Sudan", "", "https://example.test", "2026-04-28T12:00:00Z", whoDONURL)
	if ev.Source != "health_outbreaks" || ev.ExtID == "" {
		t.Fatalf("bad event: %#v", ev)
	}
	if ev.Props["country_code"] != "SD" || ev.Props["disease"] != "cholera" || ev.Props["alert_level"] != "alert" {
		t.Fatalf("unexpected props: %#v", ev.Props)
	}
}
