// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package netblocks

import "testing"

func TestEventFromItemClassifiesShutdown(t *testing.T) {
	ev, ok := eventFromItem(rssItem{
		Title:       "Internet shutdown imposed in Sudan amid protests",
		Link:        "https://netblocks.org/reports/example",
		Description: "Network data show a nationwide internet shutdown across the country.",
		PubDate:     "Thu, 30 Apr 2026 10:00:00 +0000",
		GUID:        "example",
	})
	if !ok {
		t.Fatal("eventFromItem returned false")
	}
	if ev.Source != "netblocks_rss" || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("bad event identity/geocode: %#v", ev)
	}
	if got := ev.Props["country"]; got != "Sudan" {
		t.Fatalf("country = %v, want Sudan", got)
	}
	if got := ev.Props["outage_type"]; got != "internet_shutdown" {
		t.Fatalf("outage_type = %v, want internet_shutdown", got)
	}
	if score, ok := ev.Props["severity_score"].(float64); !ok || score < 3 {
		t.Fatalf("severity_score = %#v, want >= 3", ev.Props["severity_score"])
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestEventFromItemRequiresCountry(t *testing.T) {
	_, ok := eventFromItem(rssItem{Title: "Internet disruption observed", Description: "No country in this synthetic item"})
	if ok {
		t.Fatal("expected false without deterministic country match")
	}
}
