// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package wikipedia_pageviews

import "testing"

func TestEventFromResponseScoresPageviewSurge(t *testing.T) {
	body := apiResponse{Items: []item{
		{Article: "Internet_shutdown", Timestamp: "2026042600", Views: 100},
		{Article: "Internet_shutdown", Timestamp: "2026042700", Views: 105},
		{Article: "Internet_shutdown", Timestamp: "2026042800", Views: 95},
		{Article: "Internet_shutdown", Timestamp: "2026042900", Views: 400},
	}}
	ev, ok := eventFromResponse(body, articleWatch{Article: "Internet_shutdown", Label: "Internet shutdown", Lat: 20, Lon: 0.1}, apiBase)
	if !ok {
		t.Fatal("eventFromResponse returned false")
	}
	if ev.Source != "wikipedia_pageviews" || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("bad event identity/geocode: %#v", ev)
	}
	if got := ev.Props["mode"]; got != "pageviews" {
		t.Fatalf("mode = %v, want pageviews", got)
	}
	if score, ok := ev.Props["pageview_surge_score"].(float64); !ok || score <= 1 {
		t.Fatalf("pageview_surge_score = %#v, want > 1", ev.Props["pageview_surge_score"])
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}

func TestParseArticles(t *testing.T) {
	got := parseArticles("Ukraine|Ukraine|49|32,bad,Internet_shutdown|Internet shutdown|20|0.1")
	if len(got) != 2 {
		t.Fatalf("articles len = %d, want 2", len(got))
	}
	if got[1].Article != "Internet_shutdown" {
		t.Fatalf("unexpected articles: %#v", got)
	}
}
