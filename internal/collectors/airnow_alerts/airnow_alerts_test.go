// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package airnow_alerts

import (
	"encoding/xml"
	"testing"
)

func TestAirNowEventFromCAPFields(t *testing.T) {
	item := feedItem{
		ID:          "http://feeds.enviroflash.info/cap/271.xml?id=sample",
		Headline:    "Air Quality Alert for Aberdeen",
		Description: "Particle Pollution is forecast to reach 164 AQI - Unhealthy.",
		Severity:    "Moderate",
		Effective:   "2026-05-05T09:00:00Z",
		Expires:     "2026-05-06T09:00:00Z",
		AreaDesc:    "Aberdeen, WA",
		Circle:      "46.9725,-123.8317 1",
	}
	e, ok := eventFromItem(item)
	if !ok {
		t.Fatal("eventFromItem returned false")
	}
	if e.Source != sourceID || e.ExtID == "" || !e.HasPoint() {
		t.Fatalf("bad event: %+v", e)
	}
	if got := e.Props["air_quality_score"]; got == nil {
		t.Fatalf("missing air_quality_score: %#v", e.Props)
	}
	if got := e.Props["aqi"]; got != 164 {
		t.Fatalf("aqi = %#v, want 164", got)
	}
}

func TestAirNowFeedLinkParsesRSSAndAtomForms(t *testing.T) {
	raw := []byte(`
<feed>
  <entry>
    <id>atom-entry</id>
    <title>Air Quality Alert</title>
    <link href="https://example.test/atom?id=atom-1"></link>
    <info><area><circle>46.9725,-123.8317 1</circle></area></info>
  </entry>
  <channel>
    <item>
      <guid>rss-entry</guid>
      <title>Air Quality Alert</title>
      <link>https://example.test/rss?id=rss-1</link>
      <circle>46.9725,-123.8317 1</circle>
    </item>
  </channel>
</feed>`)
	var env feedEnvelope
	if err := xml.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(env.Entries) != 1 || env.Entries[0].Link.Href != "https://example.test/atom?id=atom-1" {
		t.Fatalf("bad atom link: %+v", env.Entries)
	}
	if len(env.Items) != 1 || env.Items[0].Link.Text != "https://example.test/rss?id=rss-1" {
		t.Fatalf("bad rss link: %+v", env.Items)
	}
}
