// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package road_incidents

import "testing"

func TestParseDATEXEvents(t *testing.T) {
	raw := []byte(`<mc:messageContainer xmlns:sit="http://datex2.eu/schema/3/situation" xmlns:loc="http://datex2.eu/schema/3/locationReferencing" xmlns:com="http://datex2.eu/schema/3/common" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<sit:situation id="NDW-1"><sit:overallSeverity>high</sit:overallSeverity><sit:situationVersionTime>2026-05-06T07:06:00Z</sit:situationVersionTime><sit:headerInformation><com:informationStatus>real</com:informationStatus></sit:headerInformation><sit:situationRecord xsi:type="sit:VehicleObstruction"><sit:probabilityOfOccurrence>certain</sit:probabilityOfOccurrence><sit:locationReference><loc:pointCoordinates><loc:latitude>52.36802</loc:latitude><loc:longitude>6.4190507</loc:longitude></loc:pointCoordinates></sit:locationReference><sit:generalPublicComment><com:comment><com:values><com:value lang="nl">Blocked lane near test road</com:value></com:values></com:comment></sit:generalPublicComment></sit:situationRecord></sit:situation>
</mc:messageContainer>`)
	events := parseDATEXEvents("NDW", "https://example.test/feed.xml", "https://example.test", "Netherlands", "ndw", raw)
	if len(events) != 1 {
		t.Fatalf("expected one event, got %d", len(events))
	}
	ev := events[0]
	if ev.Source != sourceID || ev.ExtID == "" || !ev.HasPoint() {
		t.Fatalf("bad event: %+v", ev)
	}
	if ev.Props["country"] != "Netherlands" || ev.Props["severity"] != "high" {
		t.Fatalf("bad props: %+v", ev.Props)
	}
	if ev.Props["closure_score"] == nil {
		t.Fatalf("missing closure score: %+v", ev.Props)
	}
}
