// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package road_traffic_flow

import "testing"

func TestEventFromOpen511UsesPointAndCongestionSignal(t *testing.T) {
	row := open511Event{
		ID:          "drivebc.ca/DBC-1",
		Headline:    "CONSTRUCTION",
		Updated:     "2026-05-18T10:00:00-07:00",
		Description: "Single lane alternating traffic. Expect minor delays.",
		EventType:   "CONSTRUCTION",
		Severity:    "MINOR",
		Geography: open511Geometry{
			Type:        "Point",
			Coordinates: []byte(`[-120.779441,49.664635]`),
		},
	}
	ev, ok := eventFromOpen511(feedSpec{Label: "DriveBC", URL: "https://example.test"}, row)
	if !ok {
		t.Fatalf("event not built")
	}
	if ev.Lat != 49.664635 || ev.Lon != -120.779441 {
		t.Fatalf("point=(%.6f, %.6f)", ev.Lat, ev.Lon)
	}
	if got := ev.Props["traffic_flow_signal"]; got != "lane_restriction" {
		t.Fatalf("traffic_flow_signal=%v", got)
	}
}

func TestParseDATEX2MeasuredData(t *testing.T) {
	raw := []byte(`<d2LogicalModel>
  <payloadPublication>
    <siteMeasurements>
      <measurementSiteReference id="site-1"/>
      <measurementTimeDefault>2026-05-18T12:00:00Z</measurementTimeDefault>
      <measuredValue>
        <measuredValue>
          <basicData>
            <averageVehicleSpeed><speed>23.5</speed></averageVehicleSpeed>
            <vehicleFlow><vehicleFlowRate>1600</vehicleFlowRate></vehicleFlow>
          </basicData>
        </measuredValue>
      </measuredValue>
    </siteMeasurements>
  </payloadPublication>
</d2LogicalModel>`)
	obs := parseDATEX2(raw)
	if len(obs) != 1 {
		t.Fatalf("obs=%d", len(obs))
	}
	if obs[0].SiteID != "site-1" || obs[0].SpeedKPH != 23.5 || obs[0].FlowPerHour != 1600 {
		t.Fatalf("obs=%#v", obs[0])
	}
	evs := eventsFromDATEX2(feedSpec{Label: "DATEX", URL: "https://example.test", Lat: 45, Lon: 9}, raw)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if got := evs[0].Props["congestion_score"]; got != 3.5 {
		t.Fatalf("score=%v", got)
	}
}
