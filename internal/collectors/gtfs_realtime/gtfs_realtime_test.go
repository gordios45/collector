// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gtfs_realtime

import (
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func TestVehicleEventsSeparatesObservationAndValidity(t *testing.T) {
	rel := gtfs.TripDescriptor_SCHEDULED
	msg := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1777723200)},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("entity-1"),
			Vehicle: &gtfs.VehiclePosition{
				Trip: &gtfs.TripDescriptor{
					TripId:               proto.String("trip-1"),
					RouteId:              proto.String("route-1"),
					ScheduleRelationship: &rel,
				},
				Vehicle: &gtfs.VehicleDescriptor{Id: proto.String("veh-1"), Label: proto.String("Train 1")},
				Position: &gtfs.Position{
					Latitude:  proto.Float32(41.9028),
					Longitude: proto.Float32(12.4964),
					Speed:     proto.Float32(11.5),
					Bearing:   proto.Float32(180),
				},
				Timestamp: proto.Uint64(1777723140),
			},
		}},
	}
	now := time.Date(2026, 5, 2, 12, 1, 0, 0, time.UTC)
	evs := vehicleEvents(feedSpec{Label: "rome", URL: "https://agency.example/vehicles.pb"}, msg, now, now)
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	ev := evs[0]
	if ev.Source != "gtfs_realtime" || ev.ExtID != "rome:vehicle:veh-1" {
		t.Fatalf("identity=%s/%s", ev.Source, ev.ExtID)
	}
	if ev.Ts != time.Unix(1777723140, 0).UTC() {
		t.Fatalf("ts=%s", ev.Ts)
	}
	if got := ev.Props["route_id"]; got != "route-1" {
		t.Fatalf("route_id=%v", got)
	}
	validity, ok := ev.Props["source_payload_validity"].(map[string]any)
	if !ok {
		t.Fatalf("missing validity")
	}
	if validity["validity_basis"] != "gtfs_rt_snapshot_freshness" {
		t.Fatalf("validity=%#v", validity)
	}
}

func TestAlertEventsUseConfiguredCentroidAndActivePeriod(t *testing.T) {
	lat, lon := 41.9, 12.5
	effect := gtfs.Alert_NO_SERVICE
	severity := gtfs.Alert_SEVERE
	msg := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1777723200)},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("alert-1"),
			Alert: &gtfs.Alert{
				Effect:        &effect,
				SeverityLevel: &severity,
				ActivePeriod: []*gtfs.TimeRange{{
					Start: proto.Uint64(1777723100),
					End:   proto.Uint64(1777726800),
				}},
				HeaderText: &gtfs.TranslatedString{Translation: []*gtfs.TranslatedString_Translation{{
					Text: proto.String("Line suspended"),
				}}},
			},
		}},
	}
	evs := alertEvents(feedSpec{Label: "rome", URL: "https://agency.example/alerts.pb", Lat: &lat, Lon: &lon}, msg, time.Now(), time.Now())
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Lat != lat || evs[0].Lon != lon {
		t.Fatalf("centroid=(%.2f, %.2f)", evs[0].Lat, evs[0].Lon)
	}
	if got := evs[0].Props["effect"]; got != "NO_SERVICE" {
		t.Fatalf("effect=%v", got)
	}
	if got := evs[0].Props["service_alert_score"]; got != 2.6 {
		t.Fatalf("service_alert_score=%v", got)
	}
	if got := evs[0].Props["valid_start"]; got != "2026-05-02T11:58:20Z" {
		t.Fatalf("valid_start=%v", got)
	}
}

func TestCrowdingScore(t *testing.T) {
	if got := crowdingScore("STANDING_ROOM_ONLY", 0); got != 3 {
		t.Fatalf("standing score=%v", got)
	}
	if got := crowdingScore("NO_DATA_AVAILABLE", 75); got != 3 {
		t.Fatalf("pct score=%v", got)
	}
	if hasCrowdingSignal("NO_DATA_AVAILABLE", 0) {
		t.Fatalf("unexpected empty crowding signal")
	}
	if !hasCrowdingSignal("FEW_SEATS_AVAILABLE", 0) {
		t.Fatalf("missing status crowding signal")
	}
}
