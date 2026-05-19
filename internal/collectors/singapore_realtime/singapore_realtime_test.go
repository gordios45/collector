// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package singapore_realtime

import "testing"

func TestStationEvents(t *testing.T) {
	body := []byte(`{"metadata":{"stations":[{"id":"S1","name":"Test Station","location":{"latitude":1.3,"longitude":103.8}}]},"items":[{"timestamp":"2026-05-19T04:08:00+08:00","readings":[{"station_id":"S1","value":12.4}],"reading_unit":"mm","reading_type":"TB1 Rainfall 5 Minute Total F"}]}`)
	evs, err := stationEvents(endpoint{Name: "rainfall", URL: "https://example.test/rainfall", Kind: "station_reading"}, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Props["station_name"] != "Test Station" {
		t.Fatalf("bad events: %+v", evs)
	}
}

func TestRegionalEvents(t *testing.T) {
	body := []byte(`{"region_metadata":[{"name":"central","label_location":{"latitude":1.35,"longitude":103.82}}],"items":[{"timestamp":"2026-05-19T04:08:00+08:00","readings":{"pm25_one_hourly":{"central":18}}}]}`)
	evs, err := regionalEvents(endpoint{Name: "pm25", URL: "https://example.test/pm25", Kind: "regional_reading"}, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Props["metric"] != "pm25_one_hourly" {
		t.Fatalf("bad events: %+v", evs)
	}
}

func TestTrafficImageEvents(t *testing.T) {
	body := []byte(`{"items":[{"timestamp":"2026-05-19T04:08:06+08:00","cameras":[{"timestamp":"2026-05-19T04:06:06+08:00","image":"https://images.data.gov.sg/test.jpg","location":{"latitude":1.31,"longitude":103.87},"camera_id":"1001"}]}]}`)
	evs, err := trafficImageEvents(endpoint{Name: "traffic_images", URL: "https://example.test/traffic-images", Kind: "traffic_images"}, body, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Props["camera_id"] != "1001" {
		t.Fatalf("bad events: %+v", evs)
	}
}
