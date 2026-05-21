// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package cctv_cameras

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gordios45/collector/internal/features"
)

type staticStatusTransport struct{}

func (staticStatusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("unavailable")),
		Request:    req,
	}, nil
}

func TestFetchFeaturesUsesCameraSeedFileWhenLiveSourcesUnavailable(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cameras-*.json")
	if err != nil {
		t.Fatal(err)
	}
	seed := `{"cameras":[{"source":"TrafficVision","name":"Bay Bridge Cam","lat":37.98765,"lon":-122.12345,"streamUrl":"https://camera.example/live.m3u8","feedType":"hls","city":"San Francisco","state":"CA","country":"US","road":"I-80","direction":"W"}]}`
	if _, err := f.WriteString(seed); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	c := &Collector{
		client:     &http.Client{Transport: staticStatusTransport{}},
		cameraFile: f.Name(),
		maxAOIs:    0,
	}
	feats, err := c.FetchFeatures(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(feats) != 1 {
		t.Fatalf("len(feats) = %d, want 1", len(feats))
	}
	got := feats[0]
	if !strings.HasPrefix(got.ExtID, "camera_file:trafficvision:") {
		t.Fatalf("ExtID = %q, want camera file trafficvision id", got.ExtID)
	}
	if got.GeomWKT != "POINT(-122.123450 37.987650)" {
		t.Fatalf("GeomWKT = %q", got.GeomWKT)
	}
	if got.Props["inventory_source"] != "camera_seed_file" {
		t.Fatalf("inventory_source = %v", got.Props["inventory_source"])
	}
	if got.Props["source_provider"] != "TrafficVision" {
		t.Fatalf("source_provider = %v", got.Props["source_provider"])
	}
	if got.Props["stream_url"] != "https://camera.example/live.m3u8" {
		t.Fatalf("stream_url = %v", got.Props["stream_url"])
	}
	if got.Props["last_seen_at"] == "" {
		t.Fatalf("last_seen_at missing")
	}
}

func TestWalkOTCMNestedInventory(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	out := map[string]features.Feature{}
	data := map[string]any{
		"outer": []any{
			map[string]any{
				"latitude":    51.5,
				"longitude":   -0.1,
				"description": "Westminster Traffic Cam",
				"url":         "https://traffic.example/camera.jpg",
			},
		},
	}

	c := &Collector{}
	c.walkOTCM(now, out, "GB", "https://example.test/cameras/GB.json", data)
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	for id, feat := range out {
		if !strings.HasPrefix(id, "otcm:GB:") {
			t.Fatalf("id = %q, want otcm GB id", id)
		}
		if feat.GeomWKT != "POINT(-0.100000 51.500000)" {
			t.Fatalf("GeomWKT = %q", feat.GeomWKT)
		}
		if feat.Props["inventory_source"] != "open_traffic_cam_map" {
			t.Fatalf("inventory_source = %v", feat.Props["inventory_source"])
		}
		if feat.Props["source_provider"] != "OpenTrafficCamMap" {
			t.Fatalf("source_provider = %v", feat.Props["source_provider"])
		}
		if feat.Props["country"] != "GB" {
			t.Fatalf("country = %v", feat.Props["country"])
		}
	}
}

func TestCameraInventoryParsersForAdditionalSources(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	out := map[string]features.Feature{}

	addASFINAGRows(now, out, []map[string]any{{
		"wcs_id":       "AT-1",
		"wgs84_lat":    48.2,
		"wgs84_lon":    16.3,
		"position_txt": "Vienna A23",
		"url_campic":   "https://asfinag.example/cam.jpg",
	}})
	addCameraMapRows(now, out, "ontario511", "ontario_511_cameras", "Ontario 511", ontario511CamerasURL, "https://511on.ca/", "CA", "Ontario 511 Camera", []map[string]any{{
		"id":          "on-1",
		"latitude":    43.7,
		"longitude":   -79.4,
		"description": "Toronto",
		"imageUrl":    "https://on.example/cam.jpg",
	}})
	addCameraMapRows(now, out, "alberta511", "alberta_511_cameras", "Alberta 511", alberta511CamerasURL, "https://511.alberta.ca/", "CA", "Alberta 511 Camera", []map[string]any{{
		"Id":        "ab-1",
		"Latitude":  53.5,
		"Longitude": -113.5,
		"Location":  "Edmonton",
		"Views": []any{
			map[string]any{"Url": "https://ab.example/cam.jpg"},
		},
	}})

	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3", len(out))
	}
	if out["asfinag:AT-1"].Props["inventory_source"] != "asfinag_webcams" {
		t.Fatalf("asfinag inventory_source = %v", out["asfinag:AT-1"].Props["inventory_source"])
	}
	if out["ontario511:on-1"].Props["thumb_url"] != "https://on.example/cam.jpg" {
		t.Fatalf("ontario thumb_url = %v", out["ontario511:on-1"].Props["thumb_url"])
	}
	if out["alberta511:ab-1"].Props["thumb_url"] != "https://ab.example/cam.jpg" {
		t.Fatalf("alberta thumb_url = %v", out["alberta511:ab-1"].Props["thumb_url"])
	}
}
