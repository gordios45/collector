// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gtfs_feed_catalog

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventFromFeedBuildsCollectorHintAndCrowdingMetadata(t *testing.T) {
	raw := []byte(`{
	  "mdb_source_id": 1423,
	  "data_type": "gtfs-rt",
	  "entity_type": ["vp", "sa", "tu"],
	  "provider": "Canberra Metro Operations",
	  "name": "Canberra Metro Light Rail",
	  "features": ["occupancy"],
	  "urls": {
	    "direct_download": "http://files.transport.act.gov.au/feeds/lightrail.pb",
	    "authentication_type": 0,
	    "license": "https://creativecommons.org/licenses/by/4.0/legalcode"
	  },
	  "location": {
	    "bounding_box": {
	      "minimum_latitude": -35.3,
	      "maximum_latitude": -35.2,
	      "minimum_longitude": 149.0,
	      "maximum_longitude": 149.2
	    }
	  }
	}`)
	var feed catalogFeed
	if err := json.Unmarshal(raw, &feed); err != nil {
		t.Fatal(err)
	}
	ev, ok := eventFromFeed("realtime/example.json", feed, time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("event not built")
	}
	if ev.ExtID != "mdb-1423" || ev.Source != sourceID {
		t.Fatalf("identity=%s/%s", ev.Source, ev.ExtID)
	}
	if ev.Lat >= -35.2 || ev.Lon != 149.1 {
		t.Fatalf("centroid=(%.3f, %.3f)", ev.Lat, ev.Lon)
	}
	if got := ev.Props["has_occupancy_metadata"]; got != true {
		t.Fatalf("occupancy=%v", got)
	}
	if got := ev.Props["collector_config_hint"]; got == "" {
		t.Fatalf("missing collector hint")
	}
}

func TestLatestTreePathFilter(t *testing.T) {
	raw := treeResponse{Tree: []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	}{
		{Path: "realtime/a.json", Type: "blob"},
		{Path: "schedule/b.json", Type: "blob"},
		{Path: "realtime", Type: "tree"},
	}}
	buf, _ := json.Marshal(raw)
	var decoded treeResponse
	if err := json.Unmarshal(buf, &decoded); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, item := range decoded.Tree {
		if item.Type == "blob" && item.Path == "realtime/a.json" {
			paths = append(paths, item.Path)
		}
	}
	if len(paths) != 1 {
		t.Fatalf("paths=%v", paths)
	}
}
