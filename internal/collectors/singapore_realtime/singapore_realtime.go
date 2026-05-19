// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package singapore_realtime ingests no-key official Singapore real-time
// environment and traffic-camera signals from data.gov.sg.
package singapore_realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const sourceID = "singapore_realtime"

type Collector struct {
	endpoints []endpoint
	maxCams   int
}

type endpoint struct {
	Name string
	URL  string
	Kind string
}

var defaultEndpoints = []endpoint{
	{Name: "rainfall", URL: "https://api.data.gov.sg/v1/environment/rainfall", Kind: "station_reading"},
	{Name: "air_temperature", URL: "https://api.data.gov.sg/v1/environment/air-temperature", Kind: "station_reading"},
	{Name: "psi", URL: "https://api.data.gov.sg/v1/environment/psi", Kind: "regional_reading"},
	{Name: "pm25", URL: "https://api.data.gov.sg/v1/environment/pm25", Kind: "regional_reading"},
	{Name: "traffic_images", URL: "https://api.data.gov.sg/v1/transport/traffic-images", Kind: "traffic_images"},
}

func New() (*Collector, error) {
	return &Collector{
		endpoints: defaultEndpoints,
		maxCams:   collectorutil.EnvInt("SINGAPORE_REALTIME_MAX_TRAFFIC_CAMERAS", 100, 0, 500),
	}, nil
}

func (c *Collector) ID() string { return sourceID }

func (c *Collector) PollEvery() time.Duration {
	return time.Duration(collectorutil.EnvInt("SINGAPORE_REALTIME_POLL_EVERY_S", 600, 60, 86400)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, ep := range c.endpoints {
		buf, err := httpx.GetBytes(ctx, ep.URL, map[string]string{"Accept": "application/json"})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		var evs []events.Event
		switch ep.Kind {
		case "station_reading":
			evs, err = stationEvents(ep, buf)
		case "regional_reading":
			evs, err = regionalEvents(ep, buf)
		case "traffic_images":
			evs, err = trafficImageEvents(ep, buf, c.maxCams)
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type stationResponse struct {
	Metadata struct {
		Stations []station `json:"stations"`
	} `json:"metadata"`
	Items []struct {
		Timestamp     string           `json:"timestamp"`
		UpdateTS      string           `json:"update_timestamp"`
		Readings      []stationReading `json:"readings"`
		ReadingUnit   string           `json:"reading_unit"`
		ReadingType   string           `json:"reading_type"`
		ApiInfoStatus string           `json:"api_info_status"`
	} `json:"items"`
}

type station struct {
	ID       string `json:"id"`
	DeviceID string `json:"device_id"`
	Name     string `json:"name"`
	Location struct {
		Lat float64 `json:"latitude"`
		Lon float64 `json:"longitude"`
	} `json:"location"`
}

type stationReading struct {
	StationID string  `json:"station_id"`
	Value     float64 `json:"value"`
}

func stationEvents(ep endpoint, body []byte) ([]events.Event, error) {
	var raw stationResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	stations := map[string]station{}
	for _, st := range raw.Metadata.Stations {
		stations[st.ID] = st
	}
	out := []events.Event{}
	for _, item := range raw.Items {
		ts := parseTime(firstNonEmpty(item.Timestamp, item.UpdateTS))
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		for _, reading := range item.Readings {
			st, ok := stations[reading.StationID]
			if !ok || !collectorutil.ValidLatLon(st.Location.Lat, st.Location.Lon) {
				continue
			}
			props := baseProps(ep)
			props["station_id"] = reading.StationID
			props["station_name"] = st.Name
			props["value"] = reading.Value
			props["reading_unit"] = item.ReadingUnit
			props["reading_type"] = firstNonEmpty(item.ReadingType, ep.Name)
			props["timestamp"] = item.Timestamp
			out = append(out, events.Event{
				Ts:     ts,
				Source: sourceID,
				ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s", ep.Name, reading.StationID, ts.Format(time.RFC3339))),
				Lat:    st.Location.Lat,
				Lon:    st.Location.Lon,
				Props:  props,
			})
		}
	}
	return out, nil
}

type regionalResponse struct {
	RegionMetadata []struct {
		Name          string `json:"name"`
		LabelLocation struct {
			Lat float64 `json:"latitude"`
			Lon float64 `json:"longitude"`
		} `json:"label_location"`
	} `json:"region_metadata"`
	Items []struct {
		Timestamp string                                `json:"timestamp"`
		UpdateTS  string                                `json:"update_timestamp"`
		Readings  map[string]map[string]json.RawMessage `json:"readings"`
	} `json:"items"`
}

func regionalEvents(ep endpoint, body []byte) ([]events.Event, error) {
	var raw regionalResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	regions := map[string]struct{ Lat, Lon float64 }{}
	for _, r := range raw.RegionMetadata {
		regions[strings.ToLower(r.Name)] = struct{ Lat, Lon float64 }{r.LabelLocation.Lat, r.LabelLocation.Lon}
	}
	out := []events.Event{}
	for _, item := range raw.Items {
		ts := parseTime(firstNonEmpty(item.Timestamp, item.UpdateTS))
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		for metric, values := range item.Readings {
			for region, rawValue := range values {
				loc, ok := regions[strings.ToLower(region)]
				if !ok || !collectorutil.ValidLatLon(loc.Lat, loc.Lon) {
					continue
				}
				var value any
				_ = json.Unmarshal(rawValue, &value)
				props := baseProps(ep)
				props["region"] = region
				props["metric"] = metric
				props["value"] = value
				props["timestamp"] = item.Timestamp
				out = append(out, events.Event{
					Ts:     ts,
					Source: sourceID,
					ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s|%s", ep.Name, metric, region, ts.Format(time.RFC3339))),
					Lat:    loc.Lat,
					Lon:    loc.Lon,
					Props:  props,
				})
			}
		}
	}
	return out, nil
}

type trafficResponse struct {
	Items []struct {
		Timestamp string `json:"timestamp"`
		Cameras   []struct {
			Timestamp string `json:"timestamp"`
			Image     string `json:"image"`
			Location  struct {
				Lat float64 `json:"latitude"`
				Lon float64 `json:"longitude"`
			} `json:"location"`
			CameraID string `json:"camera_id"`
			ImageMD5 string `json:"image_metadata"`
		} `json:"cameras"`
	} `json:"items"`
}

func trafficImageEvents(ep endpoint, body []byte, maxCams int) ([]events.Event, error) {
	var raw trafficResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	out := []events.Event{}
	for _, item := range raw.Items {
		for _, cam := range item.Cameras {
			if maxCams > 0 && len(out) >= maxCams {
				return out, nil
			}
			if !collectorutil.ValidLatLon(cam.Location.Lat, cam.Location.Lon) {
				continue
			}
			ts := parseTime(firstNonEmpty(cam.Timestamp, item.Timestamp))
			if ts.IsZero() {
				ts = time.Now().UTC()
			}
			props := baseProps(ep)
			props["camera_id"] = cam.CameraID
			props["image_url"] = cam.Image
			props["timestamp"] = cam.Timestamp
			props["metric"] = "traffic_camera_snapshot"
			out = append(out, events.Event{
				Ts:     ts,
				Source: sourceID,
				ExtID:  collectorutil.StableID(fmt.Sprintf("%s|%s|%s", ep.Name, cam.CameraID, ts.Format(time.RFC3339))),
				Lat:    cam.Location.Lat,
				Lon:    cam.Location.Lon,
				Props:  props,
			})
		}
	}
	return out, nil
}

func baseProps(ep endpoint) map[string]any {
	return map[string]any{
		"source_provider":     "data.gov.sg",
		"source_api_endpoint": ep.URL,
		"feed":                ep.Name,
		"country":             "Singapore",
		"country_code":        "SG",
	}
}

func parseTime(raw string) time.Time {
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano} {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}
