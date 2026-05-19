// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// GTFS Realtime collector.
//
// Configure feeds with:
//
//	GTFS_RT_VEHICLE_URLS="agency|https://.../vehicle_positions.pb|lat|lon,..."
//	GTFS_RT_ALERT_URLS="agency|https://.../alerts.pb|lat|lon,..."
//
// GTFS-RT feeds are public/open for many agencies but discovery and auth are
// agency-specific, so the collector is intentionally feed-list driven.
package gtfs_realtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
)

type Collector struct {
	vehicleFeeds []feedSpec
	alertFeeds   []feedSpec
	client       *http.Client
}

type feedSpec struct {
	Label string
	URL   string
	Lat   *float64
	Lon   *float64
}

func New() (*Collector, error) {
	vehicles := parseFeeds(os.Getenv("GTFS_RT_VEHICLE_URLS"))
	alerts := parseFeeds(os.Getenv("GTFS_RT_ALERT_URLS"))
	if len(vehicles) == 0 && len(alerts) == 0 {
		return nil, fmt.Errorf("set GTFS_RT_VEHICLE_URLS or GTFS_RT_ALERT_URLS")
	}
	return &Collector{
		vehicleFeeds: vehicles,
		alertFeeds:   alerts,
		client:       &http.Client{Timeout: 35 * time.Second},
	}, nil
}

func (c *Collector) ID() string { return "gtfs_realtime" }
func (c *Collector) PollEvery() time.Duration {
	return time.Duration(envInt("GTFS_RT_POLL_SECONDS", 60)) * time.Second
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	var lastErr error
	now := time.Now().UTC()
	for _, feed := range c.vehicleFeeds {
		msg, fetchedAt, err := c.fetchFeed(ctx, feed)
		if err != nil {
			lastErr = err
			continue
		}
		out = append(out, vehicleEvents(feed, msg, fetchedAt, now)...)
	}
	for _, feed := range c.alertFeeds {
		msg, fetchedAt, err := c.fetchFeed(ctx, feed)
		if err != nil {
			lastErr = err
			continue
		}
		out = append(out, alertEvents(feed, msg, fetchedAt, now)...)
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

func (c *Collector) fetchFeed(ctx context.Context, feed feedSpec) (*gtfs.FeedMessage, time.Time, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, feed.URL, nil)
	req.Header.Set("Accept", "application/x-protobuf, application/octet-stream")
	req.Header.Set("User-Agent", "gordios/0.1")
	if token := strings.TrimSpace(os.Getenv("GTFS_RT_BEARER_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key := strings.TrimSpace(os.Getenv("GTFS_RT_API_KEY")); key != "" {
		header := strings.TrimSpace(os.Getenv("GTFS_RT_API_KEY_HEADER"))
		if header == "" {
			header = "x-api-key"
		}
		req.Header.Set(header, key)
	}

	r, err := c.client.Do(req)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if r.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Errorf("gtfs_rt %s %d: %s", feed.Label, r.StatusCode, string(body[:min(len(body), 400)]))
	}
	var msg gtfs.FeedMessage
	if err := proto.Unmarshal(body, &msg); err != nil {
		return nil, time.Time{}, fmt.Errorf("parse gtfs_rt %s: %w", feed.Label, err)
	}
	return &msg, time.Now().UTC(), nil
}

func vehicleEvents(feed feedSpec, msg *gtfs.FeedMessage, fetchedAt, now time.Time) []events.Event {
	snap := feedTimestamp(msg, fetchedAt)
	out := make([]events.Event, 0, len(msg.GetEntity()))
	for _, entity := range msg.GetEntity() {
		v := entity.GetVehicle()
		if v == nil || v.GetPosition() == nil {
			continue
		}
		pos := v.GetPosition()
		lat, lon := float64(pos.GetLatitude()), float64(pos.GetLongitude())
		if !validLatLon(lat, lon) {
			continue
		}
		ts := snap
		if v.GetTimestamp() > 0 {
			ts = time.Unix(int64(v.GetTimestamp()), 0).UTC()
		}
		trip := v.GetTrip()
		veh := v.GetVehicle()
		vehicleID := firstNonEmpty(veh.GetId(), veh.GetLabel(), entity.GetId())
		props := map[string]any{
			"source_provider":       "gtfs_realtime",
			"event_kind":            "vehicle_position",
			"agency":                feed.Label,
			"feed_url_host":         feedHost(feed.URL),
			"feed_entity_id":        entity.GetId(),
			"vehicle_id":            veh.GetId(),
			"vehicle_label":         veh.GetLabel(),
			"license_plate":         veh.GetLicensePlate(),
			"trip_id":               trip.GetTripId(),
			"route_id":              trip.GetRouteId(),
			"direction_id":          trip.GetDirectionId(),
			"start_date":            trip.GetStartDate(),
			"start_time":            trip.GetStartTime(),
			"schedule_relationship": trip.GetScheduleRelationship().String(),
			"stop_id":               v.GetStopId(),
			"current_status":        v.GetCurrentStatus().String(),
			"congestion_level":      v.GetCongestionLevel().String(),
			"occupancy_status":      v.GetOccupancyStatus().String(),
			"occupancy_percentage":  v.GetOccupancyPercentage(),
			"crowding_score":        crowdingScore(v.GetOccupancyStatus().String(), int(v.GetOccupancyPercentage())),
			"has_crowding_signal":   hasCrowdingSignal(v.GetOccupancyStatus().String(), int(v.GetOccupancyPercentage())),
			"speed_m_s":             float64(pos.GetSpeed()),
			"bearing_deg":           float64(pos.GetBearing()),
			"observed_start":        ts.Format(time.RFC3339),
			"observed_end":          snap.Format(time.RFC3339),
			"source_payload_validity": map[string]any{
				"valid_start":    ts.Format(time.RFC3339),
				"valid_end":      now.Add(2 * time.Minute).Format(time.RFC3339),
				"validity_basis": "gtfs_rt_snapshot_freshness",
			},
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "gtfs_realtime",
			ExtID:  feed.Label + ":vehicle:" + vehicleID,
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out
}

func hasCrowdingSignal(status string, pct int) bool {
	status = strings.ToUpper(strings.TrimSpace(status))
	return pct > 0 || (status != "" && status != "EMPTY" && status != "NO_DATA_AVAILABLE")
}

func crowdingScore(status string, pct int) float64 {
	if pct > 0 {
		if pct >= 100 {
			return 4
		}
		return float64(pct) / 25.0
	}
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "FULL", "CRUSHED_STANDING_ROOM_ONLY":
		return 4
	case "STANDING_ROOM_ONLY":
		return 3
	case "FEW_SEATS_AVAILABLE":
		return 2
	case "MANY_SEATS_AVAILABLE":
		return 1
	default:
		return 0
	}
}

func alertEvents(feed feedSpec, msg *gtfs.FeedMessage, fetchedAt, now time.Time) []events.Event {
	if feed.Lat == nil || feed.Lon == nil || !validLatLon(*feed.Lat, *feed.Lon) {
		return nil
	}
	snap := feedTimestamp(msg, fetchedAt)
	var out []events.Event
	for _, entity := range msg.GetEntity() {
		a := entity.GetAlert()
		if a == nil {
			continue
		}
		validStart, validEnd := alertValidity(a, snap)
		props := map[string]any{
			"source_provider": "gtfs_realtime",
			"event_kind":      "service_alert",
			"agency":          feed.Label,
			"feed_url_host":   feedHost(feed.URL),
			"feed_entity_id":  entity.GetId(),
			"cause":           a.GetCause().String(),
			"effect":          a.GetEffect().String(),
			"severity_level":  a.GetSeverityLevel().String(),
			"service_alert_score": collectorutil.GTFSServiceAlertScore(
				a.GetEffect().String(),
				a.GetSeverityLevel().String(),
			),
			"header_text":    translatedText(a.GetHeaderText()),
			"description":    translatedText(a.GetDescriptionText()),
			"valid_start":    validStart.Format(time.RFC3339),
			"valid_end":      validEnd.Format(time.RFC3339),
			"validity_basis": "gtfs_rt_alert_active_period",
		}
		out = append(out, events.Event{
			Ts:     snap,
			Source: "gtfs_realtime",
			ExtID:  feed.Label + ":alert:" + firstNonEmpty(entity.GetId(), fmt.Sprint(len(out))),
			Lat:    *feed.Lat,
			Lon:    *feed.Lon,
			Props:  props,
		})
	}
	return out
}

func feedTimestamp(msg *gtfs.FeedMessage, fallback time.Time) time.Time {
	if msg != nil && msg.GetHeader() != nil && msg.GetHeader().GetTimestamp() > 0 {
		return time.Unix(int64(msg.GetHeader().GetTimestamp()), 0).UTC()
	}
	return fallback.UTC()
}

func alertValidity(a *gtfs.Alert, fallback time.Time) (time.Time, time.Time) {
	start, end := fallback, fallback.Add(2*time.Hour)
	for i, tr := range a.GetActivePeriod() {
		s := start
		e := end
		if tr.GetStart() > 0 {
			s = time.Unix(int64(tr.GetStart()), 0).UTC()
		}
		if tr.GetEnd() > 0 {
			e = time.Unix(int64(tr.GetEnd()), 0).UTC()
		}
		if i == 0 || s.Before(start) {
			start = s
		}
		if e.After(end) {
			end = e
		}
	}
	if end.Before(start) {
		end = start
	}
	return start, end
}

func translatedText(ts *gtfs.TranslatedString) string {
	for _, tr := range ts.GetTranslation() {
		if text := strings.TrimSpace(tr.GetText()); text != "" {
			return text
		}
	}
	return ""
}

func parseFeeds(raw string) []feedSpec {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []feedSpec
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, "|")
		if len(parts) == 1 {
			label, value := splitLabelValue(item)
			if value == "" {
				continue
			}
			out = append(out, feedSpec{Label: firstNonEmpty(label, feedHost(value)), URL: value})
			continue
		}
		spec := feedSpec{Label: strings.TrimSpace(parts[0]), URL: strings.TrimSpace(parts[1])}
		if spec.Label == "" {
			spec.Label = feedHost(spec.URL)
		}
		if len(parts) >= 4 {
			lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
			lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
			if err1 == nil && err2 == nil && validLatLon(lat, lon) {
				spec.Lat = &lat
				spec.Lon = &lon
			}
		}
		if spec.URL != "" {
			out = append(out, spec)
		}
	}
	return out
}

func splitLabelValue(raw string) (string, string) {
	for _, sep := range []string{"=", "|"} {
		if idx := strings.Index(raw, sep); idx > 0 {
			return strings.TrimSpace(raw[:idx]), strings.TrimSpace(raw[idx+1:])
		}
	}
	return "", raw
}

func feedHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "gtfs_rt"
	}
	return strings.ReplaceAll(u.Host, ":", "_")
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x = strings.TrimSpace(x); x != "" {
			return x
		}
	}
	return ""
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
