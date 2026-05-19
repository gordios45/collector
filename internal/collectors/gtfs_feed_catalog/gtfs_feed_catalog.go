// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package gtfs_feed_catalog ingests MobilityData GTFS Realtime catalog
// metadata so operators can discover public vehicle/service/crowding feeds
// without hand-maintaining every agency URL.
package gtfs_feed_catalog

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	sourceID      = "gtfs_feed_catalog"
	treeURL       = "https://api.github.com/repos/MobilityData/mobility-database-catalogs/git/trees/main?recursive=1"
	rawCatalogURL = "https://raw.githubusercontent.com/MobilityData/mobility-database-catalogs/main/catalogs/sources/gtfs"
)

type Collector struct {
	maxFeeds int
}

func New() (*Collector, error) {
	return &Collector{maxFeeds: envInt("GTFS_CATALOG_MAX_FEEDS", 50, 1, 500)}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	paths, err := fetchRealtimePaths(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, c.maxFeeds)
	var firstErr error
	for _, p := range paths {
		feed, err := fetchFeed(ctx, p)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ev, ok := eventFromFeed(p, feed, time.Now().UTC())
		if !ok {
			continue
		}
		out = append(out, ev)
		if len(out) >= c.maxFeeds {
			break
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type treeResponse struct {
	Tree []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	} `json:"tree"`
}

func fetchRealtimePaths(ctx context.Context) ([]string, error) {
	var raw treeResponse
	if err := httpx.GetJSON(ctx, treeURL, map[string]string{"Accept": "application/vnd.github+json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw.Tree))
	for _, item := range raw.Tree {
		if item.Type != "blob" || !strings.HasSuffix(item.Path, ".json") {
			continue
		}
		if strings.HasPrefix(item.Path, "catalogs/sources/gtfs/realtime/") {
			out = append(out, strings.TrimPrefix(item.Path, "catalogs/sources/gtfs/"))
			continue
		}
		if strings.HasPrefix(item.Path, "realtime/") {
			out = append(out, item.Path)
		}
	}
	return out, nil
}

func fetchFeed(ctx context.Context, p string) (catalogFeed, error) {
	var feed catalogFeed
	u := strings.TrimRight(rawCatalogURL, "/") + "/" + path.Clean(p)
	if err := httpx.GetJSON(ctx, u, map[string]string{"Accept": "application/json"}, &feed); err != nil {
		return feed, err
	}
	return feed, nil
}

type catalogFeed struct {
	MDBSourceID     int      `json:"mdb_source_id"`
	DataType        string   `json:"data_type"`
	EntityType      []string `json:"entity_type"`
	Provider        string   `json:"provider"`
	Name            string   `json:"name"`
	Note            string   `json:"note"`
	Features        []string `json:"features"`
	Status          string   `json:"status"`
	IsOfficial      bool     `json:"is_official"`
	StaticReference []string `json:"static_reference"`
	URLs            struct {
		DirectDownload        string `json:"direct_download"`
		DirectDownloadURL     string `json:"direct_download_url"`
		AuthenticationType    int    `json:"authentication_type"`
		AuthenticationInfoURL string `json:"authentication_info_url"`
		APIKeyParameterName   string `json:"api_key_parameter_name"`
		License               string `json:"license"`
		LicenseURL            string `json:"license_url"`
	} `json:"urls"`
	Location struct {
		CountryCode     string `json:"country_code"`
		SubdivisionName string `json:"subdivision_name"`
		Municipality    string `json:"municipality"`
		BoundingBox     struct {
			MinimumLatitude  float64 `json:"minimum_latitude"`
			MaximumLatitude  float64 `json:"maximum_latitude"`
			MinimumLongitude float64 `json:"minimum_longitude"`
			MaximumLongitude float64 `json:"maximum_longitude"`
		} `json:"bounding_box"`
	} `json:"location"`
}

func eventFromFeed(catalogPath string, feed catalogFeed, now time.Time) (events.Event, bool) {
	if strings.ToLower(feed.DataType) != "gtfs-rt" || feed.MDBSourceID == 0 {
		return events.Event{}, false
	}
	status := strings.TrimSpace(feed.Status)
	if status == "" {
		status = "active"
	}
	directURL := firstNonEmpty(feed.URLs.DirectDownload, feed.URLs.DirectDownloadURL)
	authType := feed.URLs.AuthenticationType
	props := map[string]any{
		"source_provider":         "MobilityData Mobility Database Catalogs",
		"mdb_source_id":           feed.MDBSourceID,
		"catalog_path":            catalogPath,
		"data_type":               feed.DataType,
		"entity_type":             feed.EntityType,
		"provider":                feed.Provider,
		"name":                    feed.Name,
		"note":                    feed.Note,
		"features":                feed.Features,
		"status":                  status,
		"is_official":             feed.IsOfficial,
		"static_reference":        feed.StaticReference,
		"direct_download_url":     directURL,
		"authentication_type":     authType,
		"authentication_info_url": feed.URLs.AuthenticationInfoURL,
		"api_key_parameter_name":  feed.URLs.APIKeyParameterName,
		"license_url":             firstNonEmpty(feed.URLs.License, feed.URLs.LicenseURL),
		"has_vehicle_positions":   contains(feed.EntityType, "vp"),
		"has_trip_updates":        contains(feed.EntityType, "tu"),
		"has_service_alerts":      contains(feed.EntityType, "sa"),
		"has_occupancy_metadata":  contains(feed.Features, "occupancy"),
		"collector_config_hint":   feedSpecHint(feed, directURL),
		"source_catalog_endpoint": treeURL,
		"source_payload_validity": map[string]any{
			"valid_start":    now.Format(time.RFC3339),
			"valid_end":      now.Add(48 * time.Hour).Format(time.RFC3339),
			"validity_basis": "mobility_database_catalog_refresh",
		},
	}
	lat, lon, _ := bboxCentroid(feed)
	return events.Event{
		Ts:     now,
		Source: sourceID,
		ExtID:  fmt.Sprintf("mdb-%d", feed.MDBSourceID),
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true
}

func feedSpecHint(feed catalogFeed, directURL string) string {
	if directURL == "" || feed.URLs.AuthenticationType != 0 {
		return ""
	}
	if !(contains(feed.EntityType, "vp") || contains(feed.EntityType, "sa")) {
		return ""
	}
	label := strings.TrimSpace(feed.Provider)
	if feed.Name != "" {
		label = strings.TrimSpace(label + " " + feed.Name)
	}
	label = strings.ReplaceAll(label, "|", " ")
	lat, lon, ok := bboxCentroid(feed)
	if ok {
		return fmt.Sprintf("%s|%s|%.5f|%.5f", label, directURL, lat, lon)
	}
	return label + "|" + directURL
}

func bboxCentroid(feed catalogFeed) (float64, float64, bool) {
	b := feed.Location.BoundingBox
	if b.MinimumLatitude == 0 && b.MaximumLatitude == 0 && b.MinimumLongitude == 0 && b.MaximumLongitude == 0 {
		return 0, 0, false
	}
	lat := (b.MinimumLatitude + b.MaximumLatitude) / 2
	lon := (b.MinimumLongitude + b.MaximumLongitude) / 2
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return 0, 0, false
	}
	return lat, lon, true
}

func contains(xs []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	for _, x := range xs {
		if strings.ToLower(strings.TrimSpace(x)) == want {
			return true
		}
	}
	return false
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func envInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return def
	}
	if n > max {
		return max
	}
	return n
}
