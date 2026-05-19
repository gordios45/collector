// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package collectorutil contains small shared helpers for collectors that
// sample public feeds over ingestion-owned AOIs.
package collectorutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AOI struct {
	ID        string
	Label     string
	Kind      string
	Lat       float64
	Lon       float64
	Priority  float64
	RadiusM   float64
	Metadata  map[string]any
	StartedAt time.Time
}

var StrategicAOIs = []AOI{
	{ID: "zone:ukraine", Label: "Ukraine", Kind: "strategic_zone", Lat: 48.4, Lon: 31.2, Priority: 2.0},
	{ID: "zone:iran", Label: "Iran", Kind: "strategic_zone", Lat: 32.0, Lon: 53.0, Priority: 2.0},
	{ID: "zone:israel", Label: "Israel", Kind: "strategic_zone", Lat: 31.5, Lon: 35.0, Priority: 1.8},
	{ID: "zone:lebanon", Label: "Lebanon", Kind: "strategic_zone", Lat: 33.9, Lon: 35.8, Priority: 1.8},
	{ID: "zone:taiwan_strait", Label: "Taiwan Strait", Kind: "strategic_zone", Lat: 24.0, Lon: 120.0, Priority: 1.6},
	{ID: "zone:red_sea", Label: "Red Sea", Kind: "strategic_zone", Lat: 20.0, Lon: 38.0, Priority: 1.5},
	{ID: "zone:mediterranean", Label: "Mediterranean", Kind: "strategic_zone", Lat: 38.0, Lon: 20.0, Priority: 1.2},
	{ID: "zone:south_asia", Label: "South Asia", Kind: "strategic_zone", Lat: 25.0, Lon: 78.0, Priority: 1.2},
}

func EnvInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func FirstEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func HTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.ExpectContinueTimeout = 2 * time.Second
	return &http.Client{Timeout: timeout, Transport: transport}
}

func SplitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key := strings.ToLower(part)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, part)
	}
	return out
}

func ValidLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) &&
		!math.IsInf(lat, 0) && !math.IsInf(lon, 0) &&
		lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 &&
		(lat != 0 || lon != 0)
}

func StableID(s string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(s))))
	return fmt.Sprintf("%08x", h.Sum32())
}

func Round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func SelectAOIs(ctx context.Context, pool *pgxpool.Pool, max int, lookback time.Duration, fallback []AOI) []AOI {
	return SelectAOIsForCollector(ctx, pool, "", max, lookback, fallback)
}

func SelectAOIsForCollector(ctx context.Context, pool *pgxpool.Pool, collectorID string, max int, lookback time.Duration, fallback []AOI) []AOI {
	_ = ctx
	_ = lookback
	out := ConfiguredAOIs(ctx, pool, collectorID, max)
	out = append(out, fallback...)
	return LimitAOIs(out, max)
}

func ConfiguredAOIs(ctx context.Context, pool *pgxpool.Pool, collectorID string, max int) []AOI {
	if pool == nil || max <= 0 {
		return nil
	}
	rows, err := pool.Query(ctx, `
		SELECT id, label, kind, lat, lon, priority, COALESCE(radius_m, 0), metadata, updated_at
		  FROM ingestion_aois
		 WHERE enabled
		   AND ($1 = '' OR cardinality(collectors) = 0 OR $1 = ANY(collectors))
		 ORDER BY priority DESC, updated_at DESC
		 LIMIT $2`, strings.TrimSpace(collectorID), max)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []AOI{}
	for rows.Next() {
		var a AOI
		if err := rows.Scan(&a.ID, &a.Label, &a.Kind, &a.Lat, &a.Lon, &a.Priority, &a.RadiusM, &a.Metadata, &a.StartedAt); err == nil && ValidLatLon(a.Lat, a.Lon) {
			out = append(out, a)
		}
	}
	return out
}

func LimitAOIs(in []AOI, max int) []AOI {
	if max <= 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]AOI, 0, minInt(max, len(in)))
	for _, a := range in {
		if !ValidLatLon(a.Lat, a.Lon) || strings.TrimSpace(a.ID) == "" {
			continue
		}
		key := strings.ToLower(a.ID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
		if len(out) >= max {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority > out[j].Priority })
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
