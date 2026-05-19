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

func ClampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func FIRMSFRPScore(frp float64) float64 {
	return ClampFloat(math.Sqrt(math.Max(0, frp)/25.0), 0, 3)
}

func GeoThermalFRPScore(frp float64) float64 {
	return ClampFloat(math.Sqrt(math.Max(0, frp)/20.0), 0, 3)
}

func SeismicMagnitudeScore(mag float64) float64 {
	return ClampFloat((mag-4.0)*1.2, 0, 4)
}

func SeismicBlastLikeScore(props map[string]any) float64 {
	score := 0.0
	eventType := strings.ToLower(stringAt(props, "type"))
	place := strings.ToLower(stringAt(props, "place"))
	title := strings.ToLower(stringAt(props, "title"))
	if strings.Contains(eventType, "explosion") || strings.Contains(eventType, "blast") ||
		strings.Contains(place, "explosion") || strings.Contains(title, "explosion") {
		score += 2.0
	}
	if strings.Contains(eventType, "quarry") || strings.Contains(place, "quarry") {
		score += 0.8
	}
	if depth, ok := floatAt(props, "depth_km"); ok {
		switch {
		case depth <= 0:
			score += 0.7
		case depth <= 2:
			score += 0.5
		case depth <= 5:
			score += 0.2
		}
	}
	if mag, ok := floatAt(props, "mag"); ok {
		score += ClampFloat((mag-2.0)/2.0, 0, 1.0)
	}
	return ClampFloat(score, 0, 3)
}

func ThermalBrightnessScore(temp, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return ClampFloat((temp-330.0)/denominator, 0, 3)
}

func GPSJammingIntensityScore(intensity float64, badAircraft int) float64 {
	score := intensity * 4.0
	if badAircraft >= 10 {
		score += 0.35
	}
	return ClampFloat(score, 0, 3)
}

func BGPOutageSeverity(routedRatio float64, missingASNs int) float64 {
	if routedRatio <= 0 {
		return 0
	}
	score := (0.92 - routedRatio) * 8.0
	if missingASNs >= 50 {
		score += 0.3
	}
	return ClampFloat(score, 0, 3)
}

func RISRoutingInstabilityScore(updates, withdrawals int, withdrawalRatio float64) float64 {
	score := math.Log1p(float64(updates))/2.5 + math.Log1p(float64(withdrawals))/2.0 + 2.0*withdrawalRatio
	return ClampFloat(score, 0, 4)
}

func CloudflareOutageScore(eventType, typ, cause string, rank int) float64 {
	text := strings.ToLower(eventType + " " + typ + " " + cause)
	score := 0.0
	if strings.Contains(text, "outage") {
		score += 2.0
	}
	if rank > 0 {
		score += ClampFloat(float64(21-rank)/10.0, 0, 2)
	}
	return ClampFloat(score, 0, 3)
}

func ADSBMilitaryActivityScore(military bool) float64 {
	if !military {
		return 0
	}
	return 1
}

func ADSBEmergencyScore(emergency, squawk string) float64 {
	text := strings.ToLower(emergency + " " + squawk)
	if containsAny(text, "7700", "7600", "7500", "emergency", "radio", "hijack") {
		return 2
	}
	return 0
}

func ADSBLowAltitudeScore(alt, speed float64, onGround bool) float64 {
	if onGround || alt <= 100 || alt >= 1200 || speed <= 40 {
		return 0
	}
	return ClampFloat((1200-alt)/500.0, 0, 2)
}

func GTFSServiceAlertScore(effect, severity string) float64 {
	score := 0.8
	effect = strings.ToLower(effect)
	severity = strings.ToLower(severity)
	if containsAny(effect, "no_service", "significant_delays", "detour", "stop_moved", "reduced_service", "modified_service") {
		score += 1.0
	}
	if containsAny(severity, "severe", "warning") {
		score += 0.8
	}
	return ClampFloat(score, 0, 3)
}

func AlertSeverityScore(props map[string]any) float64 {
	if v, ok := floatAt(props, "severity_code"); ok {
		switch int(v) {
		case 1:
			return 3.0
		case 2:
			return 2.2
		case 3:
			return 1.1
		case 4:
			return 0.3
		}
	}
	switch strings.ToLower(strings.TrimSpace(stringAt(props, "severity"))) {
	case "extreme":
		return 3.0
	case "severe":
		return 2.2
	case "moderate":
		return 1.1
	case "minor":
		return 0.3
	default:
		return 0
	}
}

func AlertUrgencyScore(props map[string]any) float64 {
	if v, ok := floatAt(props, "urgency_code"); ok {
		switch int(v) {
		case 1:
			return 1.5
		case 2:
			return 1.0
		case 3:
			return 0.4
		}
	}
	switch strings.ToLower(strings.TrimSpace(stringAt(props, "urgency"))) {
	case "immediate":
		return 1.5
	case "expected":
		return 1.0
	case "future":
		return 0.4
	default:
		return 0
	}
}

func AlertCertaintyScore(props map[string]any) float64 {
	if v, ok := floatAt(props, "certainty_code"); ok {
		switch int(v) {
		case 1:
			return 1.3
		case 2:
			return 1.0
		case 3:
			return 0.4
		}
	}
	switch strings.ToLower(strings.TrimSpace(stringAt(props, "certainty"))) {
	case "observed":
		return 1.3
	case "likely":
		return 1.0
	case "possible":
		return 0.4
	default:
		return 0
	}
}

func AddAlertScores(props map[string]any) {
	if props == nil || AlertIsNoWarning(AlertHazardText(props)) {
		return
	}
	if score := AlertSeverityScore(props); score > 0 {
		props["severity_score"] = score
	}
	if score := AlertUrgencyScore(props); score > 0 {
		props["urgency_score"] = score
	}
	if score := AlertCertaintyScore(props); score > 0 {
		props["certainty_score"] = score
	}
	props["official_alert_score"] = OfficialAlertScore(props)
}

func AddNWSHazardScores(props map[string]any) {
	text := AlertHazardText(props)
	if AlertIsNoWarning(text) {
		return
	}
	score := OfficialAlertScore(props)
	if HazardTextContains(text, "flash flood", "flash flooding") {
		props["flash_flood_score"] = ClampFloat(score+0.5, 0, 3)
		props["flood_score"] = ClampFloat(score+0.2, 0, 3)
	} else if HazardTextContains(text, "flood", "inundation", "river rise") {
		props["flood_score"] = score
	}
	if HazardTextContains(text, "heavy rain", "excessive rainfall", "rainfall", "rain storm", "rainstorm") {
		props["heavy_rain_score"] = ClampFloat(score*0.8, 0, 2.5)
	}
	if HazardTextContains(text, "red flag warning", "fire weather", "wildfire", "brush fire") {
		props["fire_weather_score"] = ClampFloat(score*0.9, 0, 2.5)
	}
}

func AddWMOHazardScores(props map[string]any) {
	text := AlertHazardText(props)
	if AlertIsNoWarning(text) {
		return
	}
	score := OfficialAlertScore(props)
	if HazardTextContains(text, "flash flood", "riverine flood", "flood", "inundation") {
		props["flood_score"] = score
	}
	if HazardTextContains(text, "rainstorm", "heavy rain", "rainfall", "lluvias", "precipitation") {
		props["heavy_rain_score"] = ClampFloat(score*0.8, 0, 2.5)
	}
	if HazardTextContains(text, "wildfire", "pre fire", "forest fire", "fire danger", "red flag") {
		props["wildfire_score"] = score
	}
}

func OfficialAlertScore(props map[string]any) float64 {
	text := AlertHazardText(props)
	score := 1.0
	switch {
	case strings.Contains(text, "emergency"):
		score += 1.1
	case strings.Contains(text, "warning"):
		score += 0.8
	case strings.Contains(text, "watch"):
		score += 0.45
	case strings.Contains(text, "advisory") || strings.Contains(text, "statement"):
		score += 0.2
	}
	score += 0.16*AlertSeverityScore(props) + 0.12*AlertUrgencyScore(props) + 0.10*AlertCertaintyScore(props)
	return ClampFloat(score, 0, 3)
}

func AlertHazardText(props map[string]any) string {
	fields := []string{
		"event", "headline", "title", "name", "eventname", "category",
		"classification", "description", "htmldescription", "instruction",
		"areaDesc", "comments", "summary", "severity", "alertlevel",
		"episodealertlevel",
	}
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		if v := stringAt(props, field); strings.TrimSpace(v) != "" {
			parts = append(parts, v)
		}
	}
	return strings.ToLower(strings.Join(parts, " "))
}

func HazardTextContains(text string, terms ...string) bool {
	return containsAny(strings.ToLower(text), terms...)
}

func AlertIsNoWarning(text string) bool {
	text = strings.ToLower(text)
	return strings.Contains(text, "no warning") || strings.Contains(text, "no warnings")
}

func containsAny(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, strings.ToLower(term)) {
			return true
		}
	}
	return false
}

func floatAt(props map[string]any, key string) (float64, bool) {
	switch v := props[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func stringAt(props map[string]any, key string) string {
	if props == nil || props[key] == nil {
		return ""
	}
	return fmt.Sprint(props[key])
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
