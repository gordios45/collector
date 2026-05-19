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

func SeismicShallowScore(depthKm float64) float64 {
	switch {
	case depthKm <= 0:
		return 1.5
	case depthKm <= 2:
		return 1.2
	case depthKm <= 5:
		return 0.7
	case depthKm <= 10:
		return 0.3
	default:
		return 0
	}
}

func SeismicBlastLikeScore(props map[string]any) float64 {
	score := 0.0
	eventType := strings.ToLower(stringAt(props, "type"))
	place := strings.ToLower(stringAt(props, "place"))
	title := strings.ToLower(stringAt(props, "title"))
	text := strings.Join([]string{eventType, place, title}, " ")
	if !blastLikeText(text) {
		return 0
	}
	if containsAny(eventType, "explosion", "blast") ||
		containsAny(place, "explosion", "blast") ||
		containsAny(title, "explosion", "blast") {
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

func AddEMSCSeismicScores(props map[string]any) {
	if props == nil {
		return
	}
	if mag, ok := floatAt(props, "mag"); ok {
		props["mag_score"] = SeismicMagnitudeScore(mag)
	}
	if depth, ok := floatAtAny(props, "depth", "depth_km"); ok {
		if score := SeismicShallowScore(depth); score > 0 {
			props["shallow_score"] = score
		}
	}
	if score := seismicTextBlastScore(props, "evtype", "type", "flynn_region", "title", "place"); score > 0 {
		props["blast_like_score"] = score
	}
}

func AddUSGSShakeMapScores(props map[string]any) {
	if props == nil {
		return
	}
	if mag, ok := floatAtAny(props, "mag", "pager_magnitude"); ok {
		props["mag_score"] = SeismicMagnitudeScore(mag)
	}
	if mmi, ok := floatAtAny(props, "shakemap_maxmmi_grid", "shakemap_maxmmi", "pager_maxmmi", "mmi", "cdi"); ok {
		if score := ClampFloat((mmi-4.0)/2.0, 0, 3); score > 0 {
			props["shaking_score"] = score
		}
	}
	if score := alertColorScore(firstNonEmptyString(
		stringAt(props, "pager_alertlevel"),
		stringAt(props, "alert"),
	)); score > 0 {
		props["pager_alert_score"] = score
	}
	if tsunami, ok := floatAt(props, "tsunami"); ok && tsunami > 0 {
		props["tsunami_flag"] = 1.5
	}
}

func AddNOAATsunamiScores(props map[string]any) {
	if props == nil {
		return
	}
	text := strings.Join([]string{
		stringAt(props, "category"),
		stringAt(props, "title"),
		stringAt(props, "definition"),
		stringAt(props, "note"),
	}, " ")
	if score := tsunamiAlertScore(text); score > 0 {
		props["alert_score"] = score
	}
	if mag, ok := floatAt(props, "magnitude"); ok {
		props["magnitude_score"] = SeismicMagnitudeScore(mag)
	}
}

func AddVolcanoNoticeScores(props map[string]any) {
	if props == nil {
		return
	}
	if score := volcanoAlertScore(firstNonEmptyString(
		stringAt(props, "alert_level"),
		stringAt(props, "highest_alert"),
		stringAt(props, "notice_type"),
		stringAt(props, "notice_type_cd"),
	)); score > 0 {
		props["alert_score"] = score
	}
	if score := aviationColorScore(firstNonEmptyString(
		stringAt(props, "color_code"),
		stringAt(props, "highest_color"),
		stringAt(props, "aviation_colour_code"),
	)); score > 0 {
		props["aviation_color_score"] = score
	}
}

func AddVAACScores(props map[string]any) {
	if props == nil {
		return
	}
	score := 1.0
	text := strings.ToLower(strings.Join([]string{
		stringAt(props, "eruption_details"),
		stringAt(props, "obs_va_cld"),
		stringAt(props, "obs_status"),
		stringAt(props, "remarks"),
	}, " "))
	if strings.Contains(text, "not identifiable") || strings.Contains(text, "not observed") {
		score = 0.4
	} else {
		if strings.Contains(text, "identifiable") || strings.Contains(text, "observed") || stringAt(props, "ash_polygon_wkt") != "" {
			score += 0.9
		}
		if strings.Contains(text, "eruption") || strings.Contains(text, "explosive") || strings.Contains(text, "reported") {
			score += 0.6
		}
	}
	if upper, ok := flightLevelAt(props, "upper_limit_fl"); ok && upper >= 200 {
		score += 0.3
	}
	props["ash_score"] = ClampFloat(score, 0, 3)
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

func AddSPCStormReportScores(props map[string]any) {
	if props == nil {
		return
	}
	typ := strings.ToLower(strings.TrimSpace(stringAt(props, "type")))
	mag, _ := floatAt(props, "magnitude")
	comments := strings.ToLower(stringAt(props, "comments"))
	damageBonus := 0.0
	if strings.Contains(comments, "damage") || strings.Contains(comments, "roof") || strings.Contains(comments, "tree") {
		damageBonus += 0.3
	}
	if strings.Contains(comments, "injur") || strings.Contains(comments, "fatal") {
		damageBonus += 0.5
	}
	var measured float64
	switch typ {
	case "tornado":
		score := ClampFloat(2.0+efScaleScore(stringAt(props, "magnitude"))+damageBonus, 0, 3)
		props["tornado_score"] = score
		measured = score
	case "wind":
		score := ClampFloat(0.8+ClampFloat((mag-50.0)/25.0, 0, 2.0)+damageBonus, 0, 3)
		props["wind_damage_score"] = score
		measured = score
	case "hail":
		inches := hailMagnitudeInches(mag)
		score := ClampFloat(0.6+ClampFloat((inches-0.75)/0.75, 0, 2.2)+damageBonus, 0, 3)
		props["hail_score"] = score
		measured = score
	}
	if measured > 0 {
		props["measured_severe_score"] = measured
	}
}

func AddSWDIRadarScores(props map[string]any) {
	if props == nil {
		return
	}
	dataset := strings.ToLower(stringAt(props, "dataset"))
	cellType := strings.ToLower(stringAt(props, "cell_type"))
	var score float64
	switch {
	case strings.Contains(dataset, "tvs") || strings.Contains(cellType, "tvs"):
		score = 2.5
		props["tvs_score"] = score
	case strings.Contains(dataset, "hail") || strings.Contains(cellType, "hail"):
		score = 1.2
		if size, ok := floatAtAny(props, "max_size", "maxsize", "size"); ok {
			score += ClampFloat(size/2.0, 0, 1.0)
		}
		score = ClampFloat(score, 0, 3)
		props["hail_signature_score"] = score
	case strings.Contains(dataset, "meso") || strings.Contains(cellType, "meso"):
		score = 1.7
		props["mesocyclone_score"] = score
	}
	if score > 0 {
		props["radar_severity_score"] = score
	}
}

func AddTropicalCycloneScores(props map[string]any, cone bool) {
	if props == nil {
		return
	}
	var intensity float64
	if wind, ok := tropicalCycloneWindKT(props); ok {
		intensity = math.Max(intensity, ClampFloat((wind-34.0)/30.0, 0, 3))
	}
	intensity = math.Max(intensity, tropicalCycloneClassScore(firstNonEmptyString(
		stringAt(props, "classification"),
		stringAt(props, "category"),
		stringAt(props, "intensity"),
		stringAt(props, "storm_type"),
		stringAt(props, "nature"),
	)))
	if intensity > 0 {
		props["tc_intensity_score"] = ClampFloat(intensity, 0, 3)
	}
	if pmb, ok := floatAtAny(props, "pressure", "pressure_mb", "pres_mb", "mslp_mb"); ok && pmb > 0 {
		if score := ClampFloat((1000.0-pmb)/35.0, 0, 3); score > 0 {
			props["low_pressure_score"] = score
		}
	}
	if cone {
		props["cone_score"] = 1.0
	}
}

func AddFAAStatusScores(props map[string]any) {
	if props == nil {
		return
	}
	category := strings.ToLower(stringAt(props, "category"))
	reason := strings.ToLower(stringAt(props, "reason") + " " + stringAt(props, "type"))
	avgMin := ParseAviationDelayMinutes(stringAt(props, "avg"))
	maxMin := ParseAviationDelayMinutes(stringAt(props, "max"))
	delayScore := ClampFloat(avgMin/45.0+maxMin/150.0, 0, 2.2)
	var base float64
	switch category {
	case "closure":
		base = 2.4
		props["closure_score"] = base
	case "ground_stop":
		base = math.Max(2.0, delayScore)
		props["ground_stop_score"] = base
	case "ground_delay", "arrive_depart":
		base = delayScore
		if base > 0 {
			props["delay_score"] = base
		}
	case "airspace_flow":
		base = math.Max(1.0, delayScore)
		props["airspace_flow_score"] = base
	default:
		base = delayScore
		if base > 0 {
			props["delay_score"] = base
		}
	}
	if HazardTextContains(reason, "wind", "weather", "thunderstorm", "storm", "snow", "ice", "icing", "low ceiling", "visibility") {
		props["weather_impact_score"] = math.Max(0.8, base)
	}
	if score := AirspaceRestrictionDurationScore(props); score > 0 {
		props["standing_restriction_score"] = score
	}
}

func AirspaceRestrictionDurationScore(props map[string]any) float64 {
	start, end := airspaceRestrictionTimes(props)
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	switch duration := end.Sub(start); {
	case duration >= 30*24*time.Hour:
		return 2.4
	case duration >= 7*24*time.Hour:
		return 1.6
	case duration >= 72*time.Hour:
		return 1.0
	default:
		return 0
	}
}

func ParseAviationDelayMinutes(s string) float64 {
	fields := strings.Fields(strings.ToLower(strings.ReplaceAll(s, ",", " ")))
	var out float64
	for i := 0; i < len(fields); i++ {
		n, err := strconv.ParseFloat(fields[i], 64)
		if err != nil || i+1 >= len(fields) {
			continue
		}
		unit := strings.Trim(fields[i+1], ".")
		switch {
		case strings.HasPrefix(unit, "hour"):
			out += n * 60
		case strings.HasPrefix(unit, "min"):
			out += n
		}
	}
	return out
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

func alertColorScore(raw string) float64 {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "red":
		return 3.0
	case "orange":
		return 2.3
	case "yellow":
		return 1.3
	case "green":
		return 0.4
	default:
		return 0
	}
}

func tsunamiAlertScore(text string) float64 {
	text = strings.ToLower(text)
	switch {
	case strings.Contains(text, "warning"):
		return 3.0
	case strings.Contains(text, "advisory"):
		return 2.1
	case strings.Contains(text, "watch"):
		return 1.6
	case strings.Contains(text, "threat"):
		return 1.2
	case strings.Contains(text, "information") || strings.Contains(text, "statement"):
		return 0.4
	default:
		return 0
	}
}

func volcanoAlertScore(raw string) float64 {
	text := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.Contains(text, "warning"):
		return 3.0
	case strings.Contains(text, "watch"):
		return 2.1
	case strings.Contains(text, "advisory"):
		return 1.3
	case strings.Contains(text, "normal"):
		return 0.2
	default:
		return 0
	}
}

func aviationColorScore(raw string) float64 {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "red":
		return 3.0
	case "orange":
		return 2.2
	case "yellow":
		return 1.2
	case "green":
		return 0.2
	default:
		return 0
	}
}

func efScaleScore(raw string) float64 {
	s := strings.ToUpper(strings.TrimSpace(raw))
	for _, prefix := range []string{"EF", "F"} {
		if strings.HasPrefix(s, prefix) && len(s) > len(prefix) {
			if n, err := strconv.Atoi(s[len(prefix) : len(prefix)+1]); err == nil {
				return ClampFloat(float64(n)*0.35, 0, 1.2)
			}
		}
	}
	return 0
}

func hailMagnitudeInches(v float64) float64 {
	if v >= 10 {
		return v / 100.0
	}
	return v
}

func tropicalCycloneWindKT(props map[string]any) (float64, bool) {
	for _, key := range []string{"wind_kt", "vmax_kt", "max_wind_kt"} {
		if v, ok := floatAt(props, key); ok {
			return v, true
		}
	}
	for _, key := range []string{"intensity", "classification", "category"} {
		raw := stringAt(props, key)
		if raw == "" {
			continue
		}
		if v, ok := leadingFloat(raw); ok {
			if strings.Contains(strings.ToLower(raw), "mph") {
				v *= 0.868976
			}
			return v, true
		}
	}
	return 0, false
}

func tropicalCycloneClassScore(raw string) float64 {
	text := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case containsAny(text, "category 5", "cat 5", "super typhoon", "violent"):
		return 3.0
	case containsAny(text, "category 4", "cat 4", "very strong"):
		return 2.6
	case containsAny(text, "category 3", "cat 3", "major hurricane", "intense"):
		return 2.2
	case containsAny(text, "hurricane", "typhoon", "cyclone"):
		return 1.7
	case containsAny(text, "tropical storm", "severe tropical storm", "storm"):
		return 1.0
	case containsAny(text, "tropical depression", "td"):
		return 0.4
	default:
		return 0
	}
}

func seismicTextBlastScore(props map[string]any, keys ...string) float64 {
	text := strings.ToLower(strings.Join(textFields(props, keys...), " "))
	if !blastLikeText(text) && !blastLikeEventCode(stringAt(props, "evtype")) {
		return 0
	}
	score := 0.0
	if blastLikeText(text) || blastLikeEventCode(stringAt(props, "evtype")) {
		score += 1.8
	}
	if depth, ok := floatAtAny(props, "depth", "depth_km"); ok {
		score += ClampFloat(SeismicShallowScore(depth)/2.0, 0, 0.7)
	}
	if mag, ok := floatAt(props, "mag"); ok {
		score += ClampFloat((mag-2.0)/2.0, 0, 0.6)
	}
	return ClampFloat(score, 0, 3)
}

func blastLikeText(text string) bool {
	return containsAny(strings.ToLower(text), "explosion", "blast", "quarry", "mining", "mine collapse", "rockburst", "rock burst")
}

func blastLikeEventCode(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "kx", "sx", "kr", "sr":
		return true
	default:
		return false
	}
}

func textFields(props map[string]any, keys ...string) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if v := stringAt(props, key); strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func floatAtAny(props map[string]any, keys ...string) (float64, bool) {
	var out float64
	var okAny bool
	for _, key := range keys {
		v, ok := floatAt(props, key)
		if !ok {
			continue
		}
		if !okAny || v > out {
			out = v
			okAny = true
		}
	}
	return out, okAny
}

func flightLevelAt(props map[string]any, key string) (float64, bool) {
	if v, ok := floatAt(props, key); ok {
		return v, true
	}
	raw := strings.ToUpper(strings.TrimSpace(stringAt(props, key)))
	raw = strings.TrimPrefix(raw, "FL")
	return leadingFloat(raw)
}

func airspaceRestrictionTimes(props map[string]any) (time.Time, time.Time) {
	if props == nil {
		return time.Time{}, time.Time{}
	}
	if nested, ok := props["source_payload_validity"].(map[string]any); ok {
		if start := firstTimeFromMap(nested, "valid_start", "valid_from", "validity_start", "start", "start_time"); !start.IsZero() {
			end := firstTimeFromMap(nested, "valid_end", "valid_to", "validity_end", "end", "end_time")
			return start, end
		}
	}
	start := firstTimeFromMap(props,
		"valid_start", "valid_from", "validity_start", "validTimeFrom",
		"start", "start_time", "afp_start_time", "fca_start_date_time", "effective_start", "begin")
	end := firstTimeFromMap(props,
		"valid_end", "valid_to", "validity_end", "validTimeTo",
		"end", "end_time", "afp_end_time", "fca_end_date_time", "effective_end", "expire", "expires")
	if start.IsZero() || end.IsZero() || end.Before(start) {
		if compactStart, compactEnd := notamCompactValidityTimes(props); !compactStart.IsZero() && !compactEnd.IsZero() {
			return compactStart, compactEnd
		}
	}
	return start, end
}

func notamCompactValidityTimes(props map[string]any) (time.Time, time.Time) {
	text := strings.Join(textFields(props, "reason", "description", "text", "notam_text", "notamText"), " ")
	for _, token := range strings.Fields(text) {
		token = strings.Trim(token, ".,;()[]")
		parts := strings.Split(token, "-")
		if len(parts) != 2 || len(parts[0]) != 10 || len(parts[1]) != 10 || !allDigits(parts[0]) || !allDigits(parts[1]) {
			continue
		}
		start, err1 := time.ParseInLocation("0601021504", parts[0], time.UTC)
		end, err2 := time.ParseInLocation("0601021504", parts[1], time.UTC)
		if err1 == nil && err2 == nil && end.After(start) {
			return start.UTC(), end.UTC()
		}
	}
	return time.Time{}, time.Time{}
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

func firstTimeFromMap(props map[string]any, keys ...string) time.Time {
	for _, key := range keys {
		if t, ok := parseTimeValue(props[key]); ok {
			return t
		}
	}
	return time.Time{}
}

func parseTimeValue(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		if x.IsZero() {
			return time.Time{}, false
		}
		return x.UTC(), true
	case string:
		x = strings.TrimSpace(x)
		if x == "" {
			return time.Time{}, false
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05-07", "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UTC(), true
			}
		}
	case fmt.Stringer:
		return parseTimeValue(x.String())
	}
	return time.Time{}, false
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
		return leadingFloat(v)
	default:
		return 0, false
	}
}

func leadingFloat(raw string) (float64, bool) {
	var f float64
	if _, err := fmt.Sscanf(strings.TrimSpace(raw), "%f", &f); err == nil {
		return f, true
	}
	return 0, false
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
