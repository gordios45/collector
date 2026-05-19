// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package noaa_cpc_gth ingests NOAA CPC Global Tropics Hazards outlook KML
// layers. The feed is keyless and provides week-2/week-3 hazard probability
// polygons for tropical cyclogenesis, precipitation, and temperature anomalies.
package noaa_cpc_gth

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID = "noaa_cpc_global_tropics_hazards"
	baseURL  = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/ghaz/kmzs/"
	docsURL  = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/ghazards/index.php"
)

type Collector struct {
	client *http.Client
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_NOAA_CPC_GTH") == "1" {
		return nil, fmt.Errorf("disabled via GORDIOS_DISABLE_NOAA_CPC_GTH=1")
	}
	return &Collector{client: collectorutil.HTTPClient(90 * time.Second)}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	var firstErr error
	for _, layer := range defaultLayers() {
		evs, err := fetchLayer(ctx, c.client, layer)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, evs...)
	}
	out = dedupe(out)
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type layerSpec struct {
	Code        string
	Week        string
	HazardKind  string
	FeatureName string
	Label       string
}

func defaultLayers() []layerSpec {
	kinds := []struct {
		code, hazard, feature, label string
	}{
		{"TC", "tropical_cyclogenesis", "tropical_cyclogenesis_score", "Tropical cyclogenesis"},
		{"WET", "enhanced_precip", "enhanced_precip_score", "Enhanced precipitation"},
		{"DRY", "suppressed_precip", "suppressed_precip_score", "Suppressed precipitation"},
		{"WARM", "warm_anomaly", "warm_anomaly_score", "Above-normal temperature"},
		{"COLD", "cold_anomaly", "cold_anomaly_score", "Below-normal temperature"},
	}
	out := []layerSpec{}
	for _, week := range []string{"W2", "W3"} {
		for _, k := range kinds {
			out = append(out, layerSpec{
				Code:        week + "_" + k.code,
				Week:        week,
				HazardKind:  k.hazard,
				FeatureName: k.feature,
				Label:       k.label,
			})
		}
	}
	return out
}

func fetchLayer(ctx context.Context, client *http.Client, layer layerSpec) ([]events.Event, error) {
	rawURL := baseURL + layer.Code + ".kml"
	buf, err := httpx.GetBytesWithClient(ctx, client, rawURL, map[string]string{"Accept": "application/vnd.google-earth.kml+xml,application/xml"})
	if err != nil {
		return nil, err
	}
	return parseKML(layer, rawURL, buf)
}

type kmlRoot struct {
	Document kmlDocument `xml:"Document"`
}

type kmlDocument struct {
	Name       string         `xml:"name"`
	Folders    []kmlFolder    `xml:"Folder"`
	Placemarks []kmlPlacemark `xml:"Placemark"`
}

type kmlFolder struct {
	Name       string         `xml:"name"`
	Placemarks []kmlPlacemark `xml:"Placemark"`
}

type kmlPlacemark struct {
	ID            string     `xml:"id,attr"`
	Name          string     `xml:"name"`
	Description   string     `xml:"description"`
	Polygon       kmlPolygon `xml:"Polygon"`
	MultiGeometry multiGeom  `xml:"MultiGeometry"`
}

type multiGeom struct {
	Polygons []kmlPolygon `xml:"Polygon"`
}

type kmlPolygon struct {
	Outer struct {
		Ring struct {
			Coordinates string `xml:"coordinates"`
		} `xml:"LinearRing"`
	} `xml:"outerBoundaryIs"`
}

func parseKML(layer layerSpec, rawURL string, buf []byte) ([]events.Event, error) {
	var root kmlRoot
	if err := xml.Unmarshal(buf, &root); err != nil {
		return nil, err
	}
	docName := strings.TrimSpace(root.Document.Name)
	issued, validStart, validEnd := parseDates(docName)
	if issued.IsZero() {
		issued = time.Now().UTC()
	}
	placemarks := append([]kmlPlacemark{}, root.Document.Placemarks...)
	for _, f := range root.Document.Folders {
		placemarks = append(placemarks, f.Placemarks...)
	}
	out := []events.Event{}
	for i, pm := range placemarks {
		prob := probability(pm.Description)
		if prob <= 0 {
			continue
		}
		lat, lon, ok := centroid(pm)
		if !ok || !collectorutil.ValidLatLon(lat, lon) {
			continue
		}
		score := probabilityScore(layer.HazardKind, prob)
		props := map[string]any{
			"source_provider":         "NOAA Climate Prediction Center",
			"source_api_endpoint":     rawURL,
			"source_public_url":       docsURL,
			"source_provider_kind":    "official_subseasonal_hazard_outlook",
			"layer_code":              layer.Code,
			"week":                    layer.Week,
			"hazard_kind":             layer.HazardKind,
			"hazard_label":            layer.Label,
			"probability_pct":         round(prob, 1),
			layer.FeatureName:         round(score, 2),
			"document_name":           docName,
			"placemark_id":            firstNonEmpty(pm.ID, pm.Name, strconv.Itoa(i)),
			"forecast_valid_start":    formatDate(validStart),
			"forecast_valid_end":      formatDate(validEnd),
			"source_payload_validity": validity(validStart, validEnd, "noaa_cpc_gth_outlook_window"),
		}
		out = append(out, events.Event{
			Ts:     issued,
			Source: sourceID,
			ExtID:  layer.Code + ":" + firstNonEmpty(pm.ID, pm.Name, strconv.Itoa(i)) + ":" + formatDate(validStart),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out, nil
}

var probRe = regexp.MustCompile(`(?is)<td>\s*prob\s*</td>\s*<td>\s*([0-9.]+)\s*</td>`)

func probability(description string) float64 {
	description = html.UnescapeString(description)
	if m := probRe.FindStringSubmatch(description); len(m) > 1 {
		v, _ := strconv.ParseFloat(strings.TrimSpace(m[1]), 64)
		return v
	}
	if v, err := strconv.ParseFloat(strings.TrimSpace(stripTags(description)), 64); err == nil {
		return v
	}
	return 0
}

func centroid(pm kmlPlacemark) (float64, float64, bool) {
	coords := []point{}
	addCoords := func(raw string) {
		coords = append(coords, parseCoordinates(raw)...)
	}
	addCoords(pm.Polygon.Outer.Ring.Coordinates)
	for _, poly := range pm.MultiGeometry.Polygons {
		addCoords(poly.Outer.Ring.Coordinates)
	}
	if len(coords) == 0 {
		return 0, 0, false
	}
	var lat, lon float64
	for _, p := range coords {
		lat += p.Lat
		lon += p.Lon
	}
	return lat / float64(len(coords)), lon / float64(len(coords)), true
}

type point struct {
	Lat float64
	Lon float64
}

func parseCoordinates(raw string) []point {
	out := []point{}
	for _, token := range strings.Fields(strings.TrimSpace(raw)) {
		parts := strings.Split(token, ",")
		if len(parts) < 2 {
			continue
		}
		lon, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		lat, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err1 == nil && err2 == nil && collectorutil.ValidLatLon(lat, lon) {
			out = append(out, point{Lat: lat, Lon: lon})
		}
	}
	return out
}

var dateRe = regexp.MustCompile(`(?i)Issued:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}).*?Valid:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})\s*-\s*([0-9]{2}/[0-9]{2}/[0-9]{4})`)

func parseDates(name string) (time.Time, time.Time, time.Time) {
	m := dateRe.FindStringSubmatch(html.UnescapeString(name))
	if len(m) != 4 {
		return time.Time{}, time.Time{}, time.Time{}
	}
	return parseMDY(m[1]), parseMDY(m[2]), parseMDY(m[3])
}

func parseMDY(s string) time.Time {
	t, err := time.Parse("01/02/2006", strings.TrimSpace(s))
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func probabilityScore(kind string, prob float64) float64 {
	switch kind {
	case "tropical_cyclogenesis":
		return propx.ClampFloat(prob/25.0, 0, 3)
	default:
		return propx.ClampFloat((prob-25.0)/25.0+0.7, 0, 3)
	}
}

func stripTags(s string) string {
	return strings.Join(strings.Fields(regexp.MustCompile(`<[^>]+>`).ReplaceAllString(s, " ")), " ")
}

func validity(start, end time.Time, basis string) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(7 * 24 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
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

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]struct{}{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		key := e.Source + ":" + e.ExtID
		if _, ok := seen[key]; ok || !e.HasPoint() {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, e)
	}
	return out
}
