// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NHC forecast cone GIS polygons from active-storm KMZ/KML products.
package nhc_gis_cones

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const currentStormsURL = "https://www.nhc.noaa.gov/CurrentStorms.json"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nhc_gis_cones" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

type advisory struct {
	URL string `json:"url"`
}

type kmz struct {
	ZoomURL string `json:"zoomURL"`
	KMZFile string `json:"kmzFile"`
}

type storm struct {
	ID               string   `json:"id"`
	BinNumber        string   `json:"binNumber"`
	Name             string   `json:"name"`
	Classification   string   `json:"classification"`
	Intensity        string   `json:"intensity"`
	Pressure         string   `json:"pressure"`
	LatitudeNumeric  float64  `json:"latitudeNumeric"`
	LongitudeNumeric float64  `json:"longitudeNumeric"`
	LastUpdate       string   `json:"lastUpdate"`
	PublicAdvisory   advisory `json:"publicAdvisory"`
	ForecastCone     kmz      `json:"forecastCone"`
}

type placemark struct {
	Name        string `xml:"name"`
	Description string `xml:"description"`
	Inner       string `xml:",innerxml"`
}

type kmlPolygon struct {
	Outer string `xml:"outerBoundaryIs>LinearRing>coordinates"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var resp struct {
		ActiveStorms []storm `json:"activeStorms"`
	}
	if err := httpx.GetJSON(ctx, currentStormsURL, nil, &resp); err != nil {
		return nil, err
	}
	out := []events.Event{}
	for _, s := range resp.ActiveStorms {
		if s.ID == "" || s.ForecastCone.KMZFile == "" {
			continue
		}
		productURL := normalizeProductURL(s.ForecastCone.KMZFile)
		buf, err := httpx.GetBytes(ctx, productURL, nil)
		if err != nil {
			continue
		}
		kml, err := kmlBytes(buf, productURL)
		if err != nil {
			continue
		}
		out = append(out, eventsFromKML(kml, s, productURL)...)
	}
	return out, nil
}

func eventsFromKML(kml []byte, s storm, productURL string) []events.Event {
	ts := parseTime(s.LastUpdate)
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	placemarks := placemarksFromKML(kml)
	out := []events.Event{}
	for i, pm := range placemarks {
		for j, poly := range polygonsFromInner(pm.Inner) {
			ring := parseCoordinates(poly.Outer)
			wkt := polygonWKT(ring)
			if wkt == "" {
				continue
			}
			lon, lat := centroid(ring)
			props := map[string]any{
				"storm_id":            s.ID,
				"storm_name":          s.Name,
				"binNumber":           s.BinNumber,
				"classification":      s.Classification,
				"intensity":           s.Intensity,
				"pressure":            s.Pressure,
				"lastUpdate":          s.LastUpdate,
				"advisoryUrl":         s.PublicAdvisory.URL,
				"product_type":        "forecast_cone",
				"product_url":         productURL,
				"placemark_name":      pm.Name,
				"description":         strings.TrimSpace(pm.Description),
				"source_api_endpoint": currentStormsURL,
			}
			collectorutil.AddTropicalCycloneScores(props, true)
			out = append(out, events.Event{
				Ts:     ts,
				Source: "nhc_gis_cones",
				ExtID:  fmt.Sprintf("%s:%s:%d:%d", s.ID, s.BinNumber, i, j),
				Lat:    lat,
				Lon:    lon,
				Geom:   wkt,
				Props:  props,
			})
		}
	}
	return out
}

func kmlBytes(buf []byte, productURL string) ([]byte, error) {
	if !strings.HasSuffix(strings.ToLower(productURL), ".kmz") {
		return buf, nil
	}
	zr, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return nil, err
	}
	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".kml") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
	return nil, fmt.Errorf("kmz contains no kml")
}

func placemarksFromKML(buf []byte) []placemark {
	dec := xml.NewDecoder(bytes.NewReader(buf))
	out := []placemark{}
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "Placemark" {
			continue
		}
		var pm placemark
		if err := dec.DecodeElement(&pm, &start); err == nil {
			out = append(out, pm)
		}
	}
}

func polygonsFromInner(inner string) []kmlPolygon {
	dec := xml.NewDecoder(strings.NewReader("<root>" + inner + "</root>"))
	out := []kmlPolygon{}
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "Polygon" {
			continue
		}
		var poly kmlPolygon
		if err := dec.DecodeElement(&poly, &start); err == nil {
			out = append(out, poly)
		}
	}
}

func normalizeProductURL(s string) string {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return s
	}
	base, _ := url.Parse("https://www.nhc.noaa.gov/")
	ref, err := url.Parse(s)
	if err != nil {
		return "https://www.nhc.noaa.gov/" + strings.TrimLeft(s, "/")
	}
	if !strings.HasPrefix(s, "/") && !strings.Contains(path.Dir(s), ".") {
		ref.Path = "/" + strings.TrimLeft(ref.Path, "/")
	}
	return base.ResolveReference(ref).String()
}

func parseCoordinates(raw string) [][2]float64 {
	out := [][2]float64{}
	for _, token := range strings.Fields(strings.TrimSpace(raw)) {
		parts := strings.Split(token, ",")
		if len(parts) < 2 {
			continue
		}
		lon, err1 := strconv.ParseFloat(parts[0], 64)
		lat, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 == nil && err2 == nil {
			out = append(out, [2]float64{lon, lat})
		}
	}
	return out
}

func polygonWKT(ring [][2]float64) string {
	if len(ring) < 3 {
		return ""
	}
	pts := make([]string, 0, len(ring)+1)
	for _, p := range ring {
		pts = append(pts, fmt.Sprintf("%.6f %.6f", p[0], p[1]))
	}
	if ring[0] != ring[len(ring)-1] {
		pts = append(pts, fmt.Sprintf("%.6f %.6f", ring[0][0], ring[0][1]))
	}
	return "POLYGON((" + strings.Join(pts, ",") + "))"
}

func centroid(ring [][2]float64) (lon, lat float64) {
	if len(ring) == 0 {
		return 0, 0
	}
	for _, p := range ring {
		lon += p[0]
		lat += p[1]
	}
	n := float64(len(ring))
	return lon / n, lat / n
}

func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(s)); err == nil {
		return t.UTC()
	}
	return time.Time{}
}
