// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA HMS smoke polygons.
package hms_smoke

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	baseDir = "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/KML"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "hms_smoke" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		day := now.AddDate(0, 0, -i)
		url := kmlURL(day)
		buf, err := httpx.GetBytes(ctx, url, nil)
		if err != nil {
			if i < 2 {
				continue
			}
			return nil, err
		}
		rows, err := eventsFromKML(buf, day, url)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 || i == 2 {
			return rows, nil
		}
	}
	return nil, nil
}

func kmlURL(day time.Time) string {
	return fmt.Sprintf("%s/%04d/%02d/hms_smoke%04d%02d%02d.kml",
		baseDir, day.Year(), int(day.Month()), day.Year(), int(day.Month()), day.Day())
}

type kmlDoc struct {
	Document struct {
		Name string `xml:"name"`
	} `xml:"Document"`
}

type placemark struct {
	Name        string    `xml:"name"`
	Description string    `xml:"description"`
	StyleURL    string    `xml:"styleUrl"`
	Polygons    []polygon `xml:"Polygon"`
}

type polygon struct {
	Outer string `xml:"outerBoundaryIs>LinearRing>coordinates"`
}

func eventsFromKML(buf []byte, day time.Time, sourceURL string) ([]events.Event, error) {
	var doc kmlDoc
	if err := xml.Unmarshal(buf, &doc); err != nil {
		return nil, err
	}
	placemarks, err := placemarksFromKML(buf)
	if err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(placemarks))
	seen := map[string]int{}
	for i, pm := range placemarks {
		props := parseDescription(pm.Description)
		density := firstNonEmpty(props["Density"], densityFromStyle(pm.StyleURL), pm.Name)
		start := parseHMSJulianTime(props["Start Time"])
		end := parseHMSJulianTime(props["End Time"])
		ts := end
		if ts.IsZero() {
			ts = start
		}
		if ts.IsZero() {
			ts = day.Add(23*time.Hour + 59*time.Minute)
		}
		for j, poly := range pm.Polygons {
			ring := parseCoordinates(poly.Outer)
			geom := polygonWKT(ring)
			if geom == "" {
				continue
			}
			lon, lat := centroid(ring)
			key := shortKey(geom)
			seen[key]++
			p := map[string]any{
				"density":             density,
				"start_time":          props["Start Time"],
				"end_time":            props["End Time"],
				"satellite":           props["Satellite"],
				"analysis_date":       day.Format("2006-01-02"),
				"document_name":       doc.Document.Name,
				"style_url":           pm.StyleURL,
				"source_api_endpoint": sourceURL,
			}
			out = append(out, events.Event{
				Ts:     ts,
				Source: "hms_smoke",
				ExtID:  fmt.Sprintf("%s:%s:%d:%d:%d", day.Format("20060102"), strings.ToLower(density), i, j, seen[key]),
				Lat:    lat,
				Lon:    lon,
				Geom:   geom,
				Props:  p,
			})
		}
	}
	return out, nil
}

func placemarksFromKML(buf []byte) ([]placemark, error) {
	dec := xml.NewDecoder(strings.NewReader(string(buf)))
	out := []placemark{}
	for {
		tok, err := dec.Token()
		if err != nil {
			if err.Error() == "EOF" {
				return out, nil
			}
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "Placemark" {
			continue
		}
		var pm placemark
		if err := dec.DecodeElement(&pm, &start); err != nil {
			return nil, err
		}
		out = append(out, pm)
	}
}

var tagRe = regexp.MustCompile(`<[^>]+>`)

func parseDescription(raw string) map[string]string {
	text := html.UnescapeString(raw)
	text = strings.ReplaceAll(text, "<br>", "\n")
	text = strings.ReplaceAll(text, "<BR>", "\n")
	text = tagRe.ReplaceAllString(text, "")
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}

func parseHMSJulianTime(raw string) time.Time {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) < 2 {
		return time.Time{}
	}
	yearDay := parts[0]
	hhmm := strings.TrimSuffix(strings.ToUpper(parts[1]), "UTC")
	if len(yearDay) < 5 || len(hhmm) < 4 {
		return time.Time{}
	}
	year, err1 := strconv.Atoi(yearDay[:4])
	doy, err2 := strconv.Atoi(yearDay[4:])
	hour, err3 := strconv.Atoi(hhmm[:2])
	min, err4 := strconv.Atoi(hhmm[2:4])
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || doy < 1 || hour > 23 || min > 59 {
		return time.Time{}
	}
	return time.Date(year, 1, 1, hour, min, 0, 0, time.UTC).AddDate(0, 0, doy-1)
}

func densityFromStyle(style string) string {
	s := strings.ToLower(style)
	switch {
	case strings.Contains(s, "heavy"):
		return "Heavy"
	case strings.Contains(s, "medium"):
		return "Medium"
	case strings.Contains(s, "light"):
		return "Light"
	}
	return ""
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

func shortKey(s string) string {
	if len(s) <= 24 {
		return s
	}
	return s[:12] + s[len(s)-12:]
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
