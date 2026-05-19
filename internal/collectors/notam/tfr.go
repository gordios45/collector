// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// FAA Temporary Flight Restrictions — keyless.
//
//	list:   GET https://tfr.faa.gov/tfrapi/exportTfrList  (JSON)
//	detail: GET https://tfr.faa.gov/download/detail_{id_w/underscore}.xml
//
// The list gives notam_id, type, facility, state, description, creation_date.
// The detail XML contains the polygon as a sequence of <Avx> vertices with
// <geoLat> / <geoLong> in DMS-decimal-suffix format (e.g. "31.34192515N",
// "081.84166667W"). We parse them into a PostGIS WKT polygon.
package notam

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	tfrListURL   = "https://tfr.faa.gov/tfrapi/exportTfrList"
	tfrDetailFmt = "https://tfr.faa.gov/download/detail_%s.xml"
)

type TFRCollector struct{}

func NewTFR() (*TFRCollector, error)             { return &TFRCollector{}, nil }
func (c *TFRCollector) ID() string               { return "notam_tfr" }
func (c *TFRCollector) PollEvery() time.Duration { return 30 * time.Minute }

type tfrListRow struct {
	NotamID      string `json:"notam_id"`
	Type         string `json:"type"`
	Facility     string `json:"facility"`
	State        string `json:"state"`
	Description  string `json:"description"`
	CreationDate string `json:"creation_date"`
}

func (c *TFRCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	// tfr.faa.gov is behind Akamai — needs browser-fingerprint TLS.
	hdrs := map[string]string{
		"Accept":     "application/json",
		"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
	}
	var list []tfrListRow
	if err := httpx.GetJSONBrowser(ctx, tfrListURL, hdrs, &list); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(list))
	for _, row := range list {
		fileID := strings.ReplaceAll(row.NotamID, "/", "_") // "6/4993" → "6_4993"
		detailURL := fmt.Sprintf(tfrDetailFmt, fileID)

		// Best-effort: if detail fetch or parse fails, still ingest the list row
		// as a point (state centroid) so the event is visible.
		buf, err := httpx.GetBytesBrowser(ctx, detailURL, hdrs)
		var wkt string
		var centroidLat, centroidLon float64
		if err == nil {
			wkt, centroidLat, centroidLon = parseTFRGeometry(buf)
		}
		if wkt == "" {
			// fallback — use US state centroid
			if lat, lon, ok := stateCentroid(row.State); ok {
				centroidLat, centroidLon = lat, lon
			}
		}

		props := map[string]any{
			"notam_id":      row.NotamID,
			"type":          row.Type,
			"facility":      row.Facility,
			"state":         row.State,
			"description":   row.Description,
			"creation_date": row.CreationDate,
			"detail_url":    detailURL,
		}
		ts := time.Now().UTC()
		if t, err := time.Parse("01/02/2006", row.CreationDate); err == nil {
			ts = t.UTC()
		}
		ev := events.Event{
			Ts: ts, Source: "notam_tfr", ExtID: row.NotamID, Props: props,
		}
		if wkt != "" {
			ev.Geom = wkt
			ev.Lat = centroidLat
			ev.Lon = centroidLon
		} else if centroidLat != 0 || centroidLon != 0 {
			ev.Lat = centroidLat
			ev.Lon = centroidLon
		} else {
			// No geom at all — skip (would fail the NOT NULL-ish semantics).
			continue
		}
		out = append(out, ev)
	}
	return out, nil
}

// parseTFRGeometry walks the detail XML and builds a POLYGON WKT from the
// first <Avx>…</Avx> ring it finds. Returns WKT + centroid.
func parseTFRGeometry(buf []byte) (wkt string, lat, lon float64) {
	type avx struct {
		GeoLat  string `xml:"geoLat"`
		GeoLong string `xml:"geoLong"`
	}
	dec := xml.NewDecoder(strings.NewReader(string(buf)))
	var vertices [][2]float64
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "Avx" {
			var a avx
			if err := dec.DecodeElement(&a, &se); err == nil {
				if la, ok := parseDMS(a.GeoLat); ok {
					if lo, ok := parseDMS(a.GeoLong); ok {
						vertices = append(vertices, [2]float64{lo, la})
					}
				}
			}
		}
	}
	if len(vertices) < 3 {
		return "", 0, 0
	}
	// Close ring if not already closed.
	if vertices[0] != vertices[len(vertices)-1] {
		vertices = append(vertices, vertices[0])
	}
	// Centroid = mean of unique vertices.
	var sx, sy float64
	n := len(vertices) - 1
	for i := 0; i < n; i++ {
		sx += vertices[i][0]
		sy += vertices[i][1]
	}
	lon = sx / float64(n)
	lat = sy / float64(n)

	parts := make([]string, 0, len(vertices))
	for _, v := range vertices {
		parts = append(parts, fmt.Sprintf("%f %f", v[0], v[1]))
	}
	wkt = "POLYGON((" + strings.Join(parts, ",") + "))"
	return wkt, lat, lon
}

// parseDMS parses FAA's "decimal + hemisphere letter" format.
// Examples: "31.34192515N" → 31.34192515 ; "081.84166667W" → -81.84166667.
func parseDMS(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	last := s[len(s)-1]
	body := strings.TrimSpace(s[:len(s)-1])
	f, err := strconv.ParseFloat(body, 64)
	if err != nil {
		return 0, false
	}
	switch last {
	case 'N', 'E':
		return f, true
	case 'S', 'W':
		return -f, true
	}
	// No hemisphere — assume already signed.
	f2, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f2, true
}

// stateCentroid — crude US state-centroid fallback for TFRs missing geometry.
var stateCentroids = map[string][2]float64{
	"AL": {32.7, -86.6}, "AK": {63.6, -154.3}, "AZ": {34.5, -111.6}, "AR": {34.7, -92.4},
	"CA": {36.7, -119.4}, "CO": {39.0, -105.5}, "CT": {41.6, -72.7}, "DE": {38.9, -75.5},
	"FL": {27.8, -81.7}, "GA": {32.2, -83.6}, "HI": {20.8, -156.3}, "ID": {44.2, -114.5},
	"IL": {40.0, -89.1}, "IN": {39.9, -86.3}, "IA": {42.1, -93.5}, "KS": {38.5, -98.4},
	"KY": {37.6, -85.3}, "LA": {31.1, -92.0}, "ME": {44.7, -69.4}, "MD": {39.0, -76.8},
	"MA": {42.2, -71.5}, "MI": {44.3, -85.4}, "MN": {46.3, -94.3}, "MS": {32.7, -89.7},
	"MO": {38.4, -92.5}, "MT": {46.9, -110.5}, "NE": {41.1, -98.3}, "NV": {38.5, -117.1},
	"NH": {43.5, -71.6}, "NJ": {40.3, -74.5}, "NM": {34.4, -106.1}, "NY": {42.9, -75.5},
	"NC": {35.6, -79.8}, "ND": {47.5, -99.8}, "OH": {40.4, -82.8}, "OK": {35.6, -97.5},
	"OR": {44.0, -120.6}, "PA": {40.6, -77.2}, "RI": {41.7, -71.5}, "SC": {33.9, -80.9},
	"SD": {44.3, -99.4}, "TN": {35.7, -86.7}, "TX": {31.1, -97.6}, "UT": {40.1, -111.9},
	"VT": {44.1, -72.7}, "VA": {37.8, -78.2}, "WA": {47.4, -121.5}, "WV": {38.5, -80.9},
	"WI": {44.3, -89.6}, "WY": {43.0, -107.3}, "DC": {38.9, -77.0}, "PR": {18.2, -66.4},
}

func stateCentroid(s string) (lat, lon float64, ok bool) {
	c, present := stateCentroids[strings.ToUpper(strings.TrimSpace(s))]
	if !present {
		return 0, 0, false
	}
	return c[0], c[1], true
}
