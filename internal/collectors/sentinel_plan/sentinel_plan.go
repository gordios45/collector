// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package sentinel_plan ingests ESA Sentinel-1 planned acquisition segments
// and projects them onto active Gordios AOIs. These rows are collection
// opportunity/coverage context, not proof of tasking intent.
package sentinel_plan

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/html/charset"
)

const (
	sourceName = "sentinel_acquisition_plan"
	pageURL    = "https://sentinels.copernicus.eu/copernicus/sentinel-1/acquisition-plans"
	baseURL    = "https://sentinels.copernicus.eu"
)

type Collector struct {
	pool       *pgxpool.Pool
	maxAOIs    int
	maxFiles   int
	window     time.Duration
	bboxPadDeg float64
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_SENTINEL_ACQUISITION_PLAN") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_SENTINEL_ACQUISITION_PLAN=1")
	}
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:       pool,
		maxAOIs:    collectorutil.EnvInt("SENTINEL_PLAN_MAX_AOIS", 16, 1, 60),
		maxFiles:   collectorutil.EnvInt("SENTINEL_PLAN_MAX_FILES", 3, 1, 6),
		window:     time.Duration(collectorutil.EnvInt("SENTINEL_PLAN_WINDOW_HOURS", 72, 6, 168)) * time.Hour,
		bboxPadDeg: float64(collectorutil.EnvInt("SENTINEL_PLAN_BBOX_PAD_TENTHS", 5, 0, 30)) / 10,
	}, nil
}

func (c *Collector) ID() string               { return sourceName }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	aois := collectorutil.SelectAOIsForCollector(ctx, c.pool, c.ID(), c.maxAOIs, 7*24*time.Hour, collectorutil.StrategicAOIs)
	if len(aois) == 0 {
		return nil, nil
	}
	urls, err := latestKMLURLs(ctx, c.maxFiles)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	start := now.Add(-1 * time.Hour)
	end := now.Add(c.window)
	out := []events.Event{}
	var lastErr error
	for _, planURL := range urls {
		rows, err := fetchSegments(ctx, planURL, start, end)
		if err != nil {
			lastErr = err
			continue
		}
		for _, seg := range rows {
			for _, aoi := range aois {
				if !seg.BBox.Contains(aoi.Lat, aoi.Lon, c.bboxPadDeg) {
					continue
				}
				out = append(out, eventForAOI(aoi, seg, planURL))
			}
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

var hrefRE = regexp.MustCompile(`href="(/documents/d/sentinel/(s1[acd]_mp_user_[^"]+))"`)

func latestKMLURLs(ctx context.Context, maxFiles int) ([]string, error) {
	body, err := httpx.GetBytes(ctx, pageURL, map[string]string{"Accept": "text/html"})
	if err != nil {
		return nil, err
	}
	matches := hrefRE.FindAllStringSubmatch(string(body), -1)
	seenSat := map[string]struct{}{}
	out := []string{}
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		sat := strings.ToUpper(m[2][:3])
		if _, ok := seenSat[sat]; ok {
			continue
		}
		seenSat[sat] = struct{}{}
		out = append(out, baseURL+m[1])
		if len(out) >= maxFiles {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no Sentinel-1 KML links found on acquisition plan page")
	}
	return out, nil
}

type kml struct {
	Document kmlDocument `xml:"Document"`
}

type kmlDocument struct {
	Name    string      `xml:"name"`
	Folders []kmlFolder `xml:"Folder"`
}

type kmlFolder struct {
	Name       string      `xml:"name"`
	Folders    []kmlFolder `xml:"Folder"`
	Placemarks []placemark `xml:"Placemark"`
}

type placemark struct {
	Name         string       `xml:"name"`
	TimeSpan     timeSpan     `xml:"TimeSpan"`
	StyleURL     string       `xml:"styleUrl"`
	ExtendedData extendedData `xml:"ExtendedData"`
	LinearRing   linearRing   `xml:"LinearRing"`
}

type timeSpan struct {
	Begin string `xml:"begin"`
	End   string `xml:"end"`
}

type extendedData struct {
	Data []dataField `xml:"Data"`
}

type dataField struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value"`
}

type linearRing struct {
	Coordinates string `xml:"coordinates"`
}

type segment struct {
	Name         string
	SatelliteID  string
	DatatakeID   string
	Mode         string
	Swath        string
	Polarisation string
	Start        time.Time
	End          time.Time
	DurationS    int
	OrbitAbs     string
	OrbitRel     string
	BBox         bbox
}

type bbox struct {
	MinLat float64
	MaxLat float64
	MinLon float64
	MaxLon float64
}

func (b bbox) Contains(lat, lon, pad float64) bool {
	return lat >= b.MinLat-pad && lat <= b.MaxLat+pad && lon >= b.MinLon-pad && lon <= b.MaxLon+pad
}

func fetchSegments(ctx context.Context, planURL string, start, end time.Time) ([]segment, error) {
	raw, err := httpx.GetBytes(ctx, planURL, map[string]string{"Accept": "application/vnd.google-earth.kml+xml,application/xml"})
	if err != nil {
		return nil, err
	}
	var doc kml
	dec := xml.NewDecoder(bytes.NewReader(raw))
	dec.CharsetReader = charset.NewReaderLabel
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	out := []segment{}
	var walk func(kmlFolder)
	walk = func(f kmlFolder) {
		for _, p := range f.Placemarks {
			seg, ok := segmentFromPlacemark(p)
			if !ok || seg.End.Before(start) || seg.Start.After(end) {
				continue
			}
			out = append(out, seg)
		}
		for _, child := range f.Folders {
			walk(child)
		}
	}
	for _, f := range doc.Document.Folders {
		walk(f)
	}
	return out, nil
}

func segmentFromPlacemark(p placemark) (segment, bool) {
	fields := map[string]string{}
	for _, d := range p.ExtendedData.Data {
		fields[strings.TrimSpace(d.Name)] = strings.TrimSpace(d.Value)
	}
	start := parsePlanTime(firstNonEmpty(fields["ObservationTimeStart"], p.TimeSpan.Begin, p.Name))
	stop := parsePlanTime(firstNonEmpty(fields["ObservationTimeStop"], p.TimeSpan.End))
	if start.IsZero() {
		return segment{}, false
	}
	if stop.IsZero() {
		stop = start
	}
	b, ok := bboxFromCoordinates(p.LinearRing.Coordinates)
	if !ok {
		return segment{}, false
	}
	dur, _ := strconv.Atoi(fields["ObservationDuration"])
	return segment{
		Name:         p.Name,
		SatelliteID:  firstNonEmpty(fields["SatelliteId"], satelliteFromStyle(p.StyleURL)),
		DatatakeID:   fields["DatatakeId"],
		Mode:         fields["Mode"],
		Swath:        fields["Swath"],
		Polarisation: fields["Polarisation"],
		Start:        start,
		End:          stop,
		DurationS:    dur,
		OrbitAbs:     fields["OrbitAbsolute"],
		OrbitRel:     fields["OrbitRelative"],
		BBox:         b,
	}, true
}

func bboxFromCoordinates(raw string) (bbox, bool) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return bbox{}, false
	}
	b := bbox{MinLat: 90, MaxLat: -90, MinLon: 180, MaxLon: -180}
	for _, field := range fields {
		parts := strings.Split(field, ",")
		if len(parts) < 2 {
			continue
		}
		lon, err1 := strconv.ParseFloat(parts[0], 64)
		lat, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil || math.IsNaN(lat) || math.IsNaN(lon) {
			continue
		}
		b.MinLat = math.Min(b.MinLat, lat)
		b.MaxLat = math.Max(b.MaxLat, lat)
		b.MinLon = math.Min(b.MinLon, lon)
		b.MaxLon = math.Max(b.MaxLon, lon)
	}
	return b, b.MinLat <= b.MaxLat && b.MinLon <= b.MaxLon
}

func eventForAOI(aoi collectorutil.AOI, seg segment, planURL string) events.Event {
	score := plannedScore(seg)
	props := map[string]any{
		"watch_aoi_id":                 aoi.ID,
		"watch_aoi_kind":               aoi.Kind,
		"watch_aoi_label":              aoi.Label,
		"satellite_id":                 seg.SatelliteID,
		"datatake_id":                  seg.DatatakeID,
		"mode":                         seg.Mode,
		"swath":                        seg.Swath,
		"polarisation":                 seg.Polarisation,
		"planned_start":                seg.Start.Format(time.RFC3339),
		"planned_end":                  seg.End.Format(time.RFC3339),
		"observation_duration_s":       seg.DurationS,
		"orbit_absolute":               seg.OrbitAbs,
		"orbit_relative":               seg.OrbitRel,
		"planned_acquisition_over_aoi": true,
		"planned_acquisition_score":    collectorutil.Round(score, 2),
		"tasking_intent_caution":       "planned acquisition confirms SAR coverage opportunity, not tasking intent",
		"source_api_endpoint":          pageURL,
		"plan_url":                     planURL,
	}
	return events.Event{
		Ts:     seg.Start,
		Source: sourceName,
		ExtID:  fmt.Sprintf("%s:%s:%s", collectorutil.StableID(aoi.ID), seg.SatelliteID, firstNonEmpty(seg.DatatakeID, url.QueryEscape(seg.Start.Format(time.RFC3339)))),
		Lat:    aoi.Lat,
		Lon:    aoi.Lon,
		Props:  props,
	}
}

func plannedScore(seg segment) float64 {
	score := 0.8
	switch strings.ToUpper(seg.Mode) {
	case "IW", "EW":
		score += 0.4
	case "SM":
		score += 0.2
	}
	if seg.DurationS >= 120 {
		score += 0.2
	}
	return math.Min(score, 2.0)
}

func parsePlanTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func satelliteFromStyle(style string) string {
	style = strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(style, "#")))
	if strings.HasPrefix(style, "S1") {
		return style
	}
	return ""
}
