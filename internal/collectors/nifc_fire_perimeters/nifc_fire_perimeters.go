// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NIFC WFIGS current wildland-fire perimeters.
package nifc_fire_perimeters

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const baseURL = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_Current/FeatureServer/0/query"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nifc_fire_perimeters" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type response struct {
	Features []feature `json:"features"`
	Error    *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type feature struct {
	Attributes map[string]any `json:"attributes"`
	Geometry   struct {
		Rings [][][]float64 `json:"rings"`
	} `json:"geometry"`
}

func queryURL() string {
	v := url.Values{}
	v.Set("where", "poly_IsVisible='Yes'")
	v.Set("outFields", strings.Join([]string{
		"OBJECTID", "GlobalID", "poly_IncidentName", "poly_FeatureCategory",
		"poly_MapMethod", "poly_GISAcres", "poly_FeatureStatus", "poly_IsVisible",
		"poly_CreateDate", "poly_DateCurrent", "poly_PolygonDateTime",
		"poly_IRWINID", "poly_Acres_AutoCalc", "poly_Source",
		"attr_IncidentName", "attr_IncidentTypeCategory", "attr_IncidentTypeKind",
		"attr_IncidentSize", "attr_PercentContained", "attr_FireDiscoveryDateTime",
		"attr_FireOutDateTime", "attr_FireCause", "attr_FireCauseGeneral",
		"attr_FireBehaviorGeneral", "attr_POOState", "attr_POOCounty",
		"attr_POOJurisdictionalAgency", "attr_TotalIncidentPersonnel",
		"attr_ModifiedOnDateTime_dt", "attr_UniqueFireIdentifier",
	}, ","))
	v.Set("outSR", "4326")
	v.Set("f", "json")
	v.Set("resultRecordCount", "2000")
	return baseURL + "?" + v.Encode()
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var resp response
	if err := httpx.GetJSON(ctx, queryURL(), map[string]string{"Accept": "application/json"}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("arcgis error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	return eventsFromFeatures(resp.Features), nil
}

func eventsFromFeatures(features []feature) []events.Event {
	out := make([]events.Event, 0, len(features))
	for _, f := range features {
		wkt, ok := ringsToWKT(f.Geometry.Rings)
		if !ok {
			continue
		}
		lon, lat, ok := centroid(f.Geometry.Rings)
		if !ok {
			continue
		}
		ext := stringAttr(f.Attributes, "GlobalID")
		if ext == "" {
			ext = stringAttr(f.Attributes, "poly_IRWINID")
		}
		if ext == "" {
			ext = fmt.Sprintf("%v", f.Attributes["OBJECTID"])
		}
		ts := firstMillis(f.Attributes, "poly_DateCurrent", "poly_PolygonDateTime", "attr_ModifiedOnDateTime_dt", "attr_FireDiscoveryDateTime")
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		props := map[string]any{
			"source_api_endpoint": baseURL,
		}
		for k, v := range f.Attributes {
			props[k] = v
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "nifc_fire_perimeters",
			ExtID:  ext,
			Lat:    lat,
			Lon:    lon,
			Geom:   wkt,
			Props:  props,
		})
	}
	return out
}

func ringsToWKT(rings [][][]float64) (string, bool) {
	polys := make([]string, 0, len(rings))
	for _, ring := range rings {
		if len(ring) < 3 {
			continue
		}
		points := make([]string, 0, len(ring)+1)
		for _, pt := range ring {
			if len(pt) < 2 || !validCoord(pt[0], pt[1]) {
				continue
			}
			points = append(points, fmt.Sprintf("%s %s", trimFloat(pt[0]), trimFloat(pt[1])))
		}
		if len(points) < 3 {
			continue
		}
		first := points[0]
		if points[len(points)-1] != first {
			points = append(points, first)
		}
		polys = append(polys, strings.Join(points, ","))
	}
	if len(polys) == 0 {
		return "", false
	}
	if len(polys) == 1 {
		return "POLYGON((" + polys[0] + "))", true
	}
	parts := make([]string, 0, len(polys))
	for _, poly := range polys {
		parts = append(parts, "(("+poly+"))")
	}
	return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")", true
}

func centroid(rings [][][]float64) (float64, float64, bool) {
	if len(rings) == 0 || len(rings[0]) == 0 {
		return 0, 0, false
	}
	var sx, sy float64
	var n int
	for _, pt := range rings[0] {
		if len(pt) >= 2 && validCoord(pt[0], pt[1]) {
			sx += pt[0]
			sy += pt[1]
			n++
		}
	}
	if n == 0 {
		return 0, 0, false
	}
	return sx / float64(n), sy / float64(n), true
}

func validCoord(lon, lat float64) bool {
	return !math.IsNaN(lon) && !math.IsNaN(lat) && lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90
}

func firstMillis(attrs map[string]any, keys ...string) time.Time {
	for _, key := range keys {
		if t := millisTime(attrs[key]); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func millisTime(v any) time.Time {
	switch x := v.(type) {
	case float64:
		if x > 0 {
			return time.UnixMilli(int64(x)).UTC()
		}
	case int64:
		if x > 0 {
			return time.UnixMilli(x).UTC()
		}
	}
	return time.Time{}
}

func stringAttr(attrs map[string]any, key string) string {
	if s, ok := attrs[key].(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func trimFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
