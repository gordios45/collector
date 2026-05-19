// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package public_facilities ingests no-key static facility layers used as
// proximity context for signal interpretation.
package public_facilities

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
)

const sourceID = "public_facilities_context"

type Collector struct {
	pool *pgxpool.Pool
}

type feedSpec struct {
	Source    string
	Provider  string
	Endpoint  string
	PublicURL string
	Kind      string
	Format    string
	Paged     bool
}

var feeds = []feedSpec{
	{
		Source: "hospitals_us", Provider: "HIFLD / ArcGIS",
		Endpoint:  "https://services2.arcgis.com/RQcpPaCpMAXzUI5g/arcgis/rest/services/US_Hospitals/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://www.arcgis.com/home/item.html?id=283286c21e8447dbb3a58b1b6952a748", Kind: "hospital", Format: "arcgis", Paged: true,
	},
	{
		Source: "capitol_buildings_us", Provider: "USGS / ArcGIS",
		Endpoint:  "https://services8.arcgis.com/KTc92aOGhsLVR3RT/arcgis/rest/services/State_Capitol/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://services8.arcgis.com/KTc92aOGhsLVR3RT/arcgis/rest/services/State_Capitol/FeatureServer/0", Kind: "capitol_building", Format: "arcgis", Paged: true,
	},
	{
		Source: "rail_stations_us", Provider: "U.S. DOT / BTS",
		Endpoint:  "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/NTAD_Amtrak_Stations/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://www.arcgis.com/home/item.html?id=1ed62a9f46304679aaa396bed4c8565a", Kind: "rail_station", Format: "arcgis",
	},
	{
		Source: "rail_stations_eu", Provider: "Trainline EU / GitHub",
		Endpoint:  "https://raw.githubusercontent.com/trainline-eu/stations/master/stations.csv",
		PublicURL: "https://github.com/trainline-eu/stations", Kind: "rail_station", Format: "trainline_csv",
	},
	{
		Source: "sports_venues_us", Provider: "ArcGIS Open Data",
		Endpoint:  "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Major_Sport_Venues/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Major_Sport_Venues/FeatureServer/0", Kind: "sports_venue", Format: "arcgis",
	},
	{
		Source: "cruise_terminals_us", Provider: "ArcGIS Open Data",
		Endpoint:  "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Cruise_Line_Terminals/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Cruise_Line_Terminals/FeatureServer/0", Kind: "cruise_terminal", Format: "arcgis",
	},
	{
		Source: "oil_refineries_us", Provider: "EIA / ArcGIS",
		Endpoint:  "https://services8.arcgis.com/XLNdV9JX2tH1BT1x/ArcGIS/rest/services/Petroleum_Refineries_US_EIA/FeatureServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json",
		PublicURL: "https://services8.arcgis.com/XLNdV9JX2tH1BT1x/ArcGIS/rest/services/Petroleum_Refineries_US_EIA/FeatureServer/6", Kind: "oil_refinery", Format: "arcgis", Paged: true,
	},
}

var arcGISClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		ForceAttemptHTTP2: false,
	},
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{pool: pool}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	for _, spec := range feeds {
		feats, err := c.fetchFeatures(ctx, spec)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", spec.Source, err)
		}
		if _, err := features.Upsert(ctx, c.pool, spec.Source, feats); err != nil {
			return nil, fmt.Errorf("upsert %s: %w", spec.Source, err)
		}
	}
	return nil, nil
}

func (c *Collector) fetchFeatures(ctx context.Context, spec feedSpec) ([]features.Feature, error) {
	switch spec.Format {
	case "geojson":
		return fetchGeoJSON(ctx, spec)
	case "arcgis":
		return fetchArcGIS(ctx, spec)
	case "opendatasoft":
		return fetchOpenDataSoft(ctx, spec)
	case "trainline_csv":
		return fetchTrainlineCSV(ctx, spec)
	default:
		return nil, fmt.Errorf("unknown facility format %q", spec.Format)
	}
}

func fetchGeoJSON(ctx context.Context, spec feedSpec) ([]features.Feature, error) {
	var raw struct {
		Features []struct {
			ID         any            `json:"id"`
			Properties map[string]any `json:"properties"`
			Geometry   struct {
				Type        string          `json:"type"`
				Coordinates json.RawMessage `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := httpx.GetJSON(ctx, spec.Endpoint, map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
		return nil, err
	}
	out := make([]features.Feature, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, ok := pointFromGeoJSON(f.Geometry.Type, f.Geometry.Coordinates)
		if !ok {
			continue
		}
		props := facilityProps(spec, f.Properties)
		id := firstNonEmpty(textAny(f.ID), textAny(f.Properties["ID"]), textAny(f.Properties["PERMANENT_IDENTIFIER"]), textAny(f.Properties["OBJECTID"]), nameFor(spec.Kind, f.Properties))
		out = append(out, feature(id, lat, lon, props))
	}
	return out, nil
}

func fetchTrainlineCSV(ctx context.Context, spec feedSpec) ([]features.Feature, error) {
	buf, err := httpx.GetBytes(ctx, spec.Endpoint, map[string]string{"Accept": "text/csv"})
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(buf))
	r.Comma = ';'
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int, len(header))
	for i, h := range header {
		index[strings.TrimSpace(h)] = i
	}
	value := func(row []string, key string) string {
		i, ok := index[key]
		if !ok || i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	out := make([]features.Feature, 0, 16000)
	for {
		row, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if strings.EqualFold(value(row, "is_city"), "t") {
			continue
		}
		if !strings.EqualFold(value(row, "is_main_station"), "t") {
			continue
		}
		lat, latOK := floatAny(value(row, "latitude"))
		lon, lonOK := floatAny(value(row, "longitude"))
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		raw := map[string]any{
			"id":              value(row, "id"),
			"name":            value(row, "name"),
			"uic":             value(row, "uic"),
			"country":         value(row, "country"),
			"time_zone":       value(row, "time_zone"),
			"is_main_station": value(row, "is_main_station"),
			"is_airport":      value(row, "is_airport"),
			"is_suggestable":  value(row, "is_suggestable"),
		}
		props := facilityProps(spec, raw)
		id := firstNonEmpty(value(row, "uic"), value(row, "id"), nameFor(spec.Kind, raw))
		out = append(out, feature(id, lat, lon, props))
	}
	return out, nil
}

func fetchArcGIS(ctx context.Context, spec feedSpec) ([]features.Feature, error) {
	const pageSize = 250

	out := make([]features.Feature, 0, 256)
	if !spec.Paged {
		raw, err := getArcGIS(ctx, spec.Endpoint)
		if err != nil {
			return nil, err
		}
		out = featuresFromArcGIS(spec, raw.Features)
		if !raw.ExceededTransferLimit {
			return out, nil
		}
	}

	out = make([]features.Feature, 0, 256)
	for offset := 0; ; offset += pageSize {
		raw, err := getArcGIS(ctx, arcGISPageURL(spec.Endpoint, offset, pageSize))
		if err != nil {
			return nil, err
		}
		out = append(out, featuresFromArcGIS(spec, raw.Features)...)
		if len(raw.Features) == 0 || !raw.ExceededTransferLimit || len(raw.Features) < pageSize {
			break
		}
	}
	return out, nil
}

type arcGISResponse struct {
	ExceededTransferLimit bool `json:"exceededTransferLimit"`
	Features              []arcGISFeature
	Error                 struct {
		Message string `json:"message"`
	} `json:"error"`
}

type arcGISFeature struct {
	Attributes map[string]any `json:"attributes"`
	Geometry   map[string]any `json:"geometry"`
}

func getArcGIS(ctx context.Context, endpoint string) (arcGISResponse, error) {
	var raw arcGISResponse
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		raw = arcGISResponse{}
		err = httpx.GetJSONWithClient(ctx, arcGISClient, endpoint, map[string]string{"Accept": "application/json"}, &raw)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return raw, ctx.Err()
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	if err != nil {
		return raw, err
	}
	if raw.Error.Message != "" {
		return raw, fmt.Errorf("%s", raw.Error.Message)
	}
	return raw, nil
}

func featuresFromArcGIS(spec feedSpec, rows []arcGISFeature) []features.Feature {
	out := make([]features.Feature, 0, len(rows))
	for _, f := range rows {
		lat, lon, ok := pointFromArcGIS(f.Geometry, f.Attributes)
		if !ok {
			continue
		}
		props := facilityProps(spec, f.Attributes)
		id := firstNonEmpty(textAny(f.Attributes["OBJECTID"]), textAny(f.Attributes["OBJECTID_1"]), textAny(f.Attributes["FID"]), textAny(f.Attributes["VENUEID"]), textAny(f.Attributes["STNCODE"]), textAny(f.Attributes["Code"]), nameFor(spec.Kind, f.Attributes))
		out = append(out, feature(id, lat, lon, props))
	}
	return out
}

func arcGISPageURL(endpoint string, offset, limit int) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	q := u.Query()
	if q.Get("where") == "" {
		q.Set("where", "1=1")
	}
	if q.Get("outFields") == "" {
		q.Set("outFields", "*")
	}
	if q.Get("outSR") == "" {
		q.Set("outSR", "4326")
	}
	if q.Get("f") == "" {
		q.Set("f", "json")
	}
	q.Set("resultRecordCount", strconv.Itoa(limit))
	q.Set("resultOffset", strconv.Itoa(offset))
	u.RawQuery = strings.ReplaceAll(q.Encode(), "%2C", ",")
	return u.String()
}

func fetchOpenDataSoft(ctx context.Context, spec feedSpec) ([]features.Feature, error) {
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, spec.Endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil, err
	}
	out := make([]features.Feature, 0, len(rows))
	for _, row := range rows {
		fields, _ := row["fields"].(map[string]any)
		if fields == nil {
			fields = row
		}
		lat, lon, ok := pointFromOpenDataSoft(row, fields)
		if !ok {
			continue
		}
		props := facilityProps(spec, fields)
		props["recordid"] = textAny(row["recordid"])
		id := firstNonEmpty(textAny(row["recordid"]), textAny(fields["uic"]), nameFor(spec.Kind, fields))
		out = append(out, feature(id, lat, lon, props))
	}
	return out, nil
}

func facilityProps(spec feedSpec, raw map[string]any) map[string]any {
	props := map[string]any{
		"source_provider":      spec.Provider,
		"source_api_endpoint":  spec.Endpoint,
		"source_public_url":    spec.PublicURL,
		"source_provider_kind": "public_static_facility_inventory",
		"facility_kind":        spec.Kind,
		"name":                 nameFor(spec.Kind, raw),
		"city":                 firstNonEmpty(textAny(raw["CITY"]), textAny(raw["CITY2"]), textAny(raw["City"]), textAny(raw["city"]), textAny(raw["addressLocality"])),
		"state":                firstNonEmpty(textAny(raw["STATE"]), textAny(raw["State"]), textAny(raw["state"]), textAny(raw["addressRegion"])),
		"country":              firstNonEmpty(textAny(raw["COUNTRY"]), textAny(raw["Country"]), textAny(raw["country"]), textAny(raw["nln1"])),
		"labels":               labelsFor(spec.Kind, raw),
	}
	for _, key := range []string{
		"ADDRESS", "ZIP", "ZIPCODE", "WEBSITE", "TELEPHONE", "BEDS", "TRAUMA", "HELIPAD", "TYPE",
		"STNCODE", "STNNAME", "StationNam", "Code", "Address1", "ZipCode", "NAICS_DESC", "PORTNAME", "URL", "OWNER", "OPERNAME", "time_zone",
		"companies", "caracteristics", "uic", "Company", "Corp", "Site", "PADD", "AD_Mbpd", "Source", "Period_",
	} {
		if v, ok := raw[key]; ok && strings.TrimSpace(textAny(v)) != "" {
			props[strings.ToLower(key)] = v
		}
	}
	return props
}

func labelsFor(kind string, raw map[string]any) []string {
	switch kind {
	case "hospital":
		return []string{"Hospital"}
	case "capitol_building":
		return []string{"Capitol Building"}
	case "rail_station":
		labels := []string{"Rail Station"}
		if companies := strings.TrimSpace(textAny(raw["companies"])); companies != "" {
			for _, company := range strings.Split(companies, ";") {
				if strings.TrimSpace(company) != "" {
					labels = append(labels, strings.TrimSpace(company))
				}
			}
		} else if code := strings.TrimSpace(firstNonEmpty(textAny(raw["STNCODE"]), textAny(raw["Code"]))); code != "" {
			labels = append(labels, "Amtrak")
		}
		return labels
	case "sports_venue":
		return []string{"Sports Venue", firstNonEmpty(textAny(raw["NAICS_DESC"]), "Sports")}
	case "cruise_terminal":
		return []string{"Cruise Terminal"}
	case "oil_refinery":
		return []string{"Oil Refinery", firstNonEmpty(textAny(raw["Company"]), textAny(raw["Corp"]), "Petroleum")}
	default:
		return []string{kind}
	}
}

func nameFor(kind string, raw map[string]any) string {
	switch kind {
	case "rail_station":
		return firstNonEmpty(textAny(raw["STNNAME"]), textAny(raw["StationNam"]), textAny(raw["Name"]), textAny(raw["nama1"]), textAny(raw["name"]))
	case "oil_refinery":
		return strings.TrimSpace(strings.Join(nonEmpty(textAny(raw["Company"]), textAny(raw["Site"])), " - "))
	default:
		return firstNonEmpty(textAny(raw["NAME"]), textAny(raw["Name"]), textAny(raw["name"]), textAny(raw["TITLE"]), textAny(raw["title"]))
	}
}

func feature(id string, lat, lon float64, props map[string]any) features.Feature {
	if id == "" {
		id = stableID(fmt.Sprint(props))
	}
	return features.Feature{
		ExtID:   stableID(id),
		GeomWKT: fmt.Sprintf("POINT(%f %f)", lon, lat),
		Props:   props,
	}
}

func pointFromGeoJSON(typ string, raw json.RawMessage) (float64, float64, bool) {
	if !strings.EqualFold(typ, "Point") {
		return 0, 0, false
	}
	var c []float64
	if err := json.Unmarshal(raw, &c); err != nil || len(c) < 2 {
		return 0, 0, false
	}
	return c[1], c[0], validLatLon(c[1], c[0])
}

func pointFromArcGIS(geom, attrs map[string]any) (float64, float64, bool) {
	if lat, latOK := floatAny(attrs["LATITUDE"]); latOK {
		if lon, lonOK := floatAny(attrs["LONGITUDE"]); lonOK && validLatLon(lat, lon) {
			return lat, lon, true
		}
	}
	if x, xOK := floatAny(geom["x"]); xOK {
		if y, yOK := floatAny(geom["y"]); yOK {
			lon, lat := normalizeXY(x, y)
			return lat, lon, validLatLon(lat, lon)
		}
	}
	return 0, 0, false
}

func pointFromOpenDataSoft(row, fields map[string]any) (float64, float64, bool) {
	for _, key := range []string{"geo_point_2d", "geopoint"} {
		if arr, ok := fields[key].([]any); ok && len(arr) >= 2 {
			lat, latOK := floatAny(arr[0])
			lon, lonOK := floatAny(arr[1])
			if latOK && lonOK && validLatLon(lat, lon) {
				return lat, lon, true
			}
		}
	}
	if geom, ok := row["geometry"].(map[string]any); ok {
		if coords, ok := geom["coordinates"].([]any); ok && len(coords) >= 2 {
			lon, lonOK := floatAny(coords[0])
			lat, latOK := floatAny(coords[1])
			if latOK && lonOK && validLatLon(lat, lon) {
				return lat, lon, true
			}
		}
	}
	return 0, 0, false
}

func normalizeXY(x, y float64) (float64, float64) {
	if math.Abs(x) > 180 || math.Abs(y) > 90 {
		lon := x * 180.0 / 20037508.34
		lat := math.Atan(math.Exp(y*math.Pi/20037508.34))*360.0/math.Pi - 90.0
		return lon, lat
	}
	return x, y
}

func floatAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func textAny(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func nonEmpty(vals ...string) []string {
	out := []string{}
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			out = append(out, strings.TrimSpace(v))
		}
	}
	return out
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(h[:])
}
