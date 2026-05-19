// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package noaa_sua ingests NOAA/MarineCadastre military special-use
// airspace polygons as static proximity context.
package noaa_sua

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceID        = "noaa_military_sua"
	defaultEndpoint = "https://coast.noaa.gov/arcgis/rest/services/Hosted/MilitarySpecialUseAirspace/FeatureServer/0/query"
	publicURL       = "https://www.arcgis.com/home/item.html?id=c02de89cbc814a3789989e1b1235a577"
	pageSize        = 2000
)

type Collector struct {
	pool     *pgxpool.Pool
	endpoint string
	limit    int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{
		pool:     pool,
		endpoint: firstNonEmpty(os.Getenv("NOAA_MILITARY_SUA_GEOJSON_URL"), defaultEndpoint),
		limit:    envInt("NOAA_MILITARY_SUA_LIMIT", 50000),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 7 * 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	feats, err := c.fetchFeatures(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := features.Upsert(ctx, c.pool, sourceID, feats); err != nil {
		return nil, err
	}
	return nil, nil
}

func (c *Collector) fetchFeatures(ctx context.Context) ([]features.Feature, error) {
	out := make([]features.Feature, 0, 4096)
	for offset := 0; ; offset += pageSize {
		if c.limit > 0 && len(out) >= c.limit {
			break
		}
		var raw featureCollection
		if err := httpx.GetJSON(ctx, arcGISQueryURL(c.endpoint, offset, pageSize), map[string]string{"Accept": "application/geo+json,application/json"}, &raw); err != nil {
			return nil, err
		}
		if raw.Error.Message != "" {
			return nil, fmt.Errorf("noaa military sua arcgis: %s", raw.Error.Message)
		}
		page := featuresFromCollection(raw, c.endpoint, offset, c.limit-len(out))
		out = append(out, page...)
		if len(raw.Features) == 0 || !raw.Properties.ExceededTransferLimit || len(raw.Features) < pageSize {
			break
		}
	}
	return out, nil
}

type featureCollection struct {
	Type       string       `json:"type"`
	Features   []geoFeature `json:"features"`
	Properties struct {
		ExceededTransferLimit bool `json:"exceededTransferLimit"`
	} `json:"properties"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

type geoFeature struct {
	ID         any             `json:"id"`
	Type       string          `json:"type"`
	Properties map[string]any  `json:"properties"`
	Geometry   json.RawMessage `json:"geometry"`
}

func featuresFromCollection(raw featureCollection, endpoint string, offset, remaining int) []features.Feature {
	out := make([]features.Feature, 0, len(raw.Features))
	for i, f := range raw.Features {
		if remaining > 0 && len(out) >= remaining {
			break
		}
		wkt := geo.GeoJSONToWKT(f.Geometry)
		if wkt == "" {
			continue
		}
		name := textAny(f.Properties["featurename"])
		airspaceType := textAny(f.Properties["specialuseairspacetype"])
		id := firstNonEmpty(
			textAny(f.ID),
			textAny(f.Properties["objectid"]),
			strings.TrimSpace(name+" "+airspaceType),
			fmt.Sprintf("feature:%d", offset+i),
		)
		props := map[string]any{
			"source_provider":     "NOAA MarineCadastre / U.S. Navy Common Operational Picture",
			"source_kind":         "military_special_use_airspace",
			"source_api_endpoint": endpoint,
			"source_public_url":   publicURL,
			"context_only":        true,
			"feature_name":        name,
			"airspace_type":       airspaceType,
		}
		copyStringProp(props, f.Properties, "featuredescription", "feature_description")
		copyStringProp(props, f.Properties, "airspacestatus", "airspace_status")
		copyStringProp(props, f.Properties, "controllingagency", "controlling_agency")
		copyStringProp(props, f.Properties, "schedulingagency", "scheduling_agency")
		copyStringProp(props, f.Properties, "region", "region")
		copyStringProp(props, f.Properties, "coparea", "cop_area")
		copyStringProp(props, f.Properties, "datasource", "data_source")
		copyAnyProp(props, f.Properties, "flooraltitude", "floor_altitude")
		copyStringProp(props, f.Properties, "floorreferencelevel", "floor_reference_level")
		copyAnyProp(props, f.Properties, "ceilingaltitude", "ceiling_altitude")
		copyStringProp(props, f.Properties, "ceilingreferencelevel", "ceiling_reference_level")
		copyStringProp(props, f.Properties, "altitudeuom", "altitude_uom")
		out = append(out, features.Feature{ExtID: id, GeomWKT: wkt, Props: props})
	}
	return out
}

func arcGISQueryURL(endpoint string, offset, count int) string {
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
	q.Set("returnGeometry", "true")
	q.Set("outSR", "4326")
	q.Set("f", "geojson")
	q.Set("resultRecordCount", strconv.Itoa(count))
	if offset > 0 {
		q.Set("resultOffset", strconv.Itoa(offset))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func copyStringProp(dst, src map[string]any, from, to string) {
	if v := textAny(src[from]); v != "" {
		dst[to] = v
	}
}

func copyAnyProp(dst, src map[string]any, from, to string) {
	if v, ok := src[from]; ok && v != nil {
		dst[to] = v
	}
}

func textAny(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}
