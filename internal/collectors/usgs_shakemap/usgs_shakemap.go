// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// USGS ShakeMap / PAGER impact products for significant earthquakes.
//
// The base USGS seismic collector stores raw earthquake points. This layer
// keeps the richer impact products that arrive shortly after larger events:
// ShakeMap ground-motion/intensity maxima and PAGER alert metadata.
package usgs_shakemap

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const feedURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "usgs_shakemap" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

type feed struct {
	Features []feedFeature `json:"features"`
}

type feedFeature struct {
	ID         string         `json:"id"`
	Properties map[string]any `json:"properties"`
	Geometry   struct {
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
}

type detailFeature struct {
	Properties map[string]any `json:"properties"`
}

type product struct {
	ID         string                   `json:"id"`
	Type       string                   `json:"type"`
	Code       string                   `json:"code"`
	Source     string                   `json:"source"`
	UpdateTime int64                    `json:"updateTime"`
	Status     string                   `json:"status"`
	Properties map[string]string        `json:"properties"`
	Contents   map[string]productObject `json:"contents"`
}

type productObject struct {
	ContentType string `json:"contentType"`
	URL         string `json:"url"`
	Length      int64  `json:"length"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var raw feed
	if err := httpx.GetJSON(ctx, feedURL, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		ev, ok, err := c.eventForFeature(ctx, f)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (c *Collector) eventForFeature(ctx context.Context, f feedFeature) (events.Event, bool, error) {
	if f.ID == "" || len(f.Geometry.Coordinates) < 2 {
		return events.Event{}, false, nil
	}
	types := str(f.Properties["types"])
	if !strings.Contains(types, "shakemap") && !strings.Contains(types, "losspager") {
		return events.Event{}, false, nil
	}
	lon, lat := f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
	depth := 0.0
	if len(f.Geometry.Coordinates) >= 3 {
		depth = f.Geometry.Coordinates[2]
	}
	ts := time.Now().UTC()
	if v, ok := f.Properties["time"].(float64); ok && v > 0 {
		ts = time.UnixMilli(int64(v)).UTC()
	}

	props := map[string]any{
		"event_id":     f.ID,
		"title":        str(f.Properties["title"]),
		"place":        str(f.Properties["place"]),
		"mag":          f.Properties["mag"],
		"depth_km":     depth,
		"alert":        f.Properties["alert"],
		"felt":         f.Properties["felt"],
		"cdi":          f.Properties["cdi"],
		"mmi":          f.Properties["mmi"],
		"sig":          f.Properties["sig"],
		"tsunami":      f.Properties["tsunami"],
		"types":        types,
		"usgs_url":     str(f.Properties["url"]),
		"detail_url":   str(f.Properties["detail"]),
		"has_shakemap": strings.Contains(types, "shakemap"),
		"has_pager":    strings.Contains(types, "losspager"),
		"source_feed":  feedURL,
	}
	for _, key := range []string{"time", "updated", "status", "net", "code", "magType"} {
		if v, ok := f.Properties[key]; ok {
			props[key] = v
		}
	}

	detailURL := str(f.Properties["detail"])
	if detailURL != "" {
		if err := enrichProducts(ctx, detailURL, props); err != nil {
			props["product_enrichment_error"] = err.Error()
		}
	}
	collectorutil.AddUSGSShakeMapScores(props)

	return events.Event{
		Ts:     ts,
		Source: "usgs_shakemap",
		ExtID:  f.ID,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true, nil
}

func enrichProducts(ctx context.Context, detailURL string, props map[string]any) error {
	var detail detailFeature
	if err := httpx.GetJSON(ctx, detailURL, nil, &detail); err != nil {
		return err
	}
	products, _ := detail.Properties["products"].(map[string]any)
	if products == nil {
		return nil
	}
	if p := firstProduct(products["shakemap"]); p != nil {
		props["has_shakemap"] = true
		props["shakemap_product_id"] = p.ID
		props["shakemap_status"] = p.Status
		props["shakemap_update_time"] = p.UpdateTime
		copyProductFloats(props, "shakemap", p.Properties,
			"maxmmi", "maxmmi-grid", "maxpga", "maxpga-grid", "maxpgv", "maxpgv-grid",
			"maxpsa03", "maxpsa10", "maxpsa30",
			"minimum-latitude", "maximum-latitude", "minimum-longitude", "maximum-longitude",
		)
		copyProductStrings(props, "shakemap", p.Properties,
			"event-description", "map-status", "review-status", "process-timestamp", "version",
		)
		copyContentURL(props, "shakemap_grid_url", p.Contents, "download/grid.xml")
		copyContentURL(props, "shakemap_intensity_url", p.Contents, "download/intensity.jpg")
		copyContentURL(props, "shakemap_intensity_overlay_url", p.Contents, "download/intensity_overlay.png")
		copyContentURL(props, "shakemap_info_url", p.Contents, "download/info.json")
	}
	if p := firstProduct(products["losspager"]); p != nil {
		props["has_pager"] = true
		props["pager_product_id"] = p.ID
		props["pager_status"] = p.Status
		props["pager_update_time"] = p.UpdateTime
		copyProductFloats(props, "pager", p.Properties, "maxmmi", "magnitude", "depth", "latitude", "longitude")
		copyProductStrings(props, "pager", p.Properties, "alertlevel", "review-status", "eventtime")
		copyContentURL(props, "pager_alerts_url", p.Contents, "alerts.json", "json/alerts.json")
		copyContentURL(props, "pager_exposures_url", p.Contents, "exposures.json", "json/exposures.json")
		copyContentURL(props, "pager_losses_url", p.Contents, "losses.json", "json/losses.json")
		copyContentURL(props, "pager_onepager_url", p.Contents, "onepager.pdf")
	}
	return nil
}

func firstProduct(raw any) *product {
	arr, _ := raw.([]any)
	if len(arr) == 0 {
		return nil
	}
	b, ok := arr[0].(map[string]any)
	if !ok {
		return nil
	}
	p := &product{
		ID:         str(b["id"]),
		Type:       str(b["type"]),
		Code:       str(b["code"]),
		Source:     str(b["source"]),
		Status:     str(b["status"]),
		Properties: map[string]string{},
		Contents:   map[string]productObject{},
	}
	if v, ok := b["updateTime"].(float64); ok {
		p.UpdateTime = int64(v)
	}
	if props, ok := b["properties"].(map[string]any); ok {
		for k, v := range props {
			p.Properties[k] = fmt.Sprintf("%v", v)
		}
	}
	if contents, ok := b["contents"].(map[string]any); ok {
		for name, rawObj := range contents {
			objMap, _ := rawObj.(map[string]any)
			if objMap == nil {
				continue
			}
			obj := productObject{
				ContentType: str(objMap["contentType"]),
				URL:         str(objMap["url"]),
			}
			if v, ok := objMap["length"].(float64); ok {
				obj.Length = int64(v)
			}
			p.Contents[name] = obj
		}
	}
	return p
}

func copyProductFloats(dst map[string]any, prefix string, src map[string]string, keys ...string) {
	for _, key := range keys {
		if v, ok := parseFloat(src[key]); ok {
			dst[prefix+"_"+cleanKey(key)] = v
		}
	}
}

func copyProductStrings(dst map[string]any, prefix string, src map[string]string, keys ...string) {
	for _, key := range keys {
		if src[key] != "" {
			dst[prefix+"_"+cleanKey(key)] = src[key]
		}
	}
}

func copyContentURL(dst map[string]any, key string, contents map[string]productObject, names ...string) {
	for _, name := range names {
		if obj, ok := contents[name]; ok && obj.URL != "" {
			dst[key] = obj.URL
			return
		}
	}
}

func cleanKey(s string) string {
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return s
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
