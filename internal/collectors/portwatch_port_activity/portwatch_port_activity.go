// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package portwatch_port_activity ingests bounded IMF PortWatch daily port
// activity aggregates. It asks ArcGIS for the busiest recent tanker-call
// ports, then resolves their coordinates from the public ports database.
package portwatch_port_activity

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	dailyEndpoint = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query"
	portsEndpoint = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_ports_database/FeatureServer/0/query"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "portwatch_port_activity" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	rows, err := fetchActivity(ctx, 500)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.PortID)
	}
	refs := fetchPortRefs(ctx, ids)
	now := time.Now().UTC()
	out := make([]events.Event, 0, len(rows))
	for _, r := range rows {
		ref, ok := refs[r.PortID]
		if !ok || !validLatLon(ref.Lat, ref.Lon) {
			continue
		}
		props := map[string]any{
			"portid":                r.PortID,
			"portname":              firstNonEmpty(r.PortName, ref.Name),
			"iso3":                  firstNonEmpty(r.ISO3, ref.ISO3),
			"latest_date":           r.LatestDate,
			"window_days":           14,
			"tanker_calls_14d":      round(r.TankerCalls, 1),
			"avg_tanker_calls_day":  round(r.AvgTankerCalls, 2),
			"import_tanker_14d":     round(r.ImportTanker, 1),
			"export_tanker_14d":     round(r.ExportTanker, 1),
			"activity_score":        activityScore(r),
			"source_api_endpoint":   dailyEndpoint,
			"ports_source_endpoint": portsEndpoint,
		}
		out = append(out, events.Event{
			Ts:     now,
			Source: "portwatch_port_activity",
			ExtID:  "port:" + r.PortID,
			Lat:    ref.Lat,
			Lon:    ref.Lon,
			Props:  props,
		})
	}
	return out, nil
}

type activityRow struct {
	PortID         string
	PortName       string
	ISO3           string
	LatestDate     string
	TankerCalls    float64
	AvgTankerCalls float64
	ImportTanker   float64
	ExportTanker   float64
}

func fetchActivity(ctx context.Context, limit int) ([]activityRow, error) {
	since := time.Now().UTC().AddDate(0, 0, -14).Format("2006-01-02 15:04:05")
	stats := []map[string]any{
		{"statisticType": "sum", "onStatisticField": "portcalls_tanker", "outStatisticFieldName": "sum_calls"},
		{"statisticType": "avg", "onStatisticField": "portcalls_tanker", "outStatisticFieldName": "avg_calls"},
		{"statisticType": "sum", "onStatisticField": "import_tanker", "outStatisticFieldName": "sum_import"},
		{"statisticType": "sum", "onStatisticField": "export_tanker", "outStatisticFieldName": "sum_export"},
		{"statisticType": "max", "onStatisticField": "date", "outStatisticFieldName": "latest_date"},
	}
	rawStats, _ := json.Marshal(stats)
	q := url.Values{}
	q.Set("where", fmt.Sprintf("date > timestamp '%s'", since))
	q.Set("groupByFieldsForStatistics", "portid,portname,ISO3")
	q.Set("outStatistics", string(rawStats))
	q.Set("orderByFields", "sum_calls DESC")
	q.Set("resultRecordCount", strconv.Itoa(limit))
	q.Set("returnGeometry", "false")
	q.Set("f", "json")

	var body arcResponse
	if err := httpx.GetJSON(ctx, dailyEndpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &body); err != nil {
		return nil, err
	}
	if body.Error.Message != "" {
		return nil, fmt.Errorf("portwatch activity arcgis: %s", body.Error.Message)
	}
	out := make([]activityRow, 0, len(body.Features))
	for _, f := range body.Features {
		a := f.Attributes
		id := text(a["portid"])
		if id == "" {
			continue
		}
		out = append(out, activityRow{
			PortID:         id,
			PortName:       text(a["portname"]),
			ISO3:           text(a["ISO3"]),
			LatestDate:     arcDate(a["latest_date"]),
			TankerCalls:    num(a["sum_calls"]),
			AvgTankerCalls: num(a["avg_calls"]),
			ImportTanker:   num(a["sum_import"]),
			ExportTanker:   num(a["sum_export"]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TankerCalls > out[j].TankerCalls })
	return out, nil
}

type portRef struct {
	Name string
	ISO3 string
	Lat  float64
	Lon  float64
}

func fetchPortRefs(ctx context.Context, ids []string) map[string]portRef {
	out := map[string]portRef{}
	for start := 0; start < len(ids); start += 80 {
		end := start + 80
		if end > len(ids) {
			end = len(ids)
		}
		parts := make([]string, 0, end-start)
		for _, id := range ids[start:end] {
			parts = append(parts, "'"+strings.ReplaceAll(id, "'", "''")+"'")
		}
		q := url.Values{}
		q.Set("where", "portid IN ("+strings.Join(parts, ",")+")")
		q.Set("outFields", "portid,portname,ISO3,lat,lon")
		q.Set("returnGeometry", "false")
		q.Set("resultRecordCount", "200")
		q.Set("outSR", "4326")
		q.Set("f", "json")
		var body arcResponse
		if err := httpx.GetJSON(ctx, portsEndpoint+"?"+q.Encode(), map[string]string{"Accept": "application/json"}, &body); err != nil {
			continue
		}
		for _, f := range body.Features {
			a := f.Attributes
			id := text(a["portid"])
			if id == "" {
				continue
			}
			out[id] = portRef{Name: text(a["portname"]), ISO3: text(a["ISO3"]), Lat: num(a["lat"]), Lon: num(a["lon"])}
		}
	}
	return out
}

type arcResponse struct {
	Features []struct {
		Attributes map[string]any `json:"attributes"`
	} `json:"features"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func activityScore(r activityRow) float64 {
	score := math.Log1p(math.Max(0, r.TankerCalls)) / 2.5
	if r.ImportTanker+r.ExportTanker > 0 {
		score += math.Log1p(r.ImportTanker+r.ExportTanker) / 8.0
	}
	return round(math.Min(score, 4), 2)
}

func text(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	default:
		return 0
	}
}

func arcDate(v any) string {
	switch x := v.(type) {
	case float64:
		if x <= 0 {
			return ""
		}
		return time.UnixMilli(int64(x)).UTC().Format("2006-01-02")
	case string:
		return strings.TrimSpace(x)
	default:
		return ""
	}
}

func validLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func round(v float64, digits int) float64 {
	scale := math.Pow10(digits)
	return math.Round(v*scale) / scale
}
