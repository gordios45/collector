// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// JMA RSMC Tokyo — active tropical cyclones in the NW Pacific basin.
//
// JMA is the WMO-designated Regional Specialized Meteorological Centre for
// the NW Pacific, which is the busiest of JTWC's areas of responsibility.
// While `jtwc` itself stays parked (CloudFront blocks data products from
// non-allowlisted IPs) and `ibtracs` carries 1–3 day provisional latency,
// JMA's public Bosai feed is reachable from any network and updates roughly
// every 3 hours during active periods.
//
// Endpoints (all JSON, public, no auth):
//
//	https://www.jma.go.jp/bosai/typhoon/data/targetTc.json
//	  → array of currently tracked TCs, e.g. [{"tropicalCyclone":"TC2410", ...}].
//	https://www.jma.go.jp/bosai/typhoon/data/{tcID}/specifications.json
//	  → array of records: a "title" record with typhoonNumber + name +
//	    category, then one record per validtime (analysis + forecasts at
//	    +24/48/72/96/120 h). Each non-title record carries position.deg,
//	    pressure, scale, intensity, course, speed.
//
// We emit one event per analysis record (advancedHours = 0) per storm,
// keeping the upcoming forecast track in props so the dashboard can
// render the cone without re-fetching. Off-season (most of the calendar
// outside Jun–Nov) targetTc.json returns [], which is a normal "0 events"
// tick — same pattern as `nhc`.
package jma_rsmc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	listURL    = "https://www.jma.go.jp/bosai/typhoon/data/targetTc.json"
	tcDataBase = "https://www.jma.go.jp/bosai/typhoon/data/"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "jma_rsmc" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

type targetTcEntry struct {
	TropicalCyclone string `json:"tropicalCyclone"`
	TyphoonNumber   string `json:"typhoonNumber"`
	Name            struct {
		EN string `json:"en"`
		JP string `json:"jp"`
	} `json:"name"`
}

// specRecord covers both the title record and per-validtime records.
// Fields not present in a given record stay at zero values.
type specRecord struct {
	// "title" (string) for the title record; otherwise an object with
	// {"en":"Analysis"|"+24h"|...,"jp":...}. We accept both shapes via any.
	Part any `json:"part"`

	// Title-only fields.
	Issue *struct {
		UTC string `json:"UTC"`
		JST string `json:"JST"`
	} `json:"issue,omitempty"`
	TyphoonNumber string `json:"typhoonNumber,omitempty"`
	Name          struct {
		EN string `json:"en"`
		JP string `json:"jp"`
	} `json:"name"`

	// Per-validtime fields.
	AdvancedHours int `json:"advancedHours,omitempty"`
	Category      struct {
		EN string `json:"en"`
		JP string `json:"jp"`
	} `json:"category"`
	Scale     string `json:"scale,omitempty"`
	Intensity string `json:"intensity,omitempty"`
	Position  *struct {
		Deg []float64 `json:"deg"`
	} `json:"position,omitempty"`
	Location string `json:"location,omitempty"`
	Course   string `json:"course,omitempty"`
	Speed    struct {
		KmH string `json:"km/h"`
		Kt  string `json:"kt"`
	} `json:"speed"`
	Pressure  string `json:"pressure,omitempty"`
	Validtime *struct {
		UTC string `json:"UTC"`
		JST string `json:"JST"`
	} `json:"validtime,omitempty"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var list []targetTcEntry
	if err := httpx.GetJSON(ctx, listURL, nil, &list); err != nil {
		return nil, fmt.Errorf("targetTc: %w", err)
	}
	if len(list) == 0 {
		return nil, nil // off-season — normal
	}

	out := make([]events.Event, 0, len(list))
	for _, entry := range list {
		tcID := strings.TrimSpace(entry.TropicalCyclone)
		if tcID == "" {
			continue
		}
		var spec []specRecord
		url := tcDataBase + tcID + "/specifications.json"
		if err := httpx.GetJSON(ctx, url, nil, &spec); err != nil {
			continue // best-effort per storm
		}
		evs := buildEvents(tcID, entry, spec)
		out = append(out, evs...)
	}
	return out, nil
}

// buildEvents emits one event per analysis record (advancedHours = 0) and
// attaches the upcoming forecast track to props.
func buildEvents(tcID string, entry targetTcEntry, spec []specRecord) []events.Event {
	if len(spec) == 0 {
		return nil
	}

	// First pass: collect title metadata + forecast (advancedHours > 0)
	// records so the analysis events can attach them.
	titleNumber := entry.TyphoonNumber
	titleNameEN := entry.Name.EN
	titleNameJP := entry.Name.JP
	var issueUTC string
	var forecast []map[string]any

	for _, r := range spec {
		if isTitle(r.Part) {
			if r.TyphoonNumber != "" {
				titleNumber = r.TyphoonNumber
			}
			if r.Name.EN != "" {
				titleNameEN = r.Name.EN
			}
			if r.Name.JP != "" {
				titleNameJP = r.Name.JP
			}
			if r.Issue != nil {
				issueUTC = r.Issue.UTC
			}
			continue
		}
		if r.AdvancedHours <= 0 {
			continue
		}
		if r.Position == nil || len(r.Position.Deg) < 2 {
			continue
		}
		f := map[string]any{
			"advanced_hours": r.AdvancedHours,
			"lat":            r.Position.Deg[0],
			"lon":            r.Position.Deg[1],
			"pressure_mb":    r.Pressure,
			"category":       r.Category.EN,
			"scale":          r.Scale,
		}
		if r.Validtime != nil {
			f["valid_utc"] = r.Validtime.UTC
		}
		forecast = append(forecast, f)
	}

	out := make([]events.Event, 0, 1)
	for _, r := range spec {
		if isTitle(r.Part) {
			continue
		}
		if r.AdvancedHours != 0 {
			continue
		}
		if r.Position == nil || len(r.Position.Deg) < 2 {
			continue
		}
		if r.Validtime == nil || r.Validtime.UTC == "" {
			continue
		}
		ts, err := parseJMATime(r.Validtime.UTC)
		if err != nil {
			continue
		}
		props := map[string]any{
			"tc_id":          tcID,
			"typhoon_number": titleNumber,
			"name_en":        titleNameEN,
			"name_jp":        titleNameJP,
			"category":       r.Category.EN,
			"category_jp":    r.Category.JP,
			"scale":          r.Scale,
			"intensity":      r.Intensity,
			"location_jp":    r.Location,
			"course_jp":      r.Course,
			"speed_kt":       r.Speed.Kt,
			"speed_kmh":      r.Speed.KmH,
			"pressure_mb":    r.Pressure,
			"valid_utc":      r.Validtime.UTC,
			"valid_jst":      r.Validtime.JST,
			"issue_utc":      issueUTC,
			"forecast":       forecast,
			"agency":         "JMA RSMC Tokyo",
			"basin":          "WP",
		}
		collectorutil.AddTropicalCycloneScores(props, false)
		out = append(out, events.Event{
			Ts:     ts,
			Source: "jma_rsmc",
			ExtID:  tcID + ":" + r.Validtime.UTC,
			Lat:    r.Position.Deg[0],
			Lon:    r.Position.Deg[1],
			Props:  props,
		})
	}
	return out
}

// isTitle reports whether a record is the leading "title" entry. The field
// is "title" (string) for the title record, an {en,jp} object otherwise.
func isTitle(p any) bool {
	s, ok := p.(string)
	return ok && s == "title"
}

// parseJMATime accepts either RFC3339 ("2024-08-20T12:00:00Z") or the
// space-separated "2024-08-20 12:00:00" variant JMA occasionally emits.
func parseJMATime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q", s)
}
