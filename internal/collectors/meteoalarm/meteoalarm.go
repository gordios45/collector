// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// MeteoAlarm — pan-European severe-weather warnings (CAP-derived JSON).
//
// Public feeds: https://feeds.meteoalarm.org/api/v1/warnings/feeds-{slug}
// One JSON document per country. Each warning carries CAP-style severity /
// urgency / certainty plus a list of NUTS3/EMMA region geocodes and free-text
// area names — but no polygon geometry. We therefore plot at the country
// centroid (matching the precedent set by travel_advisories) and stash the
// affected region names + identifiers in props so the intel panel can show
// the regional detail.
//
// Cadence: 10 min. Most country feeds are empty in calm weather, so the
// total wire traffic per tick is small.
package meteoalarm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const baseURL = "https://feeds.meteoalarm.org/api/v1/warnings/feeds-"

type country struct {
	slug string
	name string
	lat  float64
	lon  float64
}

// Country slugs match MeteoAlarm's URL convention. Centroids are deliberately
// rough — the intel panel surfaces the precise NUTS3 region names from props.
var countries = []country{
	{"austria", "Austria", 47.5, 14.5},
	{"belgium", "Belgium", 50.5, 4.5},
	{"bosnia-herzegovina", "Bosnia and Herzegovina", 44.0, 18.0},
	{"bulgaria", "Bulgaria", 42.7, 25.5},
	{"croatia", "Croatia", 45.1, 15.5},
	{"cyprus", "Cyprus", 35.0, 33.0},
	{"czechia", "Czechia", 49.7, 15.5},
	{"denmark", "Denmark", 56.0, 10.0},
	{"estonia", "Estonia", 58.6, 25.0},
	{"finland", "Finland", 64.0, 26.0},
	{"france", "France", 46.5, 2.5},
	{"germany", "Germany", 51.0, 10.0},
	{"greece", "Greece", 39.0, 22.0},
	{"hungary", "Hungary", 47.0, 19.5},
	{"iceland", "Iceland", 65.0, -18.0},
	{"ireland", "Ireland", 53.4, -8.0},
	{"israel", "Israel", 31.5, 34.7},
	{"italy", "Italy", 42.8, 12.6},
	{"latvia", "Latvia", 56.9, 24.6},
	{"lithuania", "Lithuania", 55.3, 24.0},
	{"luxembourg", "Luxembourg", 49.8, 6.1},
	{"malta", "Malta", 35.9, 14.4},
	{"moldova", "Moldova", 47.4, 28.4},
	{"montenegro", "Montenegro", 42.7, 19.4},
	{"netherlands", "Netherlands", 52.2, 5.3},
	{"north-macedonia", "North Macedonia", 41.6, 21.7},
	{"norway", "Norway", 64.0, 11.0},
	{"poland", "Poland", 52.0, 19.0},
	{"portugal", "Portugal", 39.5, -8.0},
	{"serbia", "Serbia", 44.0, 21.0},
	{"slovakia", "Slovakia", 48.7, 19.7},
	{"slovenia", "Slovenia", 46.1, 14.8},
	{"spain", "Spain", 40.0, -4.0},
	{"sweden", "Sweden", 60.0, 16.0},
	{"switzerland", "Switzerland", 46.8, 8.2},
	{"united-kingdom", "United Kingdom", 54.0, -2.0},
}

type area struct {
	AreaDesc string           `json:"areaDesc"`
	Geocode  []map[string]any `json:"geocode"`
}

type info struct {
	Language    string   `json:"language"`
	Category    []string `json:"category"`
	Event       string   `json:"event"`
	Urgency     string   `json:"urgency"`
	Severity    string   `json:"severity"`
	Certainty   string   `json:"certainty"`
	Effective   string   `json:"effective"`
	Onset       string   `json:"onset"`
	Expires     string   `json:"expires"`
	SenderName  string   `json:"senderName"`
	Headline    string   `json:"headline"`
	Description string   `json:"description"`
	Instruction string   `json:"instruction"`
	Web         string   `json:"web"`
	Area        []area   `json:"area"`
}

type alert struct {
	Identifier string `json:"identifier"`
	Sender     string `json:"sender"`
	Sent       string `json:"sent"`
	MsgType    string `json:"msgType"`
	Status     string `json:"status"`
	Scope      string `json:"scope"`
	Info       []info `json:"info"`
}

type warning struct {
	UUID  string `json:"uuid"`
	Alert alert  `json:"alert"`
}

type feedResp struct {
	Warnings []warning `json:"warnings"`
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "meteoalarm" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	failed := 0

	jobs := make(chan country)
	var mu sync.Mutex
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for cn := range jobs {
			reqCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
			evs, err := c.fetchOne(reqCtx, cn)
			cancel()

			mu.Lock()
			if err != nil {
				failed++
			} else {
				out = append(out, evs...)
			}
			mu.Unlock()
		}
	}

	const workers = 6
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

send:
	for _, cn := range countries {
		select {
		case <-ctx.Done():
			break send
		case jobs <- cn:
		}
	}
	close(jobs)
	wg.Wait()

	if failed == len(countries) {
		return nil, fmt.Errorf("all %d MeteoAlarm country feeds failed", len(countries))
	}
	if ctx.Err() != nil && len(out) == 0 {
		return nil, ctx.Err()
	}
	return out, nil
}

func (c *Collector) fetchOne(ctx context.Context, cn country) ([]events.Event, error) {
	var resp feedResp
	if err := httpx.GetJSON(ctx, baseURL+cn.slug, map[string]string{"Accept": "application/json"}, &resp); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(resp.Warnings))
	for _, w := range resp.Warnings {
		a := w.Alert
		if a.Identifier == "" {
			continue
		}
		inf := pickInfo(a.Info)
		if inf.Event == "" && inf.Headline == "" {
			continue
		}
		areas := make([]string, 0, len(inf.Area))
		geocodes := make([]string, 0, len(inf.Area))
		for _, ar := range inf.Area {
			if ar.AreaDesc != "" {
				areas = append(areas, ar.AreaDesc)
			}
			for _, gc := range ar.Geocode {
				if v, ok := gc["value"].(string); ok && v != "" {
					geocodes = append(geocodes, v)
				}
			}
		}
		props := map[string]any{
			"event":       inf.Event,
			"severity":    inf.Severity,
			"certainty":   inf.Certainty,
			"urgency":     inf.Urgency,
			"headline":    inf.Headline,
			"description": inf.Description,
			"instruction": inf.Instruction,
			"areaDesc":    strings.Join(areas, "; "),
			"geocodes":    strings.Join(geocodes, ","),
			"country":     cn.name,
			"sender":      a.Sender,
			"senderName":  inf.SenderName,
			"effective":   inf.Effective,
			"onset":       inf.Onset,
			"expires":     inf.Expires,
			"web":         inf.Web,
		}
		out = append(out, events.Event{
			Ts:     parseTS(inf.Onset, a.Sent),
			Source: "meteoalarm",
			ExtID:  a.Identifier,
			Lat:    cn.lat,
			Lon:    cn.lon,
			Props:  props,
		})
	}
	return out, nil
}

// pickInfo prefers the English variant; CAP feeds repeat the same warning in
// 2-3 languages and we only want one row per alert.
func pickInfo(infos []info) info {
	for _, i := range infos {
		if strings.HasPrefix(strings.ToLower(i.Language), "en") {
			return i
		}
	}
	if len(infos) > 0 {
		return infos[0]
	}
	return info{}
}

func parseTS(candidates ...string) time.Time {
	for _, s := range candidates {
		if s == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}
