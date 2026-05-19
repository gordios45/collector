// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package globalping runs low-rate active reachability probes through the
// public Globalping probe network.
package globalping

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	baseURL          = "https://api.globalping.io/v1"
	defaultTarget    = "one.one.one.one"
	defaultLocations = "Ukraine,Iran,Israel,Lebanon,Taiwan,Germany,United States"
)

type Collector struct {
	targets   []string
	locations []string
	packets   int
	client    *http.Client
}

func New() (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_GLOBALPING") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_GLOBALPING=1")
	}
	targets := collectorutil.SplitCSV(os.Getenv("GLOBALPING_TARGETS"))
	if len(targets) == 0 {
		targets = []string{defaultTarget}
	}
	locations := collectorutil.SplitCSV(os.Getenv("GLOBALPING_LOCATIONS"))
	if len(locations) == 0 {
		locations = collectorutil.SplitCSV(defaultLocations)
	}
	return &Collector{
		targets:   targets,
		locations: locations,
		packets:   collectorutil.EnvInt("GLOBALPING_PACKETS", 3, 1, 6),
		client:    &http.Client{Timeout: 25 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "globalping_measurements" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	if len(c.targets) == 0 || len(c.locations) == 0 {
		return nil, nil
	}
	out := []events.Event{}
	var firstErr error
	for _, target := range c.targets {
		id, err := c.createMeasurement(ctx, target)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		m, err := c.waitMeasurement(ctx, id)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, eventsFromMeasurement(m)...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

type createResp struct {
	ID string `json:"id"`
}

func (c *Collector) createMeasurement(ctx context.Context, target string) (string, error) {
	type loc struct {
		Magic string `json:"magic,omitempty"`
		Limit int    `json:"limit,omitempty"`
	}
	reqBody := map[string]any{
		"target": target,
		"type":   "ping",
		"measurementOptions": map[string]any{
			"packets": c.packets,
		},
	}
	locations := make([]loc, 0, len(c.locations))
	for _, item := range c.locations {
		locations = append(locations, loc{Magic: item, Limit: 1})
	}
	reqBody["locations"] = locations
	raw, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/measurements", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	if token := strings.TrimSpace(os.Getenv("GLOBALPING_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
		return "", fmt.Errorf("globalping create %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var cr createResp
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", err
	}
	if cr.ID == "" {
		return "", fmt.Errorf("globalping create returned empty id")
	}
	return cr.ID, nil
}

func (c *Collector) waitMeasurement(ctx context.Context, id string) (measurement, error) {
	var out measurement
	url := baseURL + "/measurements/" + id
	for i := 0; i < 10; i++ {
		if err := httpx.GetJSONWithClient(ctx, c.client, url, map[string]string{"Accept": "application/json"}, &out); err != nil {
			return measurement{}, err
		}
		if out.Status != "in-progress" && out.Status != "queued" && out.Status != "" {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return measurement{}, ctx.Err()
		case <-time.After(time.Duration(600+i*250) * time.Millisecond):
		}
	}
	return out, nil
}

type measurement struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Target      string          `json:"target"`
	CreatedAt   string          `json:"createdAt"`
	UpdatedAt   string          `json:"updatedAt"`
	ProbesCount int             `json:"probesCount"`
	Results     []probeResponse `json:"results"`
}

type probeResponse struct {
	Probe struct {
		Continent string  `json:"continent"`
		Region    string  `json:"region"`
		Country   string  `json:"country"`
		City      string  `json:"city"`
		State     string  `json:"state"`
		ASN       int     `json:"asn"`
		Network   string  `json:"network"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	} `json:"probe"`
	Result struct {
		Status          string   `json:"status"`
		ResolvedAddress string   `json:"resolvedAddress"`
		RawOutput       string   `json:"rawOutput"`
		Timings         []timing `json:"timings"`
		Stats           pingStat `json:"stats"`
		Error           string   `json:"error"`
		Errors          []string `json:"errors"`
		StatusCode      int      `json:"statusCode"`
		TimingsAlt      []timing `json:"-"`
		Raw             any      `json:"-"`
	} `json:"result"`
}

type timing struct {
	RTT float64 `json:"rtt"`
}

type pingStat struct {
	Min   float64 `json:"min"`
	Avg   float64 `json:"avg"`
	Max   float64 `json:"max"`
	Total int     `json:"total"`
	RCV   int     `json:"rcv"`
	Drop  int     `json:"drop"`
	Loss  float64 `json:"loss"`
}

func eventsFromMeasurement(m measurement) []events.Event {
	ts := parseTime(m.UpdatedAt)
	if ts.IsZero() {
		ts = parseTime(m.CreatedAt)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	out := make([]events.Event, 0, len(m.Results))
	for _, r := range m.Results {
		if !collectorutil.ValidLatLon(r.Probe.Latitude, r.Probe.Longitude) {
			continue
		}
		lossPct := packetLossPct(r.Result.Stats)
		reachable := strings.EqualFold(r.Result.Status, "finished") && lossPct < 100
		reachScore := math.Max(lossPct/35.0, 0)
		if !reachable {
			reachScore = math.Max(reachScore, 2.5)
		}
		latencyScore := latencyAnomalyScore(r.Result.Stats.Avg)
		props := map[string]any{
			"measurement_id":             m.ID,
			"measurement_type":           m.Type,
			"target":                     m.Target,
			"status":                     r.Result.Status,
			"reachable":                  reachable,
			"resolved_address":           r.Result.ResolvedAddress,
			"probe_continent":            r.Probe.Continent,
			"probe_region":               r.Probe.Region,
			"probe_country":              r.Probe.Country,
			"probe_city":                 r.Probe.City,
			"probe_state":                r.Probe.State,
			"probe_asn":                  r.Probe.ASN,
			"probe_network":              r.Probe.Network,
			"min_rtt_ms":                 collectorutil.Round(r.Result.Stats.Min, 2),
			"avg_rtt_ms":                 collectorutil.Round(r.Result.Stats.Avg, 2),
			"max_rtt_ms":                 collectorutil.Round(r.Result.Stats.Max, 2),
			"total_packets":              r.Result.Stats.Total,
			"received_packets":           r.Result.Stats.RCV,
			"drop_packets":               r.Result.Stats.Drop,
			"packet_loss":                collectorutil.Round(lossPct, 2),
			"reachability_loss_score":    collectorutil.Round(reachScore, 2),
			"latency_anomaly_score":      collectorutil.Round(latencyScore, 2),
			"source_api_endpoint":        baseURL + "/measurements",
			"source_measurement_credits": 1,
		}
		if r.Result.Error != "" {
			props["error"] = r.Result.Error
		}
		if len(r.Result.Errors) > 0 {
			props["errors"] = r.Result.Errors
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "globalping_measurements",
			ExtID:  strings.Join([]string{m.ID, m.Target, r.Probe.Country, strconv.Itoa(r.Probe.ASN), r.Probe.City}, ":"),
			Lat:    r.Probe.Latitude,
			Lon:    r.Probe.Longitude,
			Props:  props,
		})
	}
	return out
}

func packetLossPct(s pingStat) float64 {
	if s.Loss > 0 {
		if s.Loss <= 1 {
			return s.Loss * 100
		}
		return s.Loss
	}
	if s.Total > 0 {
		if s.Drop > 0 {
			return 100 * float64(s.Drop) / float64(s.Total)
		}
		if s.RCV > 0 && s.RCV <= s.Total {
			return 100 * float64(s.Total-s.RCV) / float64(s.Total)
		}
	}
	return 0
}

func latencyAnomalyScore(avg float64) float64 {
	if avg <= 0 {
		return 0
	}
	return math.Max(0, math.Min(3, (avg-180)/120))
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z"} {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
