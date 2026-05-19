// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Space-Track TLE collector — more complete satellite catalogue than
// Celestrak. Gated on SPACETRACK_USER + SPACETRACK_PASS. Writes to the
// same 'tle' source as the Celestrak collector; ON CONFLICT upsert
// (source, ext_id, ts) means the freshest wins — typically Space-Track.
//
// Auth: POST /ajaxauth/login with `identity` + `password` form fields →
// session cookie. Use the cookie for subsequent /basicspacedata queries.
// Free. Rate limits are generous for the "latest per object" query we use.
package spacetrack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const (
	loginURL = "https://www.space-track.org/ajaxauth/login"
	// Current GP records include the generated TLE lines. The older
	// tle_latest class now returns 404 on Space-Track, so use gp constrained
	// to non-decayed payloads. Debris/rocket bodies add substantial write
	// pressure and do not support the ISR-intent layer.
	queryURL = "https://www.space-track.org/basicspacedata/query/class/gp/OBJECT_TYPE/PAYLOAD/DECAY_DATE/null-val/orderby/NORAD_CAT_ID/format/json"
)

type Collector struct {
	user, pass string
	client     *http.Client
}

func New() (*Collector, error) {
	u := strings.TrimSpace(os.Getenv("SPACETRACK_USER"))
	p := strings.TrimSpace(os.Getenv("SPACETRACK_PASS"))
	if u == "" || p == "" {
		return nil, fmt.Errorf("SPACETRACK_USER / SPACETRACK_PASS not set")
	}
	jar, _ := cookiejar.New(nil)
	return &Collector{
		user: u, pass: p,
		client: &http.Client{Timeout: 60 * time.Second, Jar: jar},
	}, nil
}

func (c *Collector) ID() string               { return "spacetrack" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

type tleRow struct {
	NoradCatID string `json:"NORAD_CAT_ID"`
	ObjectName string `json:"OBJECT_NAME"`
	ObjectID   string `json:"OBJECT_ID"`
	Epoch      string `json:"EPOCH"`
	TLELine1   string `json:"TLE_LINE1"`
	TLELine2   string `json:"TLE_LINE2"`
	ObjectType string `json:"OBJECT_TYPE"`
	Country    string `json:"COUNTRY_CODE"`
	RCSSize    string `json:"RCS_SIZE"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	if err := c.login(ctx); err != nil {
		return nil, fmt.Errorf("login: %w", err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	req.Header.Set("User-Agent", "gordios/0.1")
	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 256))
		return nil, fmt.Errorf("spacetrack %d: %s", r.StatusCode, string(body))
	}
	var rows []tleRow
	if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		if row.NoradCatID == "" || row.TLELine1 == "" || row.TLELine2 == "" {
			continue
		}
		ts := time.Now().UTC()
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02T15:04:05.999999",
			"2006-01-02T15:04:05.000",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, row.Epoch); err == nil {
				ts = t.UTC()
				break
			}
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "tle",
			ExtID:  row.NoradCatID,
			Lat:    0, Lon: 0,
			Props: map[string]any{
				"name":         row.ObjectName,
				"line1":        row.TLELine1,
				"line2":        row.TLELine2,
				"group":        strings.ToLower(row.ObjectType), // "payload", "debris", "rocket body"
				"source":       "spacetrack",
				"object_id":    row.ObjectID,
				"country_code": row.Country,
				"rcs_size":     row.RCSSize,
			},
		})
	}
	return out, nil
}

// login keeps the session cookie jar warm.
func (c *Collector) login(ctx context.Context) error {
	form := url.Values{
		"identity": {c.user},
		"password": {c.pass},
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "gordios/0.1")
	r, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 256))
		return fmt.Errorf("%d: %s", r.StatusCode, string(body))
	}
	return nil
}
