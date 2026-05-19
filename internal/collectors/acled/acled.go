// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// ACLED — Armed Conflict Location & Event Data.
// OAuth2 token: POST https://acleddata.com/oauth/token
//   grant_type=password&client_id=acled&username=EMAIL&password=PASS
// Then GET https://acleddata.com/api/acled/read/?...&_format=json
//   Header: Authorization: Bearer <token>
package acled

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const (
	tokenURL = "https://acleddata.com/oauth/token"
	apiURL   = "https://acleddata.com/api/acled/read/"
)

type Collector struct {
	email, password string

	mu      sync.Mutex
	token   string
	expires time.Time
	http    *http.Client
}

func New() (*Collector, error) {
	e := os.Getenv("ACLED_EMAIL")
	p := os.Getenv("ACLED_PASSWORD")
	if e == "" || p == "" {
		return nil, fmt.Errorf("ACLED_EMAIL / ACLED_PASSWORD required")
	}
	return &Collector{
		email: e, password: p,
		http: &http.Client{Timeout: 25 * time.Second},
	}, nil
}

func (c *Collector) ID() string                { return "acled" }
func (c *Collector) PollEvery() time.Duration  { return 2 * time.Hour }

func (c *Collector) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.token != "" && time.Now().Before(c.expires) {
		t := c.token
		c.mu.Unlock()
		return t, nil
	}
	c.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("client_id", "acled")
	form.Set("username", c.email)
	form.Set("password", c.password)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL,
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	r, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 400))
		return "", fmt.Errorf("acled token %d: %s", r.StatusCode, string(body))
	}
	var resp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return "", err
	}
	ttl := resp.ExpiresIn
	if ttl <= 0 {
		ttl = 3600
	}
	c.mu.Lock()
	c.token = resp.AccessToken
	c.expires = time.Now().Add(time.Duration(ttl-300) * time.Second)
	c.mu.Unlock()
	return resp.AccessToken, nil
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	tok, err := c.getToken(ctx)
	if err != nil {
		return nil, err
	}
	// Pull the last 7 days globally, limit 500.
	since := time.Now().UTC().Add(-7 * 24 * time.Hour).Format("2006-01-02")
	q := url.Values{}
	q.Set("_format", "json")
	q.Set("limit", "500")
	q.Set("event_date", since)
	q.Set("event_date_where", ">")
	u := apiURL + "?" + q.Encode()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	r, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 400))
		return nil, fmt.Errorf("acled %d: %s", r.StatusCode, string(body))
	}
	var resp struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(resp.Data))
	for _, ev := range resp.Data {
		lat, _ := toF(ev["latitude"])
		lon, _ := toF(ev["longitude"])
		if lat == 0 && lon == 0 {
			continue
		}
		id := fmt.Sprintf("%v", ev["event_id_cnty"])
		if id == "<nil>" {
			id = fmt.Sprintf("%v", ev["event_id_no_cnty"])
		}
		ts := time.Now().UTC()
		if s, _ := ev["event_date"].(string); s != "" {
			if t, err := time.Parse("2006-01-02", s); err == nil {
				ts = t.UTC()
			}
		}
		out = append(out, events.Event{
			Ts: ts, Source: "acled", ExtID: id,
			Lat: lat, Lon: lon, Props: ev,
		})
	}
	return out, nil
}

func toF(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}
