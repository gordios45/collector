// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Martrack collector for the ASP.NET WebForms login + JWT bearer auth.
//
// Flow:
//  1. GET /logon         → scrape __VIEWSTATE, __RequestVerificationToken.
//  2. POST /logon        → server redirects 302, sets BODJwtToken cookie.
//  3. /api/* calls       → Authorization: Bearer <jwt> + merged cookies.
//  4. On 401             → force re-login once, retry.
//
// The collector fetches the fleet once per session (80-ish vessels) and
// queries their positions on each Fetch() tick.
package martrack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
)

func urlMustParse(s string) *url.URL { u, _ := url.Parse(s); return u }

const baseURL = "https://martrack.bigoceandata.com"

type Collector struct {
	user, pass string

	mu         sync.Mutex
	client     *http.Client
	jwt        string
	loggedInAt time.Time
	fleetIDs   []int64
}

func New() (*Collector, error) {
	u := os.Getenv("MARTRACK_USER")
	p := os.Getenv("MARTRACK_PASS")
	if u == "" || p == "" {
		return nil, errors.New("MARTRACK_USER / MARTRACK_PASS not set in env")
	}
	return &Collector{user: u, pass: p}, nil
}

func (c *Collector) ID() string               { return "martrack" }
func (c *Collector) PollEvery() time.Duration { return 20 * time.Second }

// Fetch returns the latest position of every vessel in the configured fleet.
func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	if err := c.ensureSession(ctx); err != nil {
		return nil, fmt.Errorf("session: %w", err)
	}
	if len(c.fleetIDs) == 0 {
		ids, err := c.loadFleet(ctx)
		if err != nil {
			return nil, fmt.Errorf("load fleet: %w", err)
		}
		c.fleetIDs = ids
	}

	body := map[string]any{
		"neLat": 85.0, "neLng": 180.0, "swLat": -85.0, "swLng": -180.0,
		"zoomLevel": 3,
		"allAis":    false,
		"assetIds":  c.fleetIDs,
	}
	resp, err := c.call(ctx, http.MethodPost, "/api/position/latestPositions", body)
	if err != nil {
		return nil, err
	}

	var payload struct {
		Assets []map[string]any `json:"assets"`
	}
	if err := json.Unmarshal(resp, &payload); err != nil {
		return nil, fmt.Errorf("parse positions: %w (body=%s)", err, truncate(resp, 200))
	}

	now := time.Now().UTC()
	out := make([]events.Event, 0, len(payload.Assets))
	for _, v := range payload.Assets {
		lat, _ := toFloat(v["lat"])
		lon, _ := toFloat(v["lon"])
		if lat == 0 && lon == 0 {
			continue
		}
		ext := fmt.Sprintf("%v", firstNonNil(v["vesselId"], v["positionId"]))
		if ext == "<nil>" {
			continue
		}
		ts := parseTS(v["dateTime"], now)
		v["is_fleet"] = true
		out = append(out, events.Event{
			Ts:     ts,
			Source: "martrack",
			ExtID:  ext,
			Lat:    lat,
			Lon:    lon,
			Props:  v,
		})
	}
	return out, nil
}

// ---- internals ----

func (c *Collector) ensureSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil && time.Since(c.loggedInAt) < 20*time.Minute {
		return nil
	}
	return c.loginLocked(ctx)
}

func (c *Collector) loginLocked(ctx context.Context) error {
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:     jar,
		Timeout: 15 * time.Second,
		// Martrack's POST /logon redirects to /default.aspx. We want to see
		// the 302 so we can observe BODJwtToken being set, then stop.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Step 1: GET /logon
	req1, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/logon", nil)
	req1.Header.Set("User-Agent", "Mozilla/5.0")
	req1.Header.Set("Accept", "text/html")
	r1, err := client.Do(req1)
	if err != nil {
		return fmt.Errorf("GET /logon: %w", err)
	}
	defer r1.Body.Close()
	html, _ := io.ReadAll(r1.Body)
	log.Printf("[martrack] GET /logon → %d; cookies_after=%d", r1.StatusCode, len(jar.Cookies(urlMustParse(baseURL+"/logon"))))

	viewState := extractHidden(html, "__VIEWSTATE")
	viewStateG := extractHidden(html, "__VIEWSTATEGENERATOR")
	rvt := extractHidden(html, "__RequestVerificationToken")
	if viewState == "" || rvt == "" {
		return fmt.Errorf("logon tokens missing (status=%d)", r1.StatusCode)
	}

	// Step 2: POST /logon
	form := url.Values{}
	form.Set("__VIEWSTATE", viewState)
	if viewStateG != "" {
		form.Set("__VIEWSTATEGENERATOR", viewStateG)
	}
	form.Set("__RequestVerificationToken", rvt)
	form.Set("ctl00$regionBody$txtUsername", c.user)
	form.Set("ctl00$regionBody$txtPassword", c.pass)
	form.Set("ctl00$regionBody$btnLogon", "Login")

	req2, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/logon",
		bytes.NewBufferString(form.Encode()))
	req2.Header.Set("User-Agent", "Mozilla/5.0")
	req2.Header.Set("Accept", "text/html")
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r2, err := client.Do(req2)
	if err != nil {
		return fmt.Errorf("POST /logon: %w", err)
	}
	defer r2.Body.Close()
	bodyForDebug, _ := io.ReadAll(r2.Body)

	// After login the cookie jar has BODJwtToken. Extract it.
	u, _ := url.Parse(baseURL)
	var jwt string
	for _, ck := range jar.Cookies(u) {
		if ck.Name == "BODJwtToken" {
			jwt = ck.Value
			break
		}
	}
	if jwt == "" {
		// Look for ASP.NET validation error text (rendered as red span near the inputs).
		errMsg := extractValidationError(bodyForDebug)
		if errMsg == "" {
			errMsg = "(no validation text found)"
		}
		cookieNames := []string{}
		for _, ck := range jar.Cookies(u) {
			cookieNames = append(cookieNames, ck.Name)
		}
		log.Printf("[martrack] login DID NOT set JWT — status=%d error=%q cookies=%v", r2.StatusCode, errMsg, cookieNames)
		return fmt.Errorf("BODJwtToken not present after login (status=%d) error=%q", r2.StatusCode, errMsg)
	}
	_ = bodyForDebug

	// Swap to a normal client (redirects followed) for /api/* calls.
	c.client = &http.Client{Jar: jar, Timeout: 20 * time.Second}
	c.jwt = jwt
	c.loggedInAt = time.Now()
	return nil
}

func (c *Collector) call(ctx context.Context, method, path string, body any) ([]byte, error) {
	doCall := func() (*http.Response, error) {
		var rdr io.Reader
		if body != nil {
			buf, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			rdr = bytes.NewReader(buf)
		}
		req, _ := http.NewRequestWithContext(ctx, method, baseURL+path, rdr)
		req.Header.Set("User-Agent", "Mozilla/5.0 (gordios)")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.jwt)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		return c.client.Do(req)
	}
	r, err := doCall()
	if err != nil {
		return nil, err
	}
	if r.StatusCode == http.StatusUnauthorized {
		_ = r.Body.Close()
		c.mu.Lock()
		_ = c.loginLocked(ctx)
		c.mu.Unlock()
		r, err = doCall()
		if err != nil {
			return nil, err
		}
	}
	defer r.Body.Close()
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s %s: %d %s", method, path, r.StatusCode, truncate(buf, 200))
	}
	return buf, nil
}

func (c *Collector) loadFleet(ctx context.Context) ([]int64, error) {
	buf, err := c.call(ctx, http.MethodGet, "/api/fleet", nil)
	if err != nil {
		return nil, err
	}
	var list []struct {
		AssetID int64 `json:"assetId"`
	}
	if err := json.Unmarshal(buf, &list); err != nil {
		return nil, fmt.Errorf("parse fleet: %w", err)
	}
	ids := make([]int64, 0, len(list))
	for _, e := range list {
		if e.AssetID != 0 {
			ids = append(ids, e.AssetID)
		}
	}
	return ids, nil
}

// ---- tiny helpers ----

var hiddenRE = map[string]*regexp.Regexp{}
var hiddenRELock sync.Mutex

func extractHidden(html []byte, name string) string {
	hiddenRELock.Lock()
	re, ok := hiddenRE[name]
	if !ok {
		re = regexp.MustCompile(`name="` + regexp.QuoteMeta(name) + `"[^>]*value="([^"]*)"`)
		hiddenRE[name] = re
	}
	hiddenRELock.Unlock()
	m := re.FindSubmatch(html)
	if len(m) < 2 {
		return ""
	}
	return string(m[1])
}

var validationRE = regexp.MustCompile(`(?is)<span[^>]*validation[^>]*>([^<]{3,200})</span>|<div[^>]*alert[^>]*>([^<]{3,200})</div>|<p[^>]*error[^>]*>([^<]{3,200})</p>`)

func extractValidationError(b []byte) string {
	m := validationRE.FindSubmatch(b)
	if len(m) == 0 {
		return ""
	}
	for _, g := range m[1:] {
		if len(g) > 0 {
			s := string(bytes.TrimSpace(g))
			s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
			if len(s) > 200 {
				s = s[:200]
			}
			return s
		}
	}
	return ""
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func firstNonNil(args ...any) any {
	for _, a := range args {
		if a != nil {
			return a
		}
	}
	return nil
}

func parseTS(v any, fallback time.Time) time.Time {
	s, ok := v.(string)
	if !ok || s == "" {
		return fallback
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return fallback
}
