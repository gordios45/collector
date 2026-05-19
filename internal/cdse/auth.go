// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package cdse provides Copernicus Data Space Ecosystem authentication helpers.
package cdse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultTokenURL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

type Options struct {
	TokenURL     string
	ClientID     string
	AccessToken  string
	RefreshToken string
	Username     string
	Password     string
	TOTP         string
	HTTPClient   *http.Client
}

type Client struct {
	tokenURL string
	clientID string

	staticToken  string
	refreshToken string
	username     string
	password     string
	totp         string

	http *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

type Status struct {
	Configured bool   `json:"configured"`
	Method     string `json:"method,omitempty"`
}

func NewClientFromEnv() (*Client, error) {
	return NewClient(Options{
		TokenURL:     os.Getenv("CDSE_TOKEN_URL"),
		ClientID:     os.Getenv("CDSE_CLIENT_ID"),
		AccessToken:  os.Getenv("CDSE_ACCESS_TOKEN"),
		RefreshToken: os.Getenv("CDSE_REFRESH_TOKEN"),
		Username:     os.Getenv("CDSE_USERNAME"),
		Password:     os.Getenv("CDSE_PASSWORD"),
		TOTP:         os.Getenv("CDSE_TOTP"),
	})
}

func StatusFromEnv() Status {
	return statusFor(Options{
		AccessToken:  os.Getenv("CDSE_ACCESS_TOKEN"),
		RefreshToken: os.Getenv("CDSE_REFRESH_TOKEN"),
		Username:     os.Getenv("CDSE_USERNAME"),
		Password:     os.Getenv("CDSE_PASSWORD"),
	})
}

func NewClient(opts Options) (*Client, error) {
	if opts.TokenURL == "" {
		opts.TokenURL = defaultTokenURL
	}
	if opts.ClientID == "" {
		opts.ClientID = "cdse-public"
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 25 * time.Second}
	}
	st := statusFor(opts)
	if !st.Configured {
		return nil, errors.New("CDSE_ACCESS_TOKEN, CDSE_REFRESH_TOKEN, or CDSE_USERNAME/CDSE_PASSWORD required")
	}
	return &Client{
		tokenURL:     opts.TokenURL,
		clientID:     opts.ClientID,
		staticToken:  strings.TrimSpace(opts.AccessToken),
		refreshToken: strings.TrimSpace(opts.RefreshToken),
		username:     strings.TrimSpace(opts.Username),
		password:     opts.Password,
		totp:         strings.TrimSpace(opts.TOTP),
		http:         opts.HTTPClient,
	}, nil
}

func (c *Client) Status() Status {
	return statusFor(Options{
		AccessToken:  c.staticToken,
		RefreshToken: c.refreshToken,
		Username:     c.username,
		Password:     c.password,
	})
}

func (c *Client) AccessToken(ctx context.Context) (string, error) {
	if c.staticToken != "" {
		return c.staticToken, nil
	}
	c.mu.Lock()
	if c.token != "" && time.Now().Before(c.expires) {
		tok := c.token
		c.mu.Unlock()
		return tok, nil
	}
	refresh := c.refreshToken
	c.mu.Unlock()

	var tok tokenResponse
	var err error
	if refresh != "" {
		tok, err = c.fetchToken(ctx, refreshTokenGrant(refresh))
		if err == nil {
			c.cache(tok)
			return tok.AccessToken, nil
		}
		if c.username == "" || c.password == "" {
			return "", err
		}
	}

	tok, err = c.fetchToken(ctx, passwordGrant(c.username, c.password, c.totp))
	if err != nil {
		return "", err
	}
	c.cache(tok)
	return tok.AccessToken, nil
}

func (c *Client) AuthorizationHeader(ctx context.Context) (string, error) {
	tok, err := c.AccessToken(ctx)
	if err != nil {
		return "", err
	}
	return "Bearer " + tok, nil
}

func (c *Client) cache(tok tokenResponse) {
	ttl := tok.ExpiresIn
	if ttl <= 0 {
		ttl = 600
	}
	expires := time.Now().Add(time.Duration(ttl) * time.Second)
	if ttl > 60 {
		expires = expires.Add(-30 * time.Second)
	}
	c.mu.Lock()
	c.token = tok.AccessToken
	c.expires = expires
	if tok.RefreshToken != "" {
		c.refreshToken = tok.RefreshToken
	}
	c.mu.Unlock()
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

func (c *Client) fetchToken(ctx context.Context, form url.Values) (tokenResponse, error) {
	form.Set("client_id", c.clientID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return tokenResponse{}, fmt.Errorf("cdse token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return tokenResponse{}, fmt.Errorf("cdse token %d: %s", resp.StatusCode, redactTokenBody(string(body)))
	}
	body, _ := io.ReadAll(resp.Body)
	var tok tokenResponse
	if err := json.Unmarshal(body, &tok); err != nil {
		return tokenResponse{}, fmt.Errorf("cdse token parse: %w", err)
	}
	if tok.AccessToken == "" {
		return tokenResponse{}, errors.New("cdse token response missing access_token")
	}
	return tok, nil
}

func refreshTokenGrant(refreshToken string) url.Values {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	return form
}

func passwordGrant(username, password, totp string) url.Values {
	form := url.Values{}
	form.Set("grant_type", "password")
	form.Set("username", username)
	form.Set("password", password)
	if totp != "" {
		form.Set("totp", totp)
	}
	return form
}

func statusFor(opts Options) Status {
	switch {
	case strings.TrimSpace(opts.AccessToken) != "":
		return Status{Configured: true, Method: "access_token"}
	case strings.TrimSpace(opts.RefreshToken) != "":
		return Status{Configured: true, Method: "refresh_token"}
	case strings.TrimSpace(opts.Username) != "" && opts.Password != "":
		return Status{Configured: true, Method: "password"}
	default:
		return Status{Configured: false}
	}
}

func redactTokenBody(s string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return strings.TrimSpace(s)
	}
	delete(m, "access_token")
	delete(m, "refresh_token")
	delete(m, "id_token")
	delete(m, "session_state")
	b, err := json.Marshal(m)
	if err != nil {
		return strings.TrimSpace(s)
	}
	return string(b)
}
