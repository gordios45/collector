// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package cdse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStaticAccessTokenDoesNotCallTokenEndpoint(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatal("token endpoint should not be called for static access token")
	}))
	defer srv.Close()

	c, err := NewClient(Options{TokenURL: srv.URL, AccessToken: "static-token"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tok, err := c.AccessToken(context.Background())
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "static-token" {
		t.Fatalf("token = %q, want static-token", tok)
	}
	if calls != 0 {
		t.Fatalf("endpoint calls = %d, want 0", calls)
	}
}

func TestPasswordGrantRequestsTokenAndCachesIt(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		want := map[string]string{
			"grant_type": "password",
			"client_id":  "cdse-public",
			"username":   "user@example.com",
			"password":   "secret",
			"totp":       "123456",
		}
		for k, v := range want {
			if got := r.Form.Get(k); got != v {
				t.Fatalf("form %s = %q, want %q", k, got, v)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-1",
			"refresh_token": "refresh-1",
			"expires_in":    600,
		})
	}))
	defer srv.Close()

	c, err := NewClient(Options{
		TokenURL: srv.URL,
		Username: "user@example.com",
		Password: "secret",
		TOTP:     "123456",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	for i := 0; i < 2; i++ {
		tok, err := c.AccessToken(context.Background())
		if err != nil {
			t.Fatalf("AccessToken: %v", err)
		}
		if tok != "access-1" {
			t.Fatalf("token = %q, want access-1", tok)
		}
	}
	if calls != 1 {
		t.Fatalf("endpoint calls = %d, want cached single call", calls)
	}
}

func TestTokenResponseCanExceedTwoKilobytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  strings.Repeat("a", 3000),
			"refresh_token": strings.Repeat("r", 3000),
			"expires_in":    600,
		})
	}))
	defer srv.Close()

	c, err := NewClient(Options{
		TokenURL: srv.URL,
		Username: "user@example.com",
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tok, err := c.AccessToken(context.Background())
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if len(tok) != 3000 {
		t.Fatalf("token length = %d, want 3000", len(tok))
	}
}

func TestRefreshGrantPreferredOverPassword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "refresh_token" {
			t.Fatalf("grant_type = %q, want refresh_token", got)
		}
		if got := r.Form.Get("refresh_token"); got != "refresh-0" {
			t.Fatalf("refresh_token = %q, want refresh-0", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "access-refresh",
			"expires_in":   60,
		})
	}))
	defer srv.Close()

	c, err := NewClient(Options{
		TokenURL:     srv.URL,
		RefreshToken: "refresh-0",
		Username:     "user@example.com",
		Password:     "secret",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tok, err := c.AccessToken(context.Background())
	if err != nil {
		t.Fatalf("AccessToken: %v", err)
	}
	if tok != "access-refresh" {
		t.Fatalf("token = %q, want access-refresh", tok)
	}
}

func TestMissingConfigErrors(t *testing.T) {
	_, err := NewClient(Options{})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("err = %v, want required config error", err)
	}
	st := statusFor(Options{})
	if st.Configured {
		t.Fatalf("status configured = true, want false")
	}
}

func TestCacheUsesExpiryBuffer(t *testing.T) {
	c, err := NewClient(Options{AccessToken: "static"})
	if err != nil {
		t.Fatal(err)
	}
	c.cache(tokenResponse{AccessToken: "cached", ExpiresIn: 120})
	if !time.Now().Add(80 * time.Second).Before(c.expires) {
		t.Fatalf("expires = %s, want at least 80s ahead", c.expires)
	}
}
