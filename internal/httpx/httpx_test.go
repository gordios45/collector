// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestGetBytesInvalidURLReturnsError(t *testing.T) {
	_, err := GetBytes(context.Background(), "://bad-url", nil)
	if err == nil {
		t.Fatal("expected invalid URL error")
	}
	if !strings.Contains(err.Error(), "build GET") {
		t.Fatalf("error = %v, want request build context", err)
	}
}

func TestGetJSONWithClientSendsDefaultAndCustomHeaders(t *testing.T) {
	var gotUA, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		gotAccept = r.Header.Get("Accept")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	var out struct {
		Status string `json:"status"`
	}
	if err := GetJSONWithClient(context.Background(), srv.Client(), srv.URL, map[string]string{"Accept": "application/json"}, &out); err != nil {
		t.Fatalf("GetJSONWithClient: %v", err)
	}
	if out.Status != "ok" {
		t.Fatalf("status = %q, want ok", out.Status)
	}
	if gotUA != defaultUA {
		t.Fatalf("User-Agent = %q, want %q", gotUA, defaultUA)
	}
	if gotAccept != "application/json" {
		t.Fatalf("Accept = %q, want application/json", gotAccept)
	}
}

func TestGetJSONWithNilClientUsesDefaultClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	var out struct {
		Status string `json:"status"`
	}
	if err := GetJSONWithClient(context.Background(), nil, srv.URL, nil, &out); err != nil {
		t.Fatalf("GetJSONWithClient with nil client: %v", err)
	}
	if out.Status != "ok" {
		t.Fatalf("status = %q, want ok", out.Status)
	}
}

func TestBrowserClientSingletonIsConcurrentSafe(t *testing.T) {
	const workers = 64

	clients := make(chan *http.Client, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clients <- BrowserClient()
		}()
	}
	wg.Wait()
	close(clients)

	var first *http.Client
	for client := range clients {
		if client == nil {
			t.Fatal("BrowserClient returned nil")
		}
		if first == nil {
			first = client
			continue
		}
		if client != first {
			t.Fatal("BrowserClient returned multiple client instances")
		}
	}
}
