// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package fetchcache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCachedFetcherUsesConditionalRequest(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch requests {
		case 1:
			w.Header().Set("ETag", `"v1"`)
			_, _ = w.Write([]byte("first"))
		case 2:
			if got := r.Header.Get("If-None-Match"); got != `"v1"` {
				t.Errorf("If-None-Match = %q, want %q", got, `"v1"`)
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Errorf("unexpected request %d", requests)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	fetcher := CachedFetcher{Dir: t.TempDir(), Client: srv.Client()}
	body, err := fetcher.GetBytes(context.Background(), srv.URL+"/data.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "first" {
		t.Fatalf("body = %q, want first", body)
	}

	body, err = fetcher.GetBytes(context.Background(), srv.URL+"/data.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "first" {
		t.Fatalf("cached body = %q, want first", body)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestCachedFetcherKeepsUnvalidatedCacheUntilRefresh(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = fmt.Fprintf(w, "body-%d", requests)
	}))
	defer srv.Close()

	dir := t.TempDir()
	fetcher := CachedFetcher{Dir: dir, Client: srv.Client()}
	body, err := fetcher.GetBytes(context.Background(), srv.URL+"/feed.csv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "body-1" {
		t.Fatalf("body = %q, want body-1", body)
	}

	body, err = fetcher.GetBytes(context.Background(), srv.URL+"/feed.csv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "body-1" {
		t.Fatalf("cached body = %q, want body-1", body)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}

	body, err = (CachedFetcher{Dir: dir, Refresh: true, Client: srv.Client()}).GetBytes(context.Background(), srv.URL+"/feed.csv", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "body-2" {
		t.Fatalf("refreshed body = %q, want body-2", body)
	}
}
