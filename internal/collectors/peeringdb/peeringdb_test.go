// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package peeringdb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gordios45/collector/internal/db"
)

func TestParseFacilitiesBuildsNetworkContextFeature(t *testing.T) {
	body := []byte(`{
	  "data": [{
	    "id": 42,
	    "name": "Carrier Hotel",
	    "org_id": 7,
	    "org_name": "Example IX",
	    "city": "Rome",
	    "country": "IT",
	    "latitude": 41.9028,
	    "longitude": 12.4964,
	    "net_count": 300,
	    "ix_count": 12,
	    "status": "ok"
	  }]
	}`)
	feats, err := parseFacilities(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(feats) != 1 {
		t.Fatalf("features=%d", len(feats))
	}
	if feats[0].ExtID != "fac:42" {
		t.Fatalf("ext_id=%s", feats[0].ExtID)
	}
	if feats[0].GeomWKT != "POINT(12.496400 41.902800)" {
		t.Fatalf("geom=%s", feats[0].GeomWKT)
	}
	if got := feats[0].Props["abi_context_class"]; got != "network_interconnection_site" {
		t.Fatalf("context=%v", got)
	}
	if score, _ := feats[0].Props["network_rank_score"].(float64); score <= 1 {
		t.Fatalf("network_rank_score=%.2f", score)
	}
}

func TestFetchTreatsPeeringDBThrottleAsNoop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "300")
		http.Error(w, `{"message":"Request was throttled."}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := &Collector{
		endpoint: srv.URL,
		limit:    10,
		client:   srv.Client(),
	}
	evs, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch returned throttle error: %v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("events=%d, want 0", len(evs))
	}
}

func TestLiveFetchUpsertsPeeringDBFeatures(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_DB_TESTS") != "1" {
		t.Skip("set GORDIOS_LIVE_DB_TESTS=1 to run against DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := db.Open(ctx)
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	defer pool.Close()
	c, err := New(pool)
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if _, err := c.Fetch(ctx); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM features WHERE source='peeringdb_facilities'`).Scan(&count); err != nil {
		t.Fatalf("count features: %v", err)
	}
	if count == 0 {
		t.Fatalf("peeringdb features count is zero")
	}
	t.Logf("peeringdb facilities=%d", count)
}
