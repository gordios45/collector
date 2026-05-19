// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRestHandlerRawRoutesExcludeSignalRoutes(t *testing.T) {
	mux := http.NewServeMux()
	(&RestHandler{}).RegisterRaw(mux)

	assertPattern(t, mux, "/api/latest", "/api/latest")
	assertPattern(t, mux, "/api/ingestion/aois", "/api/ingestion/aois")
	assertPattern(t, mux, "/api/fusion", "")
	assertPattern(t, mux, "/api/candidates", "")
	assertPattern(t, mux, "/api/signals/replay", "")
}

func assertPattern(t *testing.T, mux *http.ServeMux, path string, want string) {
	t.Helper()
	_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil))
	if pattern != want {
		t.Fatalf("pattern for %s = %q, want %q", path, pattern, want)
	}
}
