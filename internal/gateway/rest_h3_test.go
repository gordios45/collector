// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"net/http/httptest"
	"testing"
)

func TestH3R4ParamPrefersExplicitColumnName(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/latest?h3=old&h3_r4=new", nil)
	if got := h3R4Param(req); got != "new" {
		t.Fatalf("h3R4Param = %q, want new", got)
	}
}

func TestH3R4ParamAcceptsH3Alias(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/latest?h3=842d811ffffffff", nil)
	if got := h3R4Param(req); got != "842d811ffffffff" {
		t.Fatalf("h3R4Param = %q, want alias value", got)
	}
}
