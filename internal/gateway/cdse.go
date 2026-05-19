// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/gordios45/collector/internal/cdse"
)

func (h *RestHandler) cdseAuthStatus(w http.ResponseWriter, r *http.Request) {
	status := cdse.StatusFromEnv()
	out := map[string]any{
		"configured": status.Configured,
	}
	if status.Method != "" {
		out["method"] = status.Method
	}
	if r.URL.Query().Get("check") != "1" {
		writeJSON(w, out)
		return
	}

	out["checked"] = true
	client, err := cdse.NewClientFromEnv()
	if err != nil {
		out["ok"] = false
		out["error"] = err.Error()
		writeJSON(w, out)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	if _, err := client.AccessToken(ctx); err != nil {
		out["ok"] = false
		out["error"] = err.Error()
		writeJSON(w, out)
		return
	}
	out["ok"] = true
	writeJSON(w, out)
}
