// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// WebSocket endpoint: /stream?source=martrack,flights,...
// Pushes {type:"refresh",source:"<id>"} whenever a collector lands a batch.
// The browser then re-fetches /api/latest for fresh data.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type WSHandler struct{ Hub *Hub }

func (h *WSHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/stream", h.stream)
}

func (h *WSHandler) stream(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("source")
	var sources []string
	if raw != "" {
		for _, s := range strings.Split(raw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				sources = append(sources, s)
			}
		}
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Dev convenience: permit the frontend served from the Node static
		// server (different port) until Phase 9 folds static serving in.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer c.CloseNow()

	ctx := r.Context()

	// Say hello so the client knows subscription is live.
	hello, _ := json.Marshal(map[string]any{
		"type":    "hello",
		"sources": sources,
		"ts_ms":   time.Now().UnixMilli(),
	})
	if err := c.Write(ctx, websocket.MessageText, hello); err != nil {
		return
	}

	sub := h.Hub.addSub(sources)
	defer h.Hub.removeSub(sub)

	// Drain inbound to detect closes; we don't act on anything the client says.
	go func() {
		for {
			if _, _, err := c.Reader(ctx); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		case m, ok := <-sub.ch:
			if !ok {
				return
			}
			buf, _ := json.Marshal(m)
			if err := c.Write(ctx, websocket.MessageText, buf); err != nil {
				return
			}
		}
	}
}
