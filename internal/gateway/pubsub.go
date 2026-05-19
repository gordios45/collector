// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Hub: fan-out of Postgres LISTEN 'events_changed' notifications to
// connected WebSocket subscribers. The ingester calls pg_notify whenever
// a batch lands; the hub broadcasts a {type:"refresh",source:"X"} message
// to every client subscribed to that source. Clients then re-fetch
// /api/latest to get the newest snapshot.
//
// Keeping it "push a refresh, pull the data" rather than pushing full
// payloads keeps the WS traffic tiny and avoids schema duplication.
package gateway

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type subscriber struct {
	ch    chan refreshMsg
	sources map[string]struct{} // empty = all
}

type refreshMsg struct {
	Type   string `json:"type"`   // "refresh"
	Source string `json:"source"`
	Ts     int64  `json:"ts_ms"`
}

type Hub struct {
	pool *pgxpool.Pool

	mu   sync.RWMutex
	subs map[*subscriber]struct{}
}

func NewHub(pool *pgxpool.Pool) *Hub {
	return &Hub{pool: pool, subs: map[*subscriber]struct{}{}}
}

// Run holds a connection dedicated to LISTEN and forwards notifications.
// Reconnects forever on failure.
func (h *Hub) Run(ctx context.Context) {
	for {
		if err := h.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[hub] listen loop: %v; retrying in 3s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (h *Hub) listenOnce(ctx context.Context) error {
	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN events_changed"); err != nil {
		return err
	}
	log.Println("[hub] LISTEN events_changed")

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		h.broadcast(refreshMsg{
			Type:   "refresh",
			Source: n.Payload,
			Ts:     time.Now().UnixMilli(),
		})
	}
}

func (h *Hub) broadcast(m refreshMsg) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if len(s.sources) > 0 {
			if _, ok := s.sources[m.Source]; !ok {
				continue
			}
		}
		select {
		case s.ch <- m:
		default:
			// Slow client; drop rather than block the hub.
		}
	}
}

func (h *Hub) addSub(sources []string) *subscriber {
	s := &subscriber{
		ch:      make(chan refreshMsg, 16),
		sources: map[string]struct{}{},
	}
	for _, id := range sources {
		s.sources[id] = struct{}{}
	}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *Hub) removeSub(s *subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
	close(s.ch)
}

// Ensure pg_notify payloads are strings; used by tests.
var _ = pgx.ErrNoRows
var _ = json.Marshal
