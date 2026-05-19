// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Streaming collectors: long-lived upstream connections (WebSocket, SSE)
// that ingest continuously rather than being polled on a cadence.
//
// A StreamCollector runs its own Run(ctx) loop. It emits events into the
// supplied Sink which handles DB insert (`events` hypertable) + pg_notify,
// batching under the hood so each inbound frame doesn't trigger a round-trip.
//
// Source kinds that fit:
//   - events-shaped (append-only, time-indexed): lightning strikes. Use EventSink.
//   - features-shaped (latest state per ext_id):  AIS vessel positions. Use FeatureSink.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/features"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamCollector is any process that maintains a long-lived ingest loop.
// It decides internally when to flush batches to the sink.
type StreamCollector interface {
	ID() string
	Run(ctx context.Context) error
}

// EventSink batches events for insertion into the `events` hypertable.
// Batches flush on size (maxBatch) or time (flushEvery), whichever hits
// first. pg_notify fires once per flush so WS subscribers only see one
// refresh per batch.
type EventSink struct {
	pool       *pgxpool.Pool
	source     string
	maxBatch   int
	flushEvery time.Duration

	mu  sync.Mutex
	buf []events.Event
}

func NewEventSink(pool *pgxpool.Pool, source string, maxBatch int, flushEvery time.Duration) *EventSink {
	if maxBatch <= 0 {
		maxBatch = 500
	}
	if flushEvery <= 0 {
		flushEvery = 500 * time.Millisecond
	}
	return &EventSink{pool: pool, source: source, maxBatch: maxBatch, flushEvery: flushEvery}
}

// Run the background flusher. Blocks until ctx is done.
func (s *EventSink) Run(ctx context.Context) {
	t := time.NewTicker(s.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = s.flush(context.Background())
			return
		case <-t.C:
			if err := s.flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[%s] flush: %v", s.source, err)
			}
		}
	}
}

// Push appends to the buffer. Auto-flushes when the buffer hits maxBatch.
func (s *EventSink) Push(ctx context.Context, e events.Event) {
	s.mu.Lock()
	s.buf = append(s.buf, e)
	overflow := len(s.buf) >= s.maxBatch
	s.mu.Unlock()
	if overflow {
		if err := s.flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[%s] flush (overflow): %v", s.source, err)
		}
	}
}

func (s *EventSink) flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buf
	s.buf = make([]events.Event, 0, s.maxBatch)
	s.mu.Unlock()

	start := time.Now()
	payloadBytes := estimatedEventPayloadBytes(batch)
	n, err := insertEvents(ctx, s.pool, batch)
	if err != nil {
		recordSourceRun(ctx, s.pool, s.source, start, time.Now(), false, len(batch), 0, payloadBytes, err)
		return err
	}
	recordSourceRun(ctx, s.pool, s.source, start, time.Now(), true, len(batch), n, payloadBytes, nil)
	_, _ = s.pool.Exec(ctx,
		`UPDATE sources SET last_fetch_at=now(), last_ok_at=now(), last_err=NULL WHERE id=$1`, s.source)
	if n > 0 {
		if _, err := s.pool.Exec(ctx, `SELECT pg_notify('events_changed', $1)`, s.source); err != nil {
			return fmt.Errorf("notify: %w", err)
		}
	}
	return nil
}

// FeatureSink batches feature upserts (latest state per ext_id) and fires
// pg_notify('events_changed', source) on flush — we reuse the existing
// channel so WS clients don't need a second subscription path.
type FeatureSink struct {
	pool       *pgxpool.Pool
	source     string
	maxBatch   int
	flushEvery time.Duration

	mu  sync.Mutex
	buf []features.Feature
}

func NewFeatureSink(pool *pgxpool.Pool, source string, maxBatch int, flushEvery time.Duration) *FeatureSink {
	if maxBatch <= 0 {
		maxBatch = 500
	}
	if flushEvery <= 0 {
		flushEvery = 2 * time.Second
	}
	return &FeatureSink{pool: pool, source: source, maxBatch: maxBatch, flushEvery: flushEvery}
}

func (s *FeatureSink) Run(ctx context.Context) {
	t := time.NewTicker(s.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = s.flush(context.Background())
			return
		case <-t.C:
			if err := s.flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[%s] feature flush: %v", s.source, err)
			}
		}
	}
}

// Push buffers a feature. The latest entry per ext_id in the buffer wins
// — duplicate MMSIs arriving between flushes don't bloat the batch.
func (s *FeatureSink) Push(ctx context.Context, f features.Feature) {
	s.mu.Lock()
	// Dedup by ext_id in-place (newest overwrites).
	replaced := false
	for i := range s.buf {
		if s.buf[i].ExtID == f.ExtID {
			s.buf[i] = f
			replaced = true
			break
		}
	}
	if !replaced {
		s.buf = append(s.buf, f)
	}
	overflow := len(s.buf) >= s.maxBatch
	s.mu.Unlock()
	if overflow {
		if err := s.flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[%s] feature flush (overflow): %v", s.source, err)
		}
	}
}

func (s *FeatureSink) flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buf
	s.buf = make([]features.Feature, 0, s.maxBatch)
	s.mu.Unlock()

	start := time.Now()
	payloadBytes := estimatedFeaturePayloadBytes(batch)
	n, err := features.UpsertBatch(ctx, s.pool, s.source, batch)
	if err != nil {
		recordSourceRun(ctx, s.pool, s.source, start, time.Now(), false, len(batch), 0, payloadBytes, err)
		return err
	}
	recordSourceRun(ctx, s.pool, s.source, start, time.Now(), true, len(batch), n, payloadBytes, nil)
	_, _ = s.pool.Exec(ctx,
		`UPDATE sources SET last_fetch_at=now(), last_ok_at=now(), last_err=NULL WHERE id=$1`, s.source)
	if n > 0 {
		if _, err := s.pool.Exec(ctx, `SELECT pg_notify('events_changed', $1)`, s.source); err != nil {
			return fmt.Errorf("notify: %w", err)
		}
	}
	return nil
}

// insertEvents mirrors Scheduler.insert but is exported via the sink so
// streaming collectors don't need a Scheduler reference. Shares the same
// ON CONFLICT upsert so accidental retransmits dedupe.
func insertEvents(ctx context.Context, pool *pgxpool.Pool, evs []events.Event) (int, error) {
	const chunkSize = eventInsertChunkSize
	if len(evs) > chunkSize {
		total := 0
		for start := 0; start < len(evs); start += chunkSize {
			end := start + chunkSize
			if end > len(evs) {
				end = len(evs)
			}
			n, err := insertEvents(ctx, pool, evs[start:end])
			if err != nil {
				return total, err
			}
			total += n
		}
		return total, nil
	}

	release, err := acquireEventInsertSlot(ctx)
	if err != nil {
		return 0, err
	}
	defer release()

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const stmt = `INSERT INTO events (ts, source, ext_id, geom, props)
	              VALUES ($1, $2, $3,
	                      CASE
	                        WHEN $4::text <> '' THEN ST_GeogFromText($4)
	                        WHEN $5::float8 <> 0 OR $6::float8 <> 0 THEN ST_MakePoint($5::float8, $6::float8)::geography
	                        ELSE NULL
	                      END,
	                      $7::jsonb)
	              ON CONFLICT (source, ext_id, ts) DO NOTHING
	              RETURNING TRUE`

	inserted := make([]events.Event, 0, len(evs))
	for _, e := range evs {
		raw, err := json.Marshal(e.Props)
		if err != nil {
			return 0, fmt.Errorf("marshal props: %w", err)
		}
		wkt := ""
		if e.Geom != "" {
			wkt = e.Geom
		}
		var ok bool
		if err := tx.QueryRow(ctx, stmt, e.Ts, e.Source, e.ExtID, wkt, e.Lon, e.Lat, string(raw)).Scan(&ok); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return 0, err
		}
		if ok {
			inserted = append(inserted, e)
		}
	}
	if err := upsertEventH3Bins(ctx, tx, inserted, 4); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(inserted), nil
}
