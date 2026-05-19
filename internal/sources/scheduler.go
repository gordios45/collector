// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Scheduler: owns a set of collectors, fetches them on cadence, bulk-inserts
// into `events`, and NOTIFYs the gateway. Runs forever inside the ingester.
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

type Collector interface {
	ID() string
	PollEvery() time.Duration
	Fetch(ctx context.Context) ([]events.Event, error)
}

type FeatureCollector interface {
	ID() string
	PollEvery() time.Duration
	FetchFeatures(ctx context.Context) ([]features.Feature, error)
}

type Scheduler struct {
	pool        *pgxpool.Pool
	regs        []Collector
	featureRegs []FeatureCollector
	mu          sync.Mutex
}

const eventInsertChunkSize = 25

var (
	collectorTickSem = make(chan struct{}, 2)
	eventInsertSem   = make(chan struct{}, 1)
)

func acquireCollectorTickSlot(ctx context.Context) (func(), error) {
	select {
	case collectorTickSem <- struct{}{}:
		return func() { <-collectorTickSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func acquireEventInsertSlot(ctx context.Context) (func(), error) {
	select {
	case eventInsertSem <- struct{}{}:
		return func() { <-eventInsertSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func NewScheduler(pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{pool: pool}
}

func (s *Scheduler) Register(c Collector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regs = append(s.regs, c)
}

func (s *Scheduler) RegisterFeature(c FeatureCollector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.featureRegs = append(s.featureRegs, c)
}

// Run spawns one goroutine per registered collector and blocks until ctx done.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	regs := append([]Collector{}, s.regs...)
	featureRegs := append([]FeatureCollector{}, s.featureRegs...)
	s.mu.Unlock()
	if len(regs) == 0 && len(featureRegs) == 0 {
		log.Println("[scheduler] no collectors registered; idling")
		<-ctx.Done()
		return nil
	}

	var wg sync.WaitGroup
	for _, c := range regs {
		wg.Add(1)
		go func(c Collector) {
			defer wg.Done()
			s.loop(ctx, c)
		}(c)
	}
	for _, c := range featureRegs {
		wg.Add(1)
		go func(c FeatureCollector) {
			defer wg.Done()
			s.loopFeatures(ctx, c)
		}(c)
	}
	wg.Wait()
	return nil
}

func (s *Scheduler) loop(ctx context.Context, c Collector) {
	every := c.PollEvery()
	if every < time.Second {
		every = 30 * time.Second
	}
	// Spread the startup fetch burst across a wider deterministic window.
	// Without this, dozens of collectors fetch and mark source health at
	// once after a restart, which can overwhelm the local Timescale instance.
	time.Sleep(startupStagger(c.ID(), every))

	t := time.NewTicker(every)
	defer t.Stop()

	// Fire once immediately on start-up.
	s.tick(ctx, c)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx, c)
		}
	}
}

func (s *Scheduler) loopFeatures(ctx context.Context, c FeatureCollector) {
	every := c.PollEvery()
	if every < time.Second {
		every = 30 * time.Second
	}
	time.Sleep(startupStagger(c.ID(), every))

	t := time.NewTicker(every)
	defer t.Stop()

	s.tickFeatures(ctx, c)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tickFeatures(ctx, c)
		}
	}
}

func startupStagger(id string, every time.Duration) time.Duration {
	maxDelay := 90 * time.Second
	if every <= 5*time.Minute {
		maxDelay = 30 * time.Second
	}
	var sum int
	for i := 0; i < len(id); i++ {
		sum += int(id[i]) * (i + 1)
	}
	return time.Duration(sum%int(maxDelay/time.Second)) * time.Second
}

func (s *Scheduler) tick(ctx context.Context, c Collector) {
	releaseTick, err := acquireCollectorTickSlot(ctx)
	if err != nil {
		return
	}
	defer releaseTick()

	start := time.Now()
	fetchCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	evs, err := c.Fetch(fetchCtx)
	if err != nil {
		s.markErr(ctx, c.ID(), err)
		recordSourceRun(ctx, s.pool, c.ID(), start, time.Now(), false, 0, 0, 0, err)
		log.Printf("[%s] fetch error: %v", c.ID(), err)
		return
	}
	payloadBytes := estimatedEventPayloadBytes(evs)
	if len(evs) == 0 {
		finished := time.Now()
		if violation, err := evaluateSourceFreshnessContract(ctx, s.pool, c.ID(), finished); err != nil {
			log.Printf("[%s] freshness contract check: %v", c.ID(), err)
		} else if violation != nil {
			s.markErr(ctx, c.ID(), violation)
			recordSourceRun(ctx, s.pool, c.ID(), start, finished, false, 0, 0, 0, violation)
			log.Printf("[%s] fetch ok but freshness contract violated — 0 events in %s: %v", c.ID(), time.Since(start).Round(time.Millisecond), violation)
			return
		}
		s.markOK(ctx, c.ID(), 0)
		recordSourceRun(ctx, s.pool, c.ID(), start, finished, true, 0, 0, 0, nil)
		log.Printf("[%s] fetch ok — 0 events in %s", c.ID(), time.Since(start).Round(time.Millisecond))
		return
	}
	n, err := s.insert(ctx, evs)
	if err != nil {
		s.markErr(ctx, c.ID(), err)
		recordSourceRun(ctx, s.pool, c.ID(), start, time.Now(), false, len(evs), 0, payloadBytes, err)
		log.Printf("[%s] insert error: %v", c.ID(), err)
		return
	}
	finished := time.Now()
	if violation, err := evaluateSourceFreshnessContract(ctx, s.pool, c.ID(), finished); err != nil {
		log.Printf("[%s] freshness contract check: %v", c.ID(), err)
	} else if violation != nil {
		s.markErr(ctx, c.ID(), violation)
		recordSourceRun(ctx, s.pool, c.ID(), start, finished, false, len(evs), n, payloadBytes, violation)
		if n > 0 {
			s.notify(ctx, c.ID())
		}
		log.Printf("[%s] inserted %d rows but freshness contract violated in %s: %v", c.ID(), n, time.Since(start).Round(time.Millisecond), violation)
		return
	}
	s.markOK(ctx, c.ID(), n)
	recordSourceRun(ctx, s.pool, c.ID(), start, finished, true, len(evs), n, payloadBytes, nil)
	s.notify(ctx, c.ID())
	log.Printf("[%s] ok — %d rows in %s", c.ID(), n, time.Since(start).Round(time.Millisecond))
}

func (s *Scheduler) tickFeatures(ctx context.Context, c FeatureCollector) {
	releaseTick, err := acquireCollectorTickSlot(ctx)
	if err != nil {
		return
	}
	defer releaseTick()

	start := time.Now()
	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	feats, err := c.FetchFeatures(fetchCtx)
	if err != nil {
		s.markErr(ctx, c.ID(), err)
		recordSourceRun(ctx, s.pool, c.ID(), start, time.Now(), false, 0, 0, 0, err)
		log.Printf("[%s] feature fetch error: %v", c.ID(), err)
		return
	}
	payloadBytes := estimatedFeaturePayloadBytes(feats)
	n, err := features.Upsert(ctx, s.pool, c.ID(), feats)
	if err != nil {
		s.markErr(ctx, c.ID(), err)
		recordSourceRun(ctx, s.pool, c.ID(), start, time.Now(), false, len(feats), 0, payloadBytes, err)
		log.Printf("[%s] feature upsert error: %v", c.ID(), err)
		return
	}
	finished := time.Now()
	s.markOK(ctx, c.ID(), n)
	recordSourceRun(ctx, s.pool, c.ID(), start, finished, true, len(feats), n, payloadBytes, nil)
	s.notify(ctx, c.ID())
	log.Printf("[%s] feature ok — %d rows in %s", c.ID(), n, time.Since(start).Round(time.Millisecond))
}

// insert does a batched INSERT into events. Props is marshalled per row; the
// geometry comes from Lat/Lon (POINT) unless Geom was set explicitly (GeoJSON
// string passed to ST_GeomFromGeoJSON).
func (s *Scheduler) insert(ctx context.Context, evs []events.Event) (int, error) {
	const chunkSize = eventInsertChunkSize
	if len(evs) > chunkSize {
		total := 0
		for start := 0; start < len(evs); start += chunkSize {
			end := start + chunkSize
			if end > len(evs) {
				end = len(evs)
			}
			n, err := s.insert(ctx, evs[start:end])
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

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Insert-once on (source, ext_id, ts). Most collectors re-send immutable
	// observations; DO NOTHING avoids the heavier duplicate UPDATE path during
	// high-volume refreshes.
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

func (s *Scheduler) notify(ctx context.Context, src string) {
	_, err := s.pool.Exec(ctx, `SELECT pg_notify('events_changed', $1)`, src)
	if err != nil {
		log.Printf("[%s] notify: %v", src, err)
	}
}

func (s *Scheduler) markOK(ctx context.Context, src string, n int) {
	_, err := s.pool.Exec(ctx,
		`UPDATE sources SET last_fetch_at=now(), last_ok_at=now(), last_err=NULL WHERE id=$1`, src)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[%s] markOK: %v", src, err)
	}
	_ = n
}

func (s *Scheduler) markErr(ctx context.Context, src string, errIn error) {
	_, err := s.pool.Exec(ctx,
		`UPDATE sources SET last_fetch_at=now(), last_err=$2 WHERE id=$1`, src, errIn.Error())
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[%s] markErr: %v", src, err)
	}
}
