// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Lightning collector — Blitzortung real-time WebSocket feed.
//
// Blitzortung publishes every detected strike globally via wss://ws1..ws8.
// Each frame is a small JSON object: { time, lat, lon, pol, sig, ... }.
// Time is nanoseconds since epoch. Volume is noisy: 10–100 strikes/s
// during global thunderstorm activity.
//
// We batch strikes into 500 ms flushes and dedupe by (ts, lat, lon).
// Strikes live in the `events` hypertable; the 90-day retention policy
// (migration 001) keeps volume bounded automatically.
package lightning

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/sources"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Blitzortung's real-time feed ships each JSON frame through an LZW-style
// custom compression: literal chars are ASCII, dictionary references are
// encoded as code points ≥ 256 (multi-byte UTF-8). Canonical decoder —
// several public implementations match exactly, the ported Python/JS one
// below is the cleanest. Runs per-frame; tiny allocations.
func decodeBlitz(s string) string {
	runes := []rune(s)
	if len(runes) == 0 {
		return ""
	}
	dict := make(map[int]string, 512)
	c := string(runes[0])
	d := c
	var b strings.Builder
	b.Grow(len(runes) * 2)
	b.WriteString(c)
	code := 256
	for y := 1; y < len(runes); y++ {
		aCode := int(runes[y])
		var a string
		switch {
		case aCode < 256:
			a = string(runes[y])
		default:
			if v, ok := dict[aCode]; ok {
				a = v
			} else {
				a = d + c
			}
		}
		b.WriteString(a)
		c = string([]rune(a)[0])
		dict[code] = d + c
		code++
		d = a
	}
	return b.String()
}

// Only ws1 reliably presents a cert matching its hostname. ws3/ws5 serve
// certs for *.blitzortung.de. ws7 accepts connections but the feed goes
// idle — probably a different mirror role. Stick with ws1; cycle if it drops.
var hosts = []string{
	"wss://ws1.blitzortung.org/",
}

type Collector struct {
	pool *pgxpool.Pool
	sink *sources.EventSink
}

func New(pool *pgxpool.Pool) *Collector {
	return &Collector{
		pool: pool,
		sink: sources.NewEventSink(pool, "lightning", 256, 500*time.Millisecond),
	}
}

func (c *Collector) ID() string { return "lightning" }

func (c *Collector) Run(ctx context.Context) error {
	// Background flusher.
	go c.sink.Run(ctx)

	backoff := time.Second
	for ctx.Err() == nil {
		host := hosts[rand.Intn(len(hosts))]
		if err := c.once(ctx, host); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[lightning] ws %s: %v; reconnect in %s", host, err, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	return nil
}

func (c *Collector) once(ctx context.Context, host string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, host, &websocket.DialOptions{
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 * 1024)

	// Blitzortung's subscribe message — area code 111 = world.
	if err := ws.Write(ctx, websocket.MessageText, []byte(`{"a":111}`)); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Printf("[lightning] connected: %s", host)

	var frames, strikes int
	logT := time.NewTicker(30 * time.Second)
	defer logT.Stop()
	go func() {
		for range logT.C {
			log.Printf("[lightning] recv: %d frames / %d strikes buffered in last 30 s", frames, strikes)
			frames, strikes = 0, 0
		}
	}()

	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		frames++
		var d struct {
			Time *int64   `json:"time"`
			Lat  *float64 `json:"lat"`
			Lon  *float64 `json:"lon"`
			Pol  *int     `json:"pol"`
			// "sig" is the stations array in raw Blitzortung output; keep as
			// raw JSON so we don't misparse a list into a float.
			Sig json.RawMessage `json:"sig"`
		}
		decoded := decodeBlitz(string(data))
		if err := json.Unmarshal([]byte(decoded), &d); err != nil {
			if frames%100 == 1 {
				log.Printf("[lightning] parse err: %v; head=%q", err, truncate(decoded, 160))
			}
			continue
		}
		if d.Lat == nil || d.Lon == nil {
			if frames%100 == 1 {
				log.Printf("[lightning] no lat/lon; head=%q", truncate(decoded, 160))
			}
			continue
		}
		ts := time.Now().UTC()
		if d.Time != nil && *d.Time > 0 {
			// Blitzortung time is nanoseconds since epoch.
			ts = time.Unix(0, *d.Time).UTC()
		}
		// ext_id = ns + full-precision position. Keying on (source, ext_id, ts)
		// so two distinct strikes would need the same ns AND same lat/lon to
		// 6 decimals (~0.1 m) to collide — effectively never.
		ext := fmt.Sprintf("%d_%.6f_%.6f", ts.UnixNano(), *d.Lat, *d.Lon)

		props := map[string]any{}
		if d.Pol != nil {
			props["pol"] = *d.Pol
		}
		// sig is a station-list array; store count instead of the full list
		// to keep event rows small.
		if len(d.Sig) > 0 {
			var stations []json.RawMessage
			if json.Unmarshal(d.Sig, &stations) == nil {
				props["stations"] = len(stations)
			}
		}
		c.sink.Push(ctx, events.Event{
			Ts:     ts,
			Source: "lightning",
			ExtID:  ext,
			Lat:    *d.Lat,
			Lon:    *d.Lon,
			Props:  props,
		})
		strikes++
	}
}
