// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// RIPE RIS Live collector — real-time BGP UPDATE telemetry for watched ASNs.
//
// The existing bgp_visibility collector is slow country-level state. RIS Live is
// faster but noisier. We subscribe to origin-AS filters for crisis watchlist
// networks, aggregate UPDATE/withdrawal bursts into one event per country per
// minute, and let downstream analysis reason over co-occurrence with OONI,
// Cloudflare, IODA, and reports.
package ripe_ris

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/actors"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
	"github.com/gordios45/collector/internal/sources"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

const wsURL = "wss://ris-live.ripe.net/v1/ws/?client=gordios"

var defaultASNMap = map[int]string{
	// Iran
	12880: "IR", 44244: "IR", 48159: "IR", 58224: "IR", 42337: "IR", 197207: "IR",
	// Israel
	1680: "IL", 8551: "IL", 12400: "IL", 9116: "IL",
	// Ukraine / Russia
	6849: "UA", 21219: "UA", 3255: "UA",
	12389: "RU", 8359: "RU", 3216: "RU",
	// Levant / conflict zones
	9051: "LB", 42003: "LB", 29256: "SY", 29386: "SY", 30873: "YE",
}

type Collector struct {
	sink      *sources.EventSink
	asnToCC   map[int]string
	minEvents int
}

func New(pool *pgxpool.Pool) *Collector {
	asns := defaultASNMap
	if env := strings.TrimSpace(os.Getenv("RIS_ASN_WATCHLIST")); env != "" {
		asns = parseASNWatchlist(env)
	}
	minEvents := 25
	if raw := strings.TrimSpace(os.Getenv("RIS_MIN_UPDATES")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			minEvents = n
		}
	}
	return &Collector{
		sink:      sources.NewEventSink(pool, "ripe_ris", 100, 2*time.Second),
		asnToCC:   asns,
		minEvents: minEvents,
	}
}

func (c *Collector) ID() string { return "ripe_ris" }

func (c *Collector) Run(ctx context.Context) error {
	if len(c.asnToCC) == 0 {
		return fmt.Errorf("RIS_ASN_WATCHLIST empty")
	}
	go c.sink.Run(ctx)

	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.once(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[ripe_ris] ws: %v; reconnect in %s", err, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	return nil
}

func (c *Collector) once(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(1 << 20)

	asns := make([]int, 0, len(c.asnToCC))
	for asn := range c.asnToCC {
		asns = append(asns, asn)
	}
	sort.Ints(asns)
	for _, asn := range asns {
		sub := map[string]any{
			"type": "ris_subscribe",
			"data": map[string]any{
				"type":          "UPDATE",
				"path":          fmt.Sprintf("%d$", asn),
				"socketOptions": map[string]any{"includeRaw": false, "acknowledge": false},
			},
		}
		b, _ := json.Marshal(sub)
		if err := ws.Write(ctx, websocket.MessageText, b); err != nil {
			return fmt.Errorf("subscribe %d: %w", asn, err)
		}
	}
	log.Printf("[ripe_ris] connected — %d watched origin ASNs", len(asns))

	aggs := map[string]*countryAgg{}
	flushT := time.NewTicker(time.Minute)
	defer flushT.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-flushT.C:
			c.flush(aggs)
			aggs = map[string]*countryAgg{}
		default:
		}
		_, data, err := ws.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		c.handle(data, aggs)
	}
}

type risEnvelope struct {
	Type string `json:"type"`
	Data struct {
		Timestamp     float64 `json:"timestamp"`
		ID            string  `json:"id"`
		Host          string  `json:"host"`
		Type          string  `json:"type"`
		PeerASN       string  `json:"peer_asn"`
		Path          []any   `json:"path"`
		Announcements []struct {
			Prefixes []string `json:"prefixes"`
		} `json:"announcements"`
		Withdrawals []string `json:"withdrawals"`
	} `json:"data"`
}

type countryAgg struct {
	country       string
	updates       int
	announcements int
	withdrawals   int
	asns          map[int]bool
	prefixes      []string
	lastTS        time.Time
}

func (c *Collector) handle(data []byte, aggs map[string]*countryAgg) {
	var msg risEnvelope
	if err := json.Unmarshal(data, &msg); err != nil || msg.Type != "ris_message" || msg.Data.Type != "UPDATE" {
		return
	}
	cc, asn, ok := c.countryForPath(msg.Data.Path)
	if !ok {
		return
	}
	a := aggs[cc]
	if a == nil {
		a = &countryAgg{country: cc, asns: map[int]bool{}}
		aggs[cc] = a
	}
	a.updates++
	a.asns[asn] = true
	for _, ann := range msg.Data.Announcements {
		a.announcements += len(ann.Prefixes)
		for _, p := range ann.Prefixes {
			if len(a.prefixes) < 12 {
				a.prefixes = append(a.prefixes, p)
			}
		}
	}
	a.withdrawals += len(msg.Data.Withdrawals)
	for _, p := range msg.Data.Withdrawals {
		if len(a.prefixes) < 12 {
			a.prefixes = append(a.prefixes, p)
		}
	}
	if msg.Data.Timestamp > 0 {
		sec := int64(msg.Data.Timestamp)
		nsec := int64((msg.Data.Timestamp - float64(sec)) * 1e9)
		a.lastTS = time.Unix(sec, nsec).UTC()
	} else {
		a.lastTS = time.Now().UTC()
	}
}

func (c *Collector) countryForPath(path []any) (string, int, bool) {
	for i := len(path) - 1; i >= 0; i-- {
		for _, asn := range flattenASN(path[i]) {
			if cc, ok := c.asnToCC[asn]; ok {
				return cc, asn, true
			}
		}
	}
	return "", 0, false
}

func flattenASN(v any) []int {
	switch x := v.(type) {
	case float64:
		return []int{int(x)}
	case []any:
		out := []int{}
		for _, child := range x {
			out = append(out, flattenASN(child)...)
		}
		return out
	default:
		return nil
	}
}

func (c *Collector) flush(aggs map[string]*countryAgg) {
	now := time.Now().UTC()
	for cc, a := range aggs {
		if a.updates < c.minEvents && a.withdrawals < 5 {
			continue
		}
		ll, ok := geo.Centroids[cc]
		if !ok {
			continue
		}
		asns := make([]int, 0, len(a.asns))
		for asn := range a.asns {
			asns = append(asns, asn)
		}
		sort.Ints(asns)
		ts := a.lastTS
		if ts.IsZero() {
			ts = now
		}
		ratio := 0.0
		totalPrefixes := a.announcements + a.withdrawals
		if totalPrefixes > 0 {
			ratio = float64(a.withdrawals) / float64(totalPrefixes)
		}
		props := actors.EnrichNetworkASNProps(map[string]any{
			"country":          cc,
			"updates":          a.updates,
			"announcements":    a.announcements,
			"withdrawals":      a.withdrawals,
			"withdrawal_ratio": ratio,
			"watched_asns":     asns,
			"sample_prefixes":  a.prefixes,
		}, cc, asns)
		c.sink.Push(context.Background(), events.Event{
			Ts:     ts,
			Source: "ripe_ris",
			ExtID:  fmt.Sprintf("%s_%d", cc, now.Truncate(time.Minute).Unix()),
			Lat:    ll.Lat,
			Lon:    ll.Lon,
			Props:  props,
		})
	}
}

func parseASNWatchlist(raw string) map[int]string {
	out := map[int]string{}
	for _, tok := range strings.Split(raw, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		parts := strings.Split(tok, ":")
		asnRaw := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(parts[0])), "AS")
		asn, err := strconv.Atoi(asnRaw)
		if err != nil || asn <= 0 {
			continue
		}
		cc := "ZZ"
		if len(parts) > 1 && len(strings.TrimSpace(parts[1])) == 2 {
			cc = strings.ToUpper(strings.TrimSpace(parts[1]))
		}
		out[asn] = cc
	}
	return out
}
