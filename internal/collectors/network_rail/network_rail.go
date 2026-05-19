// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package network_rail ingests a bounded sample from Network Rail's open-data
// train movement STOMP topic when credentials are configured.
package network_rail

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const sourceID = "network_rail_train_movements"

type Collector struct {
	user        string
	pass        string
	addr        string
	topic       string
	maxMessages int
	readFor     time.Duration
}

func New() (*Collector, error) {
	user := strings.TrimSpace(os.Getenv("NETWORK_RAIL_USER"))
	pass := strings.TrimSpace(os.Getenv("NETWORK_RAIL_PASS"))
	if user == "" || pass == "" {
		return nil, fmt.Errorf("set NETWORK_RAIL_USER and NETWORK_RAIL_PASS")
	}
	addr := strings.TrimSpace(os.Getenv("NETWORK_RAIL_STOMP_ADDR"))
	if addr == "" {
		addr = "datafeeds.networkrail.co.uk:61618"
	}
	topic := strings.TrimSpace(os.Getenv("NETWORK_RAIL_TOPIC"))
	if topic == "" {
		topic = "/topic/TRAIN_MVT_ALL_TOC"
	}
	return &Collector{
		user:        user,
		pass:        pass,
		addr:        addr,
		topic:       topic,
		maxMessages: envInt("NETWORK_RAIL_MAX_MESSAGES", 100, 1, 1000),
		readFor:     time.Duration(envInt("NETWORK_RAIL_READ_SECONDS", 20, 5, 120)) * time.Second,
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	raw, err := tls.DialWithDialer(&dialer, "tcp", c.addr, &tls.Config{MinVersion: tls.VersionTLS12})
	if err != nil {
		return nil, err
	}
	defer raw.Close()
	reader := bufio.NewReader(raw)

	host := c.addr
	if h, _, err := net.SplitHostPort(c.addr); err == nil {
		host = h
	}
	if err := writeFrame(raw, "CONNECT", map[string]string{
		"accept-version": "1.1",
		"host":           host,
		"login":          c.user,
		"passcode":       c.pass,
		"heart-beat":     "0,0",
	}, nil); err != nil {
		return nil, err
	}
	if err := raw.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, err
	}
	cmd, _, _, err := readFrame(reader)
	if err != nil {
		return nil, err
	}
	if cmd != "CONNECTED" {
		return nil, fmt.Errorf("network rail stomp connect returned %s", cmd)
	}
	if err := writeFrame(raw, "SUBSCRIBE", map[string]string{
		"id":          "gordios-train-mvt",
		"destination": c.topic,
		"ack":         "auto",
	}, nil); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(c.readFor)
	out := []events.Event{}
	for len(out) < c.maxMessages && time.Now().Before(deadline) {
		if err := raw.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			break
		}
		cmd, headers, body, err := readFrame(reader)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				break
			}
			return out, err
		}
		if cmd != "MESSAGE" {
			continue
		}
		out = append(out, eventsFromMessage(headers, body, time.Now().UTC())...)
	}
	_ = writeFrame(raw, "DISCONNECT", map[string]string{"receipt": "bye"}, nil)
	return out, nil
}

func writeFrame(conn net.Conn, command string, headers map[string]string, body []byte) error {
	var b strings.Builder
	b.WriteString(command)
	b.WriteByte('\n')
	for k, v := range headers {
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(v)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if _, err := conn.Write([]byte(b.String())); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			return err
		}
	}
	_, err := conn.Write([]byte{0})
	return err
}

func readFrame(r *bufio.Reader) (string, map[string]string, []byte, error) {
	raw, err := r.ReadBytes(0)
	if err != nil {
		return "", nil, nil, err
	}
	raw = raw[:len(raw)-1]
	parts := strings.SplitN(string(raw), "\n\n", 2)
	head := strings.TrimRight(parts[0], "\n")
	lines := strings.Split(head, "\n")
	if len(lines) == 0 {
		return "", nil, nil, fmt.Errorf("empty stomp frame")
	}
	headers := map[string]string{}
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if ok {
			headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	body := []byte{}
	if len(parts) == 2 {
		body = []byte(parts[1])
	}
	return strings.TrimSpace(lines[0]), headers, body, nil
}

type movementEnvelope struct {
	Header map[string]any `json:"header"`
	Body   map[string]any `json:"body"`
}

func eventsFromMessage(headers map[string]string, body []byte, now time.Time) []events.Event {
	var rows []movementEnvelope
	if err := json.Unmarshal(body, &rows); err != nil {
		var one movementEnvelope
		if err := json.Unmarshal(body, &one); err != nil {
			return nil
		}
		rows = []movementEnvelope{one}
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		ev, ok := eventFromMovement(headers, row, now)
		if ok {
			out = append(out, ev)
		}
	}
	return out
}

func eventFromMovement(headers map[string]string, row movementEnvelope, now time.Time) (events.Event, bool) {
	body := row.Body
	if len(body) == 0 {
		return events.Event{}, false
	}
	msgType := text(firstNonNil(row.Header["msg_type"], body["msg_type"]))
	trainID := text(firstNonNil(body["train_id"], body["train_id_current"], body["train_id_original"]))
	loc := text(firstNonNil(body["loc_stanox"], body["reporting_stanox"], body["planned_timestamp"]))
	ts := trustTime(firstNonNil(body["actual_timestamp"], body["gbtt_timestamp"], body["planned_timestamp"]))
	if ts.IsZero() {
		ts = now
	}
	props := map[string]any{
		"source_provider":   "Network Rail Open Data",
		"message_type":      msgType,
		"train_id":          trainID,
		"loc_stanox":        text(body["loc_stanox"]),
		"planned_timestamp": text(body["planned_timestamp"]),
		"actual_timestamp":  text(body["actual_timestamp"]),
		"event_type":        text(body["event_type"]),
		"variation_status":  text(body["variation_status"]),
		"toc_id":            text(body["toc_id"]),
		"topic_destination": headers["destination"],
		"raw_header":        row.Header,
		"raw_body":          body,
		"source_payload_validity": map[string]any{
			"valid_start":    ts.Format(time.RFC3339),
			"valid_end":      ts.Add(10 * time.Minute).Format(time.RFC3339),
			"validity_basis": "network_rail_train_movement_message",
		},
	}
	id := firstNonEmpty(trainID, msgType, stableID(stringish(body))) + ":" + firstNonEmpty(loc, stableID(ts.Format(time.RFC3339)))
	return events.Event{
		Ts:     ts,
		Source: sourceID,
		ExtID:  id,
		Props:  props,
	}, true
}

func trustTime(v any) time.Time {
	raw := text(v)
	if raw == "" {
		return time.Time{}
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n > 999999999999 {
			return time.UnixMilli(n).UTC()
		}
		if n > 999999999 {
			return time.Unix(n, 0).UTC()
		}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC()
		}
	}
	return time.Time{}
}

func text(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func firstNonNil(xs ...any) any {
	for _, x := range xs {
		if strings.TrimSpace(fmt.Sprint(x)) != "" && x != nil {
			return x
		}
	}
	return nil
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func stringish(v any) string {
	buf, _ := json.Marshal(v)
	return string(buf)
}

func stableID(s string) string {
	h := uint32(2166136261)
	for _, b := range []byte(strings.ToLower(s)) {
		h ^= uint32(b)
		h *= 16777619
	}
	return fmt.Sprintf("%08x", h)
}

func envInt(key string, def, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min {
		return def
	}
	if n > max {
		return max
	}
	return n
}
