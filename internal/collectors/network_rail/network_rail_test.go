// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package network_rail

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

func TestReadFrame(t *testing.T) {
	raw := []byte("MESSAGE\ndestination:/topic/TRAIN_MVT_ALL_TOC\n\n[]\x00")
	cmd, headers, body, err := readFrame(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "MESSAGE" || headers["destination"] == "" || string(body) != "[]" {
		t.Fatalf("frame=%s %#v %s", cmd, headers, string(body))
	}
}

func TestEventsFromMessage(t *testing.T) {
	body := []byte(`[{
	  "header": {"msg_type": "0003"},
	  "body": {
	    "train_id": "123A45",
	    "loc_stanox": "87701",
	    "actual_timestamp": "1779105600000",
	    "event_type": "ARRIVAL",
	    "variation_status": "ON TIME",
	    "toc_id": "ZZ"
	  }
	}]`)
	evs := eventsFromMessage(map[string]string{"destination": "/topic/TRAIN_MVT_ALL_TOC"}, body, time.Now().UTC())
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
	if evs[0].Source != sourceID || evs[0].ExtID != "123A45:87701" {
		t.Fatalf("identity=%s/%s", evs[0].Source, evs[0].ExtID)
	}
	if got := evs[0].Props["event_type"]; got != "ARRIVAL" {
		t.Fatalf("event_type=%v", got)
	}
}

func TestTrustTimeMilliseconds(t *testing.T) {
	got := trustTime("1779105600000")
	if got.Year() != 2026 || got.Month() != time.May {
		t.Fatalf("time=%s", got)
	}
}
