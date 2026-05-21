// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package tor_exit_nodes

import "testing"

func TestParseExitListDedupesAndRejectsPrivate(t *testing.T) {
	body := []byte(`
# comment
1.1.1.1
192.168.1.1
1.1.1.1
8.8.8.8
`)
	ips := parseExitList(body, 10)
	if len(ips) != 2 {
		t.Fatalf("len(ips) = %d, want 2", len(ips))
	}
	if ips[0].String() != "1.1.1.1" || ips[1].String() != "8.8.8.8" {
		t.Fatalf("ips = %v", ips)
	}
}

func TestParseExitListHonorsLimit(t *testing.T) {
	ips := parseExitList([]byte("1.1.1.1\n8.8.8.8\n9.9.9.9\n"), 2)
	if len(ips) != 2 {
		t.Fatalf("len(ips) = %d, want 2", len(ips))
	}
}
