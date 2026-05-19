// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package cyber_threats

import "testing"

func TestIPFromURL(t *testing.T) {
	if got := ipFromURL("http://219.156.175.226:47011/bin.sh"); got != "219.156.175.226" {
		t.Fatalf("ipFromURL = %q", got)
	}
	if got := ipFromURL("https://example.com/payload"); got != "" {
		t.Fatalf("domain URL should not produce IP, got %q", got)
	}
}
