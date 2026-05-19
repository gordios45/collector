// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package actors

import "testing"

func TestCallsignPrefix(t *testing.T) {
	tests := map[string]string{
		"RCH157":   "RCH",
		"SHELL 71": "SHELL",
		"TUAF123":  "TUAF",
		"":         "",
	}
	for in, want := range tests {
		if got := CallsignPrefix(in); got != want {
			t.Fatalf("CallsignPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAviationRoleScore(t *testing.T) {
	if got := AviationRoleScore("Signals intelligence"); got <= AviationRoleScore("Strategic airlift") {
		t.Fatalf("SIGINT role should score above airlift")
	}
}

func TestMaritimeFlagAndVesselTypeProfiles(t *testing.T) {
	if cc, ok := MMSICountry("422123456"); !ok || cc != "IR" {
		t.Fatalf("MMSICountry = (%q, %v), want IR true", cc, ok)
	}
	if prof, ok := MaritimeFlagProfileFor("IRN"); !ok || prof.Score <= 0 {
		t.Fatalf("MaritimeFlagProfileFor(IRN) = %#v, %v", prof, ok)
	}
	if prof, ok := VesselTypeProfileFor("80"); !ok || prof.Label != "tanker" || prof.Score <= 0 {
		t.Fatalf("VesselTypeProfileFor(80) = %#v, %v", prof, ok)
	}
}
