// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package actors

import "testing"

func TestEnrichNetworkASNProps(t *testing.T) {
	props := EnrichNetworkASNProps(map[string]any{"country": "IR"}, "IR", []int{44244, 12880, 12880})

	if props["actor_scope"] != "asn_set" {
		t.Fatalf("actor_scope = %v", props["actor_scope"])
	}
	if got := props["actor_asn_count"]; got != 2 {
		t.Fatalf("actor_asn_count = %v, want 2", got)
	}
	if got := props["actor_state_linked_asn_count"]; got != 1 {
		t.Fatalf("actor_state_linked_asn_count = %v, want 1", got)
	}
	if got := props["actor_national_backbone_asn_count"]; got != 1 {
		t.Fatalf("actor_national_backbone_asn_count = %v, want 1", got)
	}
	if score, ok := props["actor_network_criticality_score"].(float64); !ok || score <= 0 {
		t.Fatalf("actor_network_criticality_score = %v, want positive float64", props["actor_network_criticality_score"])
	}
}

func TestASNsFromAny(t *testing.T) {
	got := ASNsFromAny([]any{float64(12880), "AS44244", 0, "bad", float64(12880)})
	if len(got) != 2 || got[0] != 12880 || got[1] != 44244 {
		t.Fatalf("ASNsFromAny = %#v", got)
	}
}
