// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package internetdb_exposure

import "testing"

func TestParseTargetsRejectsCIDRsAndPrivateIPs(t *testing.T) {
	t.Setenv("INTERNETDB_MAX_TARGETS", "")

	if _, err := parseTargets("1.1.1.1/32"); err == nil {
		t.Fatal("expected CIDR target to be rejected")
	}
	if _, err := parseTargets("192.168.1.1"); err == nil {
		t.Fatal("expected private target to be rejected")
	}
	if _, err := parseTargets("203.0.113.10"); err == nil {
		t.Fatal("expected documentation-range target to be rejected")
	}
	targets, err := parseTargets("1.1.1.1,8.8.8.8,1.1.1.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("len(targets) = %d, want 2", len(targets))
	}
}

func TestPropsFromInternetDBClassifiesRisk(t *testing.T) {
	props := propsFromInternetDB(internetDBRecord{
		IP:        "203.0.113.10",
		Hostnames: []string{"host.example"},
		Ports:     []int{443, 23},
		Vulns:     []string{"CVE-2024-0001"},
	}, ipGeo{Status: "success", CountryCode: "US", Country: "United States", Lat: 38.9, Lon: -77})

	if props["device_type"] != "web_server" {
		t.Fatalf("device_type = %v", props["device_type"])
	}
	if props["risk_level"] != "high" {
		t.Fatalf("risk_level = %v", props["risk_level"])
	}
	if props["country"] != "US" {
		t.Fatalf("country = %v", props["country"])
	}
}
