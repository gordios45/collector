// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package actors contains small public-data attribution tables used to turn
// raw technical identifiers into analyst-readable actor context.
package actors

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

type NetworkASN struct {
	ASN              int
	Country          string
	Org              string
	Category         string
	StateLinked      bool
	NationalBackbone bool
}

type CountryNetworkProfile struct {
	Country      string
	Control      string
	ControlScore float64
}

var asnActors = map[int]NetworkASN{
	// Iran
	12880:  {ASN: 12880, Country: "IR", Org: "Telecommunication Infrastructure Company", Category: "state_backbone", StateLinked: true, NationalBackbone: true},
	44244:  {ASN: 44244, Country: "IR", Org: "Irancell", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},
	48159:  {ASN: 48159, Country: "IR", Org: "Telecommunication Company of Iran", Category: "incumbent_telco", StateLinked: true, NationalBackbone: true},
	58224:  {ASN: 58224, Country: "IR", Org: "Iran Telecommunication Company", Category: "incumbent_telco", StateLinked: true, NationalBackbone: true},
	42337:  {ASN: 42337, Country: "IR", Org: "Respina Networks", Category: "isp", StateLinked: false, NationalBackbone: false},
	197207: {ASN: 197207, Country: "IR", Org: "Mobile Communications Company of Iran", Category: "mobile_operator", StateLinked: true, NationalBackbone: false},

	// Israel
	1680:  {ASN: 1680, Country: "IL", Org: "Bezeq International", Category: "incumbent_telco", StateLinked: false, NationalBackbone: true},
	8551:  {ASN: 8551, Country: "IL", Org: "Bezeq International", Category: "incumbent_telco", StateLinked: false, NationalBackbone: true},
	12400: {ASN: 12400, Country: "IL", Org: "Partner Communications", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},
	9116:  {ASN: 9116, Country: "IL", Org: "Cellcom Israel", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},

	// Ukraine / Russia
	6849:  {ASN: 6849, Country: "UA", Org: "Ukrtelecom", Category: "incumbent_telco", StateLinked: false, NationalBackbone: true},
	21219: {ASN: 21219, Country: "UA", Org: "Datagroup-Volia", Category: "isp", StateLinked: false, NationalBackbone: false},
	3255:  {ASN: 3255, Country: "UA", Org: "UARNET", Category: "research_network", StateLinked: false, NationalBackbone: false},
	12389: {ASN: 12389, Country: "RU", Org: "Rostelecom", Category: "state_linked_backbone", StateLinked: true, NationalBackbone: true},
	8359:  {ASN: 8359, Country: "RU", Org: "MTS", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},
	3216:  {ASN: 3216, Country: "RU", Org: "VimpelCom / Beeline", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},

	// Levant / conflict-zone watchlist
	9051:  {ASN: 9051, Country: "LB", Org: "IncoNet Data Management", Category: "isp", StateLinked: false, NationalBackbone: false},
	42003: {ASN: 42003, Country: "LB", Org: "Ogero", Category: "state_telco", StateLinked: true, NationalBackbone: true},
	29256: {ASN: 29256, Country: "SY", Org: "Syrian Telecommunications Establishment", Category: "state_telco", StateLinked: true, NationalBackbone: true},
	29386: {ASN: 29386, Country: "SY", Org: "Syriatel", Category: "mobile_operator", StateLinked: false, NationalBackbone: false},
	30873: {ASN: 30873, Country: "YE", Org: "YemenNet / Public Telecommunication Corporation", Category: "state_telco", StateLinked: true, NationalBackbone: true},
}

var countryProfiles = map[string]CountryNetworkProfile{
	"CN": {Country: "CN", Control: "state_controlled", ControlScore: 2.0},
	"IR": {Country: "IR", Control: "state_controlled", ControlScore: 2.0},
	"KP": {Country: "KP", Control: "state_controlled", ControlScore: 2.0},
	"MM": {Country: "MM", Control: "state_controlled", ControlScore: 1.6},
	"RU": {Country: "RU", Control: "state_influenced", ControlScore: 1.4},
	"SY": {Country: "SY", Control: "state_controlled", ControlScore: 1.8},
	"YE": {Country: "YE", Control: "fragile_state_network", ControlScore: 1.2},
	"SD": {Country: "SD", Control: "fragile_state_network", ControlScore: 1.2},
	"UA": {Country: "UA", Control: "wartime_disruption_context", ControlScore: 1.0},
	"LB": {Country: "LB", Control: "fragile_mixed_network", ControlScore: 0.9},
	"IL": {Country: "IL", Control: "high_criticality_network", ControlScore: 0.8},
	"IQ": {Country: "IQ", Control: "fragile_mixed_network", ControlScore: 0.8},
}

func LookupASN(asn int) (NetworkASN, bool) {
	info, ok := asnActors[asn]
	return info, ok
}

func CountryProfile(cc string) (CountryNetworkProfile, bool) {
	prof, ok := countryProfiles[strings.ToUpper(strings.TrimSpace(cc))]
	return prof, ok
}

func EnrichNetworkCountryProps(props map[string]any, country string) map[string]any {
	if props == nil {
		props = map[string]any{}
	}
	cc := strings.ToUpper(strings.TrimSpace(country))
	if cc == "" {
		cc = strings.ToUpper(strings.TrimSpace(stringAt(props, "country")))
	}
	if cc == "" {
		return props
	}
	props["actor_scope"] = "country_network"
	props["actor_country"] = cc
	if prof, ok := CountryProfile(cc); ok {
		props["actor_network_control"] = prof.Control
		props["actor_country_network_control_score"] = prof.ControlScore
	}
	return props
}

func EnrichNetworkASNProps(props map[string]any, country string, asns []int) map[string]any {
	props = EnrichNetworkCountryProps(props, country)
	asns = UniqueASNs(asns)
	if len(asns) == 0 {
		return props
	}

	rows := make([]map[string]any, 0, len(asns))
	orgs := make([]string, 0, len(asns))
	stateLinked := 0
	backbone := 0
	for _, asn := range asns {
		info, ok := LookupASN(asn)
		row := map[string]any{"asn": asn}
		if ok {
			row["country"] = info.Country
			row["org"] = info.Org
			row["category"] = info.Category
			row["state_linked"] = info.StateLinked
			row["national_backbone"] = info.NationalBackbone
			orgs = append(orgs, info.Org)
			if info.StateLinked {
				stateLinked++
			}
			if info.NationalBackbone {
				backbone++
			}
		}
		rows = append(rows, row)
	}
	props["actor_scope"] = "asn_set"
	props["actor_asns"] = rows
	props["actor_primary_orgs"] = uniqueStrings(orgs)
	props["actor_asn_count"] = len(asns)
	props["actor_state_linked_asn_count"] = stateLinked
	props["actor_national_backbone_asn_count"] = backbone
	props["actor_network_criticality_score"] = CriticalityScore(stateLinked, backbone, len(asns))
	return props
}

func CriticalityScore(stateLinked, backbone, total int) float64 {
	if total <= 0 {
		return 0
	}
	score := 0.7*math.Min(float64(stateLinked), 3) + 0.5*math.Min(float64(backbone), 3)
	if stateLinked > 0 && backbone > 0 {
		score += 0.4
	}
	return math.Min(score, 3)
}

func UniqueASNs(raw []int) []int {
	seen := map[int]struct{}{}
	out := make([]int, 0, len(raw))
	for _, asn := range raw {
		if asn <= 0 {
			continue
		}
		if _, ok := seen[asn]; ok {
			continue
		}
		seen[asn] = struct{}{}
		out = append(out, asn)
	}
	sort.Ints(out)
	return out
}

func ASNsFromAny(v any) []int {
	switch x := v.(type) {
	case []int:
		return UniqueASNs(x)
	case []any:
		out := make([]int, 0, len(x))
		for _, item := range x {
			out = append(out, asnFromScalar(item))
		}
		return UniqueASNs(out)
	case []float64:
		out := make([]int, 0, len(x))
		for _, item := range x {
			out = append(out, int(item))
		}
		return UniqueASNs(out)
	case []string:
		out := make([]int, 0, len(x))
		for _, item := range x {
			out = append(out, asnFromScalar(item))
		}
		return UniqueASNs(out)
	default:
		return nil
	}
}

func asnFromScalar(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		s := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(x)), "AS")
		n, _ := strconv.Atoi(s)
		return n
	default:
		return 0
	}
}

func uniqueStrings(xs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x == "" {
			continue
		}
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

func stringAt(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		return ""
	}
}
