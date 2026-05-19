// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"math"
	"strconv"
	"strings"
)

type AviationCountryProfile struct {
	Country string
	Label   string
	Score   float64
}

type MaritimeFlagProfile struct {
	Flag  string
	Label string
	Score float64
}

type VesselTypeProfile struct {
	Label string
	Score float64
}

var aviationCountryProfiles = map[string]AviationCountryProfile{
	"CN": {Country: "CN", Label: "state/military aviation allocation", Score: 1.4},
	"IR": {Country: "IR", Label: "state aviation allocation", Score: 1.5},
	"KP": {Country: "KP", Label: "state aviation allocation", Score: 1.7},
	"RU": {Country: "RU", Label: "state/military aviation allocation", Score: 1.5},
	"SY": {Country: "SY", Label: "state aviation allocation", Score: 1.2},
	"UA": {Country: "UA", Label: "wartime aviation allocation", Score: 1.1},
	"IL": {Country: "IL", Label: "high-readiness aviation allocation", Score: 0.9},
	"US": {Country: "US", Label: "military aviation allocation", Score: 0.7},
	"GB": {Country: "GB", Label: "military aviation allocation", Score: 0.6},
	"FR": {Country: "FR", Label: "military aviation allocation", Score: 0.6},
	"DE": {Country: "DE", Label: "military aviation allocation", Score: 0.5},
	"TR": {Country: "TR", Label: "military aviation allocation", Score: 0.7},
}

var maritimeFlagProfiles = map[string]MaritimeFlagProfile{
	"IR": {Flag: "IR", Label: "Iran-flagged vessel", Score: 1.6},
	"KP": {Flag: "KP", Label: "North Korea-flagged vessel", Score: 1.8},
	"RU": {Flag: "RU", Label: "Russia-flagged vessel", Score: 1.3},
	"SY": {Flag: "SY", Label: "Syria-flagged vessel", Score: 1.2},
	"YE": {Flag: "YE", Label: "Yemen-flagged vessel", Score: 1.1},
	"CN": {Flag: "CN", Label: "China-flagged vessel", Score: 0.8},
}

var mmsiMIDCountry = map[string]string{
	"273": "RU",
	"412": "CN",
	"413": "CN",
	"414": "CN",
	"422": "IR",
	"445": "KP",
	"468": "SY",
	"473": "YE",
}

func CallsignPrefix(callsign string) string {
	s := strings.ToUpper(strings.TrimSpace(callsign))
	s = strings.ReplaceAll(s, " ", "")
	if s == "" {
		return ""
	}
	for i, r := range s {
		if r >= '0' && r <= '9' {
			return s[:i]
		}
	}
	return s
}

func AviationCountryProfileFor(cc string) (AviationCountryProfile, bool) {
	prof, ok := aviationCountryProfiles[strings.ToUpper(strings.TrimSpace(cc))]
	return prof, ok
}

func AviationCountryScore(cc string) float64 {
	if prof, ok := AviationCountryProfileFor(cc); ok {
		return prof.Score
	}
	return 0
}

func IsMilitaryOperator(text string) bool {
	s := strings.ToLower(strings.TrimSpace(text))
	if s == "" {
		return false
	}
	terms := []string{
		"air force",
		"navy",
		"army",
		"marine corps",
		"coast guard",
		"military",
		"defence",
		"defense",
		"luftwaffe",
		"royal air force",
		"nato",
	}
	for _, term := range terms {
		if strings.Contains(s, term) {
			return true
		}
	}
	return false
}

func AviationRoleScore(role string) float64 {
	s := strings.ToLower(strings.TrimSpace(role))
	if s == "" {
		return 0
	}
	switch {
	case strings.Contains(s, "signals intelligence"),
		strings.Contains(s, "surveillance"),
		strings.Contains(s, "reconnaissance"),
		strings.Contains(s, "early warning"):
		return 1.8
	case strings.Contains(s, "maritime patrol"),
		strings.Contains(s, "asw"),
		strings.Contains(s, "refueling"),
		strings.Contains(s, "fighter"),
		strings.Contains(s, "bombing"):
		return 1.4
	case strings.Contains(s, "vip"),
		strings.Contains(s, "transport"),
		strings.Contains(s, "airlift"):
		return 1.0
	default:
		return 0.8
	}
}

func AttributionScore(count int) float64 {
	if count <= 0 {
		return 0
	}
	return math.Min(math.Log1p(float64(count)), 3)
}

func MaritimeFlagProfileFor(flag string) (MaritimeFlagProfile, bool) {
	prof, ok := maritimeFlagProfiles[NormalizeCountryCode(flag)]
	return prof, ok
}

func MMSICountry(mmsi string) (string, bool) {
	s := strings.TrimSpace(mmsi)
	if len(s) < 3 {
		return "", false
	}
	cc, ok := mmsiMIDCountry[s[:3]]
	return cc, ok
}

func NormalizeCountryCode(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "IRN", "IRAN":
		return "IR"
	case "PRK", "KP", "NORTH KOREA", "DPRK":
		return "KP"
	case "RUS", "RUSSIA":
		return "RU"
	case "SYR", "SYRIA":
		return "SY"
	case "YEM", "YEMEN":
		return "YE"
	case "CHN", "CHINA":
		return "CN"
	}
	if len(s) == 2 {
		return s
	}
	return ""
}

func VesselTypeProfileFor(raw any) (VesselTypeProfile, bool) {
	var code int
	switch x := raw.(type) {
	case int:
		code = x
	case int64:
		code = int(x)
	case float64:
		code = int(x)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return VesselTypeProfile{}, false
		}
		code = n
	default:
		return VesselTypeProfile{}, false
	}
	switch {
	case code == 35:
		return VesselTypeProfile{Label: "military operations vessel", Score: 1.6}, true
	case code == 55:
		return VesselTypeProfile{Label: "law-enforcement vessel", Score: 1.2}, true
	case code >= 80 && code <= 89:
		return VesselTypeProfile{Label: "tanker", Score: 1.0}, true
	case code >= 70 && code <= 79:
		return VesselTypeProfile{Label: "cargo vessel", Score: 0.6}, true
	case code == 30:
		return VesselTypeProfile{Label: "fishing vessel", Score: 0.4}, true
	default:
		return VesselTypeProfile{}, false
	}
}
