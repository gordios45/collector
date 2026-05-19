// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Country centroids (ISO-3166 alpha-2 → lat/lon). Used by collectors that
// only get country codes (cyber_threats, ioda) so they still plot on the map.
// Only the set actually used in the wild is needed; add codes as collectors
// encounter them.
package geo

import "strings"

type LL struct{ Lat, Lon float64 }

var Centroids = map[string]LL{
	"US": {39.8, -98.6}, "RU": {55.8, 37.6}, "CN": {35.9, 104.2},
	"DE": {51.2, 10.4}, "FR": {46.2, 2.2}, "GB": {55.4, -3.4},
	"BR": {-14.2, -51.9}, "IN": {20.6, 79.0}, "JP": {36.2, 138.3},
	"KR": {35.9, 127.8}, "KP": {40.3, 127.5}, "AU": {-25.3, 133.8}, "CA": {56.1, -106.3},
	"IR": {32.4, 53.7}, "EG": {26.8, 30.8}, "SA": {23.9, 45.1},
	"IQ": {33.2, 43.7}, "PK": {30.4, 69.3}, "BD": {23.7, 90.4},
	"NG": {9.1, 8.7}, "ZA": {-30.6, 22.9}, "MX": {23.6, -102.6},
	"TR": {39.0, 35.2}, "UA": {48.4, 31.2}, "ID": {-0.8, 113.9},
	"TH": {15.9, 100.5}, "MM": {21.9, 95.9}, "SY": {34.8, 38.9},
	"YE": {15.6, 48.5}, "SD": {12.9, 30.2}, "ET": {9.1, 40.5},
	"CU": {21.5, -79.0}, "VE": {6.4, -66.6}, "VN": {14.1, 108.3},
	"PH": {12.9, 121.8}, "AF": {33.9, 67.7}, "LY": {26.3, 17.2},
	"KE": {-0.02, 37.9}, "TZ": {-6.4, 34.9}, "CO": {4.6, -74.3},
	"AR": {-38.4, -63.6}, "PE": {-9.2, -75.0}, "CL": {-35.7, -71.5},
	"IT": {41.9, 12.6}, "ES": {40.5, -3.7}, "PL": {51.9, 19.1},
	"NL": {52.1, 5.3}, "BE": {50.5, 4.5}, "SE": {60.1, 18.6},
	"NO": {60.5, 8.5}, "FI": {61.9, 25.7}, "DK": {56.3, 9.5},
	"CZ": {49.8, 15.5}, "AT": {47.5, 14.6}, "CH": {46.8, 8.2},
	"RO": {45.9, 24.9}, "HU": {47.2, 19.5}, "GR": {39.1, 21.8},
	"PT": {39.4, -8.2}, "IE": {53.4, -8.2}, "BG": {42.7, 25.5},
	"HR": {45.1, 15.2}, "RS": {44.0, 21.0}, "IL": {31.0, 34.8},
	"JO": {30.6, 36.2}, "LB": {33.9, 35.5}, "QA": {25.4, 51.2},
	"AE": {23.4, 53.8}, "KW": {29.3, 47.5}, "OM": {21.5, 55.9},
	"BH": {26.0, 50.6}, "UZ": {41.4, 64.6}, "KZ": {48.0, 66.9},
	"AZ": {40.1, 47.6}, "GE": {42.3, 43.4}, "AM": {40.1, 45.0},
	"SG": {1.35, 103.8}, "MY": {4.2, 101.9}, "TW": {23.7, 121.0},
	"HK": {22.4, 114.1}, "NZ": {-40.9, 174.9}, "MA": {31.8, -7.1},
	"DZ": {28.0, 1.7}, "TN": {33.9, 9.5},
}

func SplitCountryCodes(s string) []string {
	out := []string{}
	for _, raw := range strings.Split(s, ",") {
		if cc := strings.TrimSpace(strings.ToUpper(raw)); len(cc) == 2 {
			out = append(out, cc)
		}
	}
	return out
}
