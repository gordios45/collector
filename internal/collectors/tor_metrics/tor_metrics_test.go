// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package tor_metrics

import (
	"testing"
	"time"
)

func TestParseSeriesSkipsComments(t *testing.T) {
	rows, err := parseSeries([]byte(`# header
date,country,users,lower,upper,frac
2026-04-20,ir,100,,,50
2026-04-21,ir,80,,,49
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Users != 100 || rows[1].Frac != 49 {
		t.Fatalf("parseSeries = %#v", rows)
	}
}

func TestScoreCountryDetectsBridgeSurgeAndDirectDrop(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	var direct, bridge []dailyUsers
	for i := 0; i < 8; i++ {
		direct = append(direct, dailyUsers{Date: base.AddDate(0, 0, i), Users: 1000})
		bridge = append(bridge, dailyUsers{Date: base.AddDate(0, 0, i), Users: 100})
	}
	direct = append(direct, dailyUsers{Date: base.AddDate(0, 0, 8), Users: 500})
	bridge = append(bridge, dailyUsers{Date: base.AddDate(0, 0, 8), Users: 220})

	s := scoreCountry(direct, bridge)
	if s.directDropScore <= 0 {
		t.Fatalf("missing direct drop score: %#v", s)
	}
	if s.bridgeSurgeScore <= 0 {
		t.Fatalf("missing bridge surge score: %#v", s)
	}
	if s.pressureScore <= 0 {
		t.Fatalf("missing pressure score: %#v", s)
	}
	if !s.material() {
		t.Fatalf("score should be material: %#v", s)
	}
}
