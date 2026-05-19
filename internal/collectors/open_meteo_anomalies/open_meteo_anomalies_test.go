// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package open_meteo_anomalies

import "testing"

func TestSummarizeOpenMeteoAnomaly(t *testing.T) {
	var p meteoPayload
	for i := 0; i < 45; i++ {
		p.Daily.Time = append(p.Daily.Time, "2026-04-01")
		p.Daily.TemperatureMean = append(p.Daily.TemperatureMean, 25)
		p.Daily.PrecipitationSum = append(p.Daily.PrecipitationSum, 3)
		p.Daily.WindSpeed10mMax = append(p.Daily.WindSpeed10mMax, 20)
	}
	for i := 38; i < 45; i++ {
		p.Daily.TemperatureMean[i] = 32
		p.Daily.PrecipitationSum[i] = 0
	}
	m := summarize(p)
	if !m.OK || m.HeatStressScore <= 0 || m.DrynessScore <= 0 {
		t.Fatalf("unexpected metric: %#v", m)
	}
	if m.Severity == "normal" {
		t.Fatalf("wanted non-normal severity: %#v", m)
	}
}
