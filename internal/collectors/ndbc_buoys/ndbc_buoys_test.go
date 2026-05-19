// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package ndbc_buoys

import "testing"

func TestParseLatestBuoyObservation(t *testing.T) {
	raw := `#YY  MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr  mo dy hr mn degT m/s  m/s m    sec sec degT hPa  degC degC degC nmi ft
2026 05 02 12 50 180  18.5 25.1 4.2  8   7   175  996.4 20.0 19.0 18.0 MM  MM`
	obs, ok := parseLatest(raw)
	if !ok {
		t.Fatal("parseLatest returned false")
	}
	if obs.TS.Year() != 2026 || obs.TS.Month() != 5 || obs.TS.Minute() != 50 {
		t.Fatalf("bad timestamp: %s", obs.TS)
	}
	if obs.GustMS != 25.1 || obs.WaveHeightM != 4.2 || obs.PressureHPA != 996.4 {
		t.Fatalf("bad observation: %#v", obs)
	}
}

func TestEventFromTextScoresMarineHazard(t *testing.T) {
	raw := `#YY  MM DD hh mm WDIR WSPD GST WVHT PRES
2026 05 02 12 50 180  18.5 25.1 4.2  996.4`
	ev, ok := eventFromText(stationCatalog["44065"], realtimeBase+"/44065.txt", raw)
	if !ok {
		t.Fatal("eventFromText returned false")
	}
	if ev.Source != "ndbc_buoys" || ev.ExtID == "" || ev.Lat == 0 || ev.Lon == 0 {
		t.Fatalf("bad event identity/geocode: %#v", ev)
	}
	if score, ok := ev.Props["wind_gust_score"].(float64); !ok || score <= 0 {
		t.Fatalf("wind_gust_score = %#v, want positive", ev.Props["wind_gust_score"])
	}
	if ev.Props["source_payload_validity"] == nil {
		t.Fatal("missing source_payload_validity")
	}
}
