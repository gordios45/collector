// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package airline_safety

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveCollectors(t *testing.T) {
	if os.Getenv("LIVE_AIRLINE_SAFETY") != "1" {
		t.Skip("set LIVE_AIRLINE_SAFETY=1 to hit public upstreams")
	}
	collectors := []struct {
		name string
		new  func() (*Collector, error)
	}{
		{"iata_iosa_registry", NewIATAIOSARegistry},
		{"iata_issa_registry", NewIATAISSARegistry},
		{"eu_air_safety_list", NewEUAirSafetyList},
		{"faa_iasa", NewFAAIASA},
		{"icao_usoap", NewICAOUSOAP},
		{"faa_sdr", NewFAASDR},
		{"ntsb_aviation_accidents", NewNTSBAviationAccidents},
		{"easa_czib", NewEASACZIB},
		{"faa_flight_restrictions", NewFAAFlightRestrictions},
		{"dot_certificated_carriers", NewDOTCertificatedCarriers},
	}
	for _, tc := range collectors {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.new()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			evs, err := c.Fetch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(evs) == 0 {
				t.Fatal("no events")
			}
		})
	}
}
