// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package noaa_nwm

import (
	"strings"
	"testing"
	"time"
)

func TestProductURL(t *testing.T) {
	cycle := time.Date(2026, 4, 28, 20, 0, 0, 0, time.UTC)
	u := productURL(products[1], cycle)
	want := "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.20260428/short_range/nwm.t20z.short_range.channel_rt.f001.conus.nc"
	if u != want {
		t.Fatalf("got %s want %s", u, want)
	}
}

func TestMetadataFallback(t *testing.T) {
	ev := metadataFallback(time.Date(2026, 4, 28, 20, 34, 0, 0, time.UTC))
	if ev.Source != "noaa_nwm" || ev.ExtID != "nwm_product_catalog:2026042820" {
		t.Fatalf("bad event: %#v", ev)
	}
	if !strings.Contains(ev.Props["product_url"].(string), "products/nwm") {
		t.Fatalf("bad product url: %#v", ev.Props)
	}
}
