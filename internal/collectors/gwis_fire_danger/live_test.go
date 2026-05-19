// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gwis_fire_danger

import (
	"bytes"
	"context"
	"image/png"
	"os"
	"testing"
	"time"

	"github.com/gordios45/collector/internal/httpx"
)

func TestLiveMapFetch(t *testing.T) {
	if os.Getenv("GORDIOS_LIVE_COLLECTOR_TESTS") != "1" {
		t.Skip("set GORDIOS_LIVE_COLLECTOR_TESTS=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	buf, err := httpx.GetBytesWithClient(ctx, httpClient, fireDangerMapURL(), map[string]string{"Accept": "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	img, err := png.Decode(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := sampleRGB(img, 38, 20); !ok {
		t.Fatal("expected sampleable GWIS map pixel")
	}
}
