// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package oil_prices

import "testing"

func TestMarketStressScoreAndAssetClass(t *testing.T) {
	if got := assetClass("usdils"); got != "fx" {
		t.Fatalf("assetClass = %q, want fx", got)
	}
	if got := assetClass("gc.f"); got != "precious_metals" {
		t.Fatalf("assetClass = %q, want precious_metals", got)
	}
	if score := marketStressScore("cl.f", -5, 106, 99, 102); score <= 2 {
		t.Fatalf("market stress score = %.2f, want > 2", score)
	}
}
