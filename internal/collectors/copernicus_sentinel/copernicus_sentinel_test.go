// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package copernicus_sentinel

import (
	"testing"
	"time"
)

func TestSARProductMetadata(t *testing.T) {
	mode, productType, pol := sarProductMetadata("S1A_IW_GRDH_1SDV_20260427T031932_20260427T031957_058862_0740A1_2D3A.SAFE")
	if mode != "IW" || productType != "GRDH" || pol != "DV" {
		t.Fatalf("metadata = %q/%q/%q, want IW/GRDH/DV", mode, productType, pol)
	}
	if fam := productFamily("S1A_IW_GRDH_1SDV_20260427T031932_20260427T031957_058862_0740A1_2D3A.SAFE"); fam != "SAR_GRD" {
		t.Fatalf("family = %q, want SAR_GRD", fam)
	}
}

func TestBestSARPairScoresCompatibleBaseline(t *testing.T) {
	currentStart := time.Date(2026, 4, 27, 3, 19, 32, 0, time.UTC)
	current := sarTestProduct("cur", "S1A_IW_GRDH_1SDV_20260427T031932_20260427T031957_058862_0740A1_2D3A.SAFE", currentStart)
	baseline := sarTestProduct("base", "S1A_IW_GRDH_1SDV_20260415T031932_20260415T031957_058687_073B51_9EC1.SAFE", currentStart.AddDate(0, 0, -12))
	otherPol := sarTestProduct("other", "S1A_IW_GRDH_1SSH_20260403T031932_20260403T031957_058512_0735EF_0001.SAFE", currentStart.AddDate(0, 0, -24))

	pair, ok := bestSARPair(current, []product{baseline, otherPol})
	if !ok {
		t.Fatal("expected compatible SAR pair")
	}
	if pair.Baseline.ID != "base" {
		t.Fatalf("baseline = %q, want base", pair.Baseline.ID)
	}
	if pair.CompatibleCount != 1 {
		t.Fatalf("compatible count = %d, want 1", pair.CompatibleCount)
	}
	if pair.Score <= 1.5 {
		t.Fatalf("score = %v, want > 1.5", pair.Score)
	}
}

func TestBaselineSARProductsExcludesEventDayProducts(t *testing.T) {
	start := time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC)
	rows := []product{
		sarTestProduct("current", "S1A_IW_GRDH_1SDV_20260427T031932_20260427T031957_058862_0740A1_2D3A.SAFE", start.Add(-2*time.Hour)),
		sarTestProduct("baseline", "S1A_IW_GRDH_1SDV_20260410T031932_20260410T031957_058614_0738A1_2D3A.SAFE", start.AddDate(0, 0, -17)),
	}
	got := baselineSARProducts(rows, start)
	if len(got) != 1 || got[0].ID != "baseline" {
		t.Fatalf("baselineSARProducts = %#v, want only baseline", got)
	}
}

func TestUniqueSARProductsByAcquisitionCollapsesCOGVariant(t *testing.T) {
	start := time.Date(2026, 4, 27, 0, 51, 50, 322503000, time.UTC)
	regular := sarTestProduct("regular", "S1A_IW_GRDH_1SDV_20260427T005150_20260427T005215_064258_0816E5_4479.SAFE", start)
	cog := sarTestProduct("cog", "S1A_IW_GRDH_1SDV_20260427T005150_20260427T005215_064258_0816E5_449E_COG.SAFE", start)
	older := sarTestProduct("older", "S1A_IW_GRDH_1SDV_20260419T125531_20260419T125556_064149_0812DA_3F7F_COG.SAFE", start.AddDate(0, 0, -8))

	got := uniqueSARProductsByAcquisition([]product{regular, cog, older})
	if len(got) != 2 {
		t.Fatalf("uniqueSARProductsByAcquisition len = %d, want 2", len(got))
	}
	if got[0].ID != "cog" {
		t.Fatalf("first product ID = %q, want preferred COG variant", got[0].ID)
	}
	if got[1].ID != "older" {
		t.Fatalf("second product ID = %q, want older acquisition retained", got[1].ID)
	}
}

func TestProductEventUsesSourceFootprintGeometry(t *testing.T) {
	start := time.Date(2026, 4, 27, 3, 19, 32, 0, time.UTC)
	p := sarTestProduct("cur", "S1A_IW_GRDH_1SDV_20260427T031932_20260427T031957_058862_0740A1_2D3A.SAFE", start)
	p.Footprint = "geography'POLYGON((30 29,31 29,31 30,30 30,30 29))'"
	ev, ok := productEvent("SENTINEL-1", p)
	if !ok {
		t.Fatal("expected product event")
	}
	if ev.Geom != "POLYGON((30 29,31 29,31 30,30 30,30 29))" {
		t.Fatalf("event geom = %q, want source footprint", ev.Geom)
	}
	if ev.Lat != 0 || ev.Lon != 0 {
		t.Fatalf("event lat/lon = %v/%v, want footprint-only event", ev.Lat, ev.Lon)
	}
	if ev.Props["product_footprint_wkt"] != "POLYGON((30 29,31 29,31 30,30 30,30 29))" {
		t.Fatalf("missing normalized footprint prop: %#v", ev.Props["product_footprint_wkt"])
	}
	if ev.Props["acquisition_confirmation"] != "global_catalog_product" {
		t.Fatalf("acquisition_confirmation = %#v", ev.Props["acquisition_confirmation"])
	}
}

func sarTestProduct(id, name string, start time.Time) product {
	var p product
	p.ID = id
	p.Name = name
	p.Online = true
	p.ContentDate.Start = start.Format(time.RFC3339Nano)
	p.ContentDate.End = start.Add(25 * time.Second).Format(time.RFC3339Nano)
	return p
}
