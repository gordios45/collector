// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Copernicus Sentinel catalogue collector — global EO availability.
//
// This collector does not download imagery. It ingests recent global catalogue
// products and preserves source footprints in props/geometry.
package copernicus_sentinel

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
)

const endpoint = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

type Collector struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, fmt.Errorf("nil db pool")
	}
	return &Collector{pool: pool}, nil
}

func (c *Collector) ID() string               { return "copernicus_sentinel" }
func (c *Collector) PollEvery() time.Duration { return 1 * time.Hour }

type productsResp struct {
	Value []product `json:"value"`
}

type product struct {
	ID              string `json:"Id"`
	Name            string `json:"Name"`
	ContentLength   int64  `json:"ContentLength"`
	PublicationDate string `json:"PublicationDate"`
	Online          bool   `json:"Online"`
	S3Path          string `json:"S3Path"`
	Footprint       string `json:"Footprint"`
	ContentDate     struct {
		Start string `json:"Start"`
		End   string `json:"End"`
	} `json:"ContentDate"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	since := time.Now().UTC().Add(-24 * time.Hour)
	out := make([]events.Event, 0, 100)
	var lastErr error
	for _, collection := range []string{"SENTINEL-1", "SENTINEL-2"} {
		products, err := queryRecentProducts(ctx, collection, since, 50)
		if err != nil {
			lastErr = err
			continue
		}
		for _, p := range products {
			ev, ok := productEvent(collection, p)
			if ok {
				out = append(out, ev)
			}
		}
	}
	if len(out) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return out, nil
}

func queryRecentProducts(ctx context.Context, collection string, since time.Time, top int) ([]product, error) {
	if top <= 0 {
		top = 50
	}
	sinceStr := since.UTC().Format("2006-01-02T15:04:05.000Z")
	filter := fmt.Sprintf(
		"Collection/Name eq '%s' and ContentDate/Start gt %s",
		collection, sinceStr,
	)
	v := url.Values{}
	v.Set("$top", fmt.Sprintf("%d", top))
	v.Set("$orderby", "ContentDate/Start desc")
	v.Set("$filter", filter)
	var r productsResp
	if err := httpx.GetJSON(ctx, endpoint+"?"+v.Encode(), nil, &r); err != nil {
		return nil, err
	}
	return r.Value, nil
}

func productEvent(collection string, p product) (events.Event, bool) {
	if p.ID == "" || p.Name == "" {
		return events.Event{}, false
	}
	geom := ewktFromFootprint(p.Footprint)
	if geom == "" {
		return events.Event{}, false
	}
	ts, ok := productStart(p)
	if !ok {
		ts = time.Now().UTC()
	}
	mode, productType, polarization := sarProductMetadata(p.Name)
	family := productFamily(p.Name)
	kind := acquisitionKind(collection, family)
	acquisitionScore := actualAcquisitionScore(kind, 0, p.Online)
	props := map[string]any{
		"collection":                  collection,
		"product_id":                  p.ID,
		"product_name":                p.Name,
		"publication_date":            p.PublicationDate,
		"online":                      p.Online,
		"content_length":              p.ContentLength,
		"s3_path":                     p.S3Path,
		"content_start":               p.ContentDate.Start,
		"content_end":                 p.ContentDate.End,
		"platform":                    platformFromName(p.Name),
		"product_family":              family,
		"product_footprint_wkt":       geom,
		"acquisition_source":          "copernicus_sentinel",
		"acquisition_kind":            kind,
		"acquisition_confirmation":    "global_catalog_product",
		"satellite_acquisition_score": acquisitionScore,
	}
	if mode != "" {
		props["instrument_mode"] = mode
	}
	if productType != "" {
		props["sar_product_type"] = productType
	}
	if polarization != "" {
		props["sar_polarization"] = polarization
	}
	ev := events.Event{
		Ts:     ts,
		Source: "copernicus_sentinel",
		ExtID:  p.ID,
		Geom:   geom,
		Props:  props,
	}
	return ev, true
}

func recentProducts(rows []product, since time.Time, limit int) []product {
	out := make([]product, 0, limit)
	for _, p := range rows {
		ts, ok := productStart(p)
		if !ok || ts.Before(since) {
			continue
		}
		out = append(out, p)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func uniqueSARProductsByAcquisition(rows []product) []product {
	out := make([]product, 0, len(rows))
	seen := map[string]int{}
	for _, p := range rows {
		if productFamily(p.Name) != "SAR_GRD" {
			out = append(out, p)
			continue
		}
		key := sarAcquisitionKey(p)
		if key == "" {
			key = p.ID
		}
		if i, ok := seen[key]; ok {
			out[i] = preferredSARProduct(out[i], p)
			continue
		}
		seen[key] = len(out)
		out = append(out, p)
	}
	return out
}

func baselineSARProducts(rows []product, acquisitionStart time.Time) []product {
	cutoff := acquisitionStart.Add(-24 * time.Hour)
	out := []product{}
	for _, p := range rows {
		if productFamily(p.Name) != "SAR_GRD" {
			continue
		}
		ts, ok := productStart(p)
		if !ok || !ts.Before(cutoff) {
			continue
		}
		out = append(out, p)
	}
	return out
}

type sarPair struct {
	Baseline        product
	GapDays         float64
	CompatibleCount int
	Score           float64
}

func bestSARPair(current product, baselines []product) (sarPair, bool) {
	if productFamily(current.Name) != "SAR_GRD" {
		return sarPair{}, false
	}
	currentStart, ok := productStart(current)
	if !ok {
		return sarPair{}, false
	}
	curMode, curType, curPol := sarProductMetadata(current.Name)
	var best sarPair
	for _, b := range baselines {
		if b.ID == "" || b.ID == current.ID || productFamily(b.Name) != "SAR_GRD" {
			continue
		}
		baseStart, ok := productStart(b)
		if !ok || !baseStart.Before(currentStart) {
			continue
		}
		gapDays := currentStart.Sub(baseStart).Hours() / 24.0
		if gapDays < 3 || gapDays > 45 {
			continue
		}
		baseMode, baseType, basePol := sarProductMetadata(b.Name)
		if curMode != "" && baseMode != "" && curMode != baseMode {
			continue
		}
		if curType != "" && baseType != "" && curType != baseType {
			continue
		}
		if curPol != "" && basePol != "" && curPol != basePol {
			continue
		}
		best.CompatibleCount++
		score := sarPairScore(current, b, gapDays)
		if score > best.Score {
			best.Baseline = b
			best.GapDays = gapDays
			best.Score = score
		}
	}
	if best.CompatibleCount == 0 {
		return sarPair{}, false
	}
	best.Score = clampScore(best.Score + sarBaselineDepthBoost(best.CompatibleCount))
	return best, true
}

func sarPairScore(current, baseline product, gapDays float64) float64 {
	score := 0.45
	curMode, curType, curPol := sarProductMetadata(current.Name)
	baseMode, baseType, basePol := sarProductMetadata(baseline.Name)
	if platformFromName(current.Name) == platformFromName(baseline.Name) {
		score += 0.25
	}
	if curMode != "" && curMode == baseMode {
		score += 0.35
	}
	if curType != "" && curType == baseType {
		score += 0.25
	}
	if curPol != "" && curPol == basePol {
		score += 0.25
	}
	switch {
	case gapDays <= 15:
		score += 0.35
	case gapDays <= 30:
		score += 0.25
	default:
		score += 0.10
	}
	if current.Online && baseline.Online {
		score += 0.10
	}
	return clampScore(score)
}

func sarBaselineDepthBoost(n int) float64 {
	switch {
	case n >= 3:
		return 0.30
	case n == 2:
		return 0.20
	case n == 1:
		return 0.10
	default:
		return 0
	}
}

func attachSARPairProps(props map[string]any, pair sarPair) {
	mode, productType, polarization := sarProductMetadata(pair.Baseline.Name)
	currentPlatform, _ := props["platform"].(string)
	baselinePlatform := platformFromName(pair.Baseline.Name)
	props["sar_repeat_pair"] = true
	props["sar_repeat_pair_score"] = pair.Score
	props["sar_change_ready_score"] = pair.Score
	props["sar_pair_gap_days"] = pair.GapDays
	props["sar_baseline_product_count"] = pair.CompatibleCount
	props["baseline_depth_score"] = baselineDepthScore(pair.CompatibleCount)
	props["baseline_product_id"] = pair.Baseline.ID
	props["baseline_product_name"] = pair.Baseline.Name
	props["baseline_content_start"] = pair.Baseline.ContentDate.Start
	props["baseline_content_end"] = pair.Baseline.ContentDate.End
	props["baseline_platform"] = baselinePlatform
	props["baseline_product_family"] = productFamily(pair.Baseline.Name)
	if currentPlatform != "" && currentPlatform == baselinePlatform {
		props["same_platform_repeat_score"] = pair.Score
	}
	if mode != "" {
		props["baseline_instrument_mode"] = mode
	}
	if productType != "" {
		props["baseline_sar_product_type"] = productType
	}
	if polarization != "" {
		props["baseline_sar_polarization"] = polarization
	}
}

func productStart(p product) (time.Time, bool) {
	ts, err := time.Parse(time.RFC3339Nano, p.ContentDate.Start)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func sarAcquisitionKey(p product) string {
	mode, productType, polarization := sarProductMetadata(p.Name)
	if mode == "" || productType == "" || polarization == "" {
		return ""
	}
	start := normalizedProductTime(p.ContentDate.Start)
	end := normalizedProductTime(p.ContentDate.End)
	nameStart, nameEnd, orbit := sarNameAcquisition(p.Name)
	if start == "" {
		start = nameStart
	}
	if end == "" {
		end = nameEnd
	}
	if start == "" || end == "" {
		return ""
	}
	return strings.Join([]string{
		platformFromName(p.Name),
		mode,
		productType,
		polarization,
		start,
		end,
		orbit,
	}, "|")
}

func preferredSARProduct(a, b product) product {
	if b.Online && !a.Online {
		return b
	}
	if a.Online && !b.Online {
		return a
	}
	aCOG := isCOGProduct(a.Name)
	bCOG := isCOGProduct(b.Name)
	if bCOG && !aCOG {
		return b
	}
	if aCOG && !bCOG {
		return a
	}
	if b.PublicationDate > a.PublicationDate {
		return b
	}
	return a
}

func normalizedProductTime(s string) string {
	ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return strings.TrimSpace(s)
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func sarNameAcquisition(name string) (start, end, orbit string) {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".SAFE")
	name = strings.TrimSuffix(name, "_COG")
	parts := strings.Split(name, "_")
	if len(parts) > 4 {
		start = parts[4]
	}
	if len(parts) > 5 {
		end = parts[5]
	}
	if len(parts) > 6 {
		orbit = parts[6]
	}
	return start, end, orbit
}

func isCOGProduct(name string) bool {
	return strings.HasSuffix(strings.TrimSuffix(strings.TrimSpace(name), ".SAFE"), "_COG")
}

var footprintRe = regexp.MustCompile(`^geography'([^']+)'$`)

func ewktFromFootprint(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if m := footprintRe.FindStringSubmatch(s); len(m) == 2 {
		return m[1]
	}
	return s
}

func platformFromName(name string) string {
	if len(name) >= 3 {
		return name[:3]
	}
	return name
}

func productFamily(name string) string {
	switch {
	case strings.Contains(name, "_GRD"):
		return "SAR_GRD"
	case strings.Contains(name, "_RAW"):
		return "SAR_RAW"
	case strings.Contains(name, "_OCN"):
		return "SAR_OCN"
	case strings.Contains(name, "MSIL2A"):
		return "OPTICAL_L2A"
	case strings.Contains(name, "MSIL1C"):
		return "OPTICAL_L1C"
	default:
		return "UNKNOWN"
	}
}

func acquisitionKind(collection, family string) string {
	switch {
	case strings.HasPrefix(family, "SAR_") || collection == "SENTINEL-1":
		return "sar"
	case strings.HasPrefix(family, "OPTICAL_") || collection == "SENTINEL-2":
		return "optical"
	default:
		return "unknown"
	}
}

func actualAcquisitionScore(kind string, alignmentScore float64, online bool) float64 {
	score := 0.45 + alignmentScore*0.45
	switch kind {
	case "sar":
		score += 0.35
	case "optical":
		score += 0.10
	}
	if online {
		score += 0.10
	}
	return clampScore(score)
}

func baselineDepthScore(n int) float64 {
	switch {
	case n >= 4:
		return 1.2
	case n == 3:
		return 1.0
	case n == 2:
		return 0.75
	case n == 1:
		return 0.45
	default:
		return 0
	}
}

func sarProductMetadata(name string) (mode, productType, polarization string) {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".SAFE")
	parts := strings.Split(name, "_")
	if len(parts) < 4 || !strings.HasPrefix(parts[0], "S1") {
		return "", "", ""
	}
	mode = parts[1]
	productType = parts[2]
	classPol := parts[3]
	if len(classPol) >= 2 {
		polarization = classPol[len(classPol)-2:]
	}
	return mode, productType, polarization
}

func clampScore(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 2.5 {
		return 2.5
	}
	return v
}
