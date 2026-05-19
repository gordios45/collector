// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NVD CVE 2.0 API — per-CVE record fetcher with rate-limit pacing.
//
// Endpoint: https://services.nvd.nist.gov/rest/json/cves/2.0?cveId=CVE-XXXX-YYYY
// Rate limit (per https://nvd.nist.gov/developers/start-here):
//
//	no key   :  5 requests per 30s rolling window
//	with key : 50 requests per 30s rolling window
//
// We enforce a self-imposed inter-request delay safely under the limit.
//
// NIST has slowed enrichment of new CVEs since 2024 — many recent records
// have empty `metrics` and `weaknesses` blocks even when the CVE is real.
// That's the precise gap CISA Vulnrichment fills (see vulnrichment.go).
package cisa_kev

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const nvdAPIBase = "https://services.nvd.nist.gov/rest/json/cves/2.0"

// nvdRateDelay is the inter-request delay we enforce. Under both quotas
// it leaves comfortable headroom for retries and clock skew.
func nvdRateDelay(hasKey bool) time.Duration {
	if hasKey {
		return 700 * time.Millisecond // ~42 req / 30s
	}
	return 7 * time.Second // ~4.3 req / 30s
}

type nvdRecord struct {
	CVE          string    `json:"cve"`
	Published    string    `json:"published,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	VulnStatus   string    `json:"vuln_status,omitempty"`
	Description  string    `json:"description,omitempty"`
	CVSS31       *cvssData `json:"cvss31,omitempty"`
	CVSS40       *cvssData `json:"cvss40,omitempty"`
	Weaknesses   []string  `json:"weaknesses,omitempty"`
	References   []string  `json:"references,omitempty"`
	FetchedAt    time.Time `json:"fetched_at"`
	NotFound     bool      `json:"not_found,omitempty"`
}

type cvssData struct {
	BaseScore    float64 `json:"base_score"`
	BaseSeverity string  `json:"base_severity,omitempty"`
	VectorString string  `json:"vector,omitempty"`
}

// fetchNVD fetches a single CVE from NVD. Returns an nvdRecord with
// NotFound=true and no error for 404 (CVE doesn't exist or hasn't been
// published yet) so callers can cache the negative result.
func fetchNVD(ctx context.Context, client *http.Client, cve string, apiKey string) (nvdRecord, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, nvdAPIBase+"?cveId="+cve, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept", "application/json")
	if apiKey != "" {
		req.Header.Set("apiKey", apiKey)
	}
	r, err := client.Do(req)
	if err != nil {
		return nvdRecord{}, err
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNotFound {
		return nvdRecord{CVE: cve, NotFound: true, FetchedAt: time.Now().UTC()}, nil
	}
	if r.StatusCode == http.StatusForbidden || r.StatusCode == http.StatusTooManyRequests {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 200))
		return nvdRecord{}, fmt.Errorf("nvd %d (rate-limited?): %s", r.StatusCode, string(body))
	}
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 200))
		return nvdRecord{}, fmt.Errorf("nvd %d: %s", r.StatusCode, string(body))
	}
	var resp nvdResponse
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nvdRecord{}, fmt.Errorf("decode: %w", err)
	}
	if resp.TotalResults == 0 || len(resp.Vulnerabilities) == 0 {
		return nvdRecord{CVE: cve, NotFound: true, FetchedAt: time.Now().UTC()}, nil
	}
	return parseNVDRecord(resp.Vulnerabilities[0].CVE), nil
}

type nvdResponse struct {
	TotalResults    int                `json:"totalResults"`
	Vulnerabilities []nvdVulnerability `json:"vulnerabilities"`
}

type nvdVulnerability struct {
	CVE nvdCVE `json:"cve"`
}

type nvdCVE struct {
	ID           string         `json:"id"`
	Published    string         `json:"published"`
	LastModified string         `json:"lastModified"`
	VulnStatus   string         `json:"vulnStatus"`
	Descriptions []nvdLangText  `json:"descriptions"`
	Metrics      nvdMetrics     `json:"metrics"`
	Weaknesses   []nvdWeakness  `json:"weaknesses"`
	References   []nvdReference `json:"references"`
}

type nvdLangText struct {
	Lang  string `json:"lang"`
	Value string `json:"value"`
}

type nvdMetrics struct {
	V31 []nvdCVSSWrap `json:"cvssMetricV31"`
	V30 []nvdCVSSWrap `json:"cvssMetricV30"`
	V40 []nvdCVSSWrap `json:"cvssMetricV40"`
}

type nvdCVSSWrap struct {
	Source   string         `json:"source"`
	Type     string         `json:"type"`
	CVSSData nvdCVSSContent `json:"cvssData"`
}

type nvdCVSSContent struct {
	Version      string  `json:"version"`
	VectorString string  `json:"vectorString"`
	BaseScore    float64 `json:"baseScore"`
	BaseSeverity string  `json:"baseSeverity"`
}

type nvdWeakness struct {
	Source      string        `json:"source"`
	Type        string        `json:"type"`
	Description []nvdLangText `json:"description"`
}

type nvdReference struct {
	URL    string   `json:"url"`
	Source string   `json:"source"`
	Tags   []string `json:"tags"`
}

func parseNVDRecord(c nvdCVE) nvdRecord {
	rec := nvdRecord{
		CVE:          c.ID,
		Published:    c.Published,
		LastModified: c.LastModified,
		VulnStatus:   c.VulnStatus,
		FetchedAt:    time.Now().UTC(),
	}
	for _, d := range c.Descriptions {
		if strings.EqualFold(d.Lang, "en") {
			rec.Description = d.Value
			break
		}
	}
	if len(c.Metrics.V31) > 0 {
		m := c.Metrics.V31[0]
		rec.CVSS31 = &cvssData{
			BaseScore:    m.CVSSData.BaseScore,
			BaseSeverity: m.CVSSData.BaseSeverity,
			VectorString: m.CVSSData.VectorString,
		}
	} else if len(c.Metrics.V30) > 0 {
		m := c.Metrics.V30[0]
		rec.CVSS31 = &cvssData{
			BaseScore:    m.CVSSData.BaseScore,
			BaseSeverity: m.CVSSData.BaseSeverity,
			VectorString: m.CVSSData.VectorString,
		}
	}
	if len(c.Metrics.V40) > 0 {
		m := c.Metrics.V40[0]
		rec.CVSS40 = &cvssData{
			BaseScore:    m.CVSSData.BaseScore,
			BaseSeverity: m.CVSSData.BaseSeverity,
			VectorString: m.CVSSData.VectorString,
		}
	}
	for _, w := range c.Weaknesses {
		for _, d := range w.Description {
			if strings.HasPrefix(d.Value, "CWE-") {
				rec.Weaknesses = append(rec.Weaknesses, d.Value)
			}
		}
	}
	for _, r := range c.References {
		if r.URL != "" {
			rec.References = append(rec.References, r.URL)
		}
	}
	return rec
}
