// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// CISA Vulnrichment — per-CVE JSON published at
//
//	https://github.com/cisagov/vulnrichment
//
// CISA's program to enrich CVE records that NIST hasn't analysed yet
// (since 2024 NIST has been ~6 weeks behind on CVE enrichment). The
// repo layout is `{year}/{thousands}xxx/CVE-{year}-{number}.json`,
// e.g. CVE-2024-7399 → 2024/7xxx/CVE-2024-7399.json. For 5-digit CVE
// numbers the prefix is the leading two digits, e.g. CVE-2024-12345
// → 2024/12xxx/CVE-2024-12345.json.
//
// We pull the raw JSON via raw.githubusercontent.com to avoid the
// repo-listing API and its low anonymous rate limit. Coverage is not
// universal — many CVEs aren't enriched yet — so 404 is a normal
// negative-cache result, not an error.
//
// The interesting payload is in `containers.adp[]` where the CISA-ADP
// provider attaches:
//   - SSVC decision points (Exploitation, Automatable, Technical Impact,
//     Mission Prevalence, Public Well-being Impact)
//   - CVSS v3.1 score CISA assigned (when NIST hadn't)
package cisa_kev

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const vulnrichmentRawBase = "https://raw.githubusercontent.com/cisagov/vulnrichment/develop/"

type vulnRecord struct {
	CVE        string            `json:"cve"`
	SSVC       map[string]string `json:"ssvc,omitempty"`      // Exploitation, Automatable, etc.
	SSVCRole   string            `json:"ssvc_role,omitempty"` // e.g. "CISA Coordinator"
	SSVCAt     string            `json:"ssvc_timestamp,omitempty"`
	CISACvss31 *cvssData         `json:"cisa_cvss31,omitempty"`  // CVSS31 from CISA-ADP container
	HasKEVTag  bool              `json:"cisa_kev_tag,omitempty"` // whether CISA-ADP tagged it as KEV in the file
	FetchedAt  time.Time         `json:"fetched_at"`
	NotFound   bool              `json:"not_found,omitempty"`
}

func vulnrichmentURL(cve string) (string, error) {
	// CVE-YYYY-NNNN[+]
	parts := strings.Split(cve, "-")
	if len(parts) != 3 || parts[0] != "CVE" {
		return "", fmt.Errorf("bad CVE id %q", cve)
	}
	year := parts[1]
	num := parts[2]
	if len(num) < 4 {
		return "", fmt.Errorf("bad CVE number %q", num)
	}
	// Prefix is everything except the last 3 digits; "7399" → "7", "12345" → "12".
	prefix := num[:len(num)-3]
	if _, err := strconv.Atoi(prefix); err != nil {
		return "", fmt.Errorf("non-numeric CVE prefix %q", prefix)
	}
	return vulnrichmentRawBase + year + "/" + prefix + "xxx/" + cve + ".json", nil
}

func fetchVulnrichment(ctx context.Context, client *http.Client, cve string) (vulnRecord, error) {
	u, err := vulnrichmentURL(cve)
	if err != nil {
		return vulnRecord{}, err
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept", "application/json")
	r, err := client.Do(req)
	if err != nil {
		return vulnRecord{}, err
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNotFound {
		return vulnRecord{CVE: cve, NotFound: true, FetchedAt: time.Now().UTC()}, nil
	}
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 200))
		return vulnRecord{}, fmt.Errorf("vulnrichment %d: %s", r.StatusCode, string(body))
	}
	var raw vulnRaw
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return vulnRecord{}, fmt.Errorf("decode: %w", err)
	}
	return parseVulnrichment(cve, raw), nil
}

// vulnRaw mirrors the subset of the CVE-Schema-5 envelope we read.
type vulnRaw struct {
	Containers struct {
		ADP []vulnADP `json:"adp"`
	} `json:"containers"`
}

type vulnADP struct {
	ProviderMetadata struct {
		ShortName string `json:"shortName"`
	} `json:"providerMetadata"`
	Metrics []vulnMetric `json:"metrics"`
}

type vulnMetric struct {
	Other   *vulnOther      `json:"other,omitempty"`
	CVSSV31 *nvdCVSSContent `json:"cvssV3_1,omitempty"`
}

type vulnOther struct {
	Type    string          `json:"type"`
	Content json.RawMessage `json:"content"`
}

type ssvcContent struct {
	Role      string              `json:"role"`
	Timestamp string              `json:"timestamp"`
	Options   []map[string]string `json:"options"`
}

func parseVulnrichment(cve string, raw vulnRaw) vulnRecord {
	rec := vulnRecord{CVE: cve, FetchedAt: time.Now().UTC()}
	for _, adp := range raw.Containers.ADP {
		if !strings.EqualFold(adp.ProviderMetadata.ShortName, "CISA-ADP") {
			continue
		}
		for _, m := range adp.Metrics {
			if m.CVSSV31 != nil && m.CVSSV31.BaseScore > 0 {
				rec.CISACvss31 = &cvssData{
					BaseScore:    m.CVSSV31.BaseScore,
					BaseSeverity: m.CVSSV31.BaseSeverity,
					VectorString: m.CVSSV31.VectorString,
				}
			}
			if m.Other == nil {
				continue
			}
			switch m.Other.Type {
			case "ssvc":
				var s ssvcContent
				if err := json.Unmarshal(m.Other.Content, &s); err == nil {
					rec.SSVCRole = s.Role
					rec.SSVCAt = s.Timestamp
					rec.SSVC = map[string]string{}
					for _, opt := range s.Options {
						for k, v := range opt {
							rec.SSVC[k] = v
						}
					}
				}
			case "kev":
				rec.HasKEVTag = true
			}
		}
	}
	return rec
}
