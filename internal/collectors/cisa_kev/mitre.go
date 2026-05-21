// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// MITRE CVE AWG CVE 5.0 API fallback. This supplements NVD when NIST is
// missing a CVE record or throttles enrichment requests.
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

const mitreAPIBase = "https://cveawg.mitre.org/api/cve/"

func fetchMITRECVE(ctx context.Context, client *http.Client, cve string) (nvdRecord, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, mitreAPIBase+cve, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept", "application/json")
	r, err := client.Do(req)
	if err != nil {
		return nvdRecord{}, err
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNotFound {
		return nvdRecord{CVE: cve, RecordSource: "mitre_cve_awg", NotFound: true, FetchedAt: time.Now().UTC()}, nil
	}
	if r.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 200))
		return nvdRecord{}, fmt.Errorf("mitre cve %d: %s", r.StatusCode, string(body))
	}
	var raw mitreRecord
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nvdRecord{}, err
	}
	return parseMITRERecord(raw, cve), nil
}

type mitreRecord struct {
	CVEMetadata struct {
		CVEID         string `json:"cveId"`
		DatePublished string `json:"datePublished"`
		DateUpdated   string `json:"dateUpdated"`
		State         string `json:"state"`
	} `json:"cveMetadata"`
	Containers struct {
		CNA mitreContainer   `json:"cna"`
		ADP []mitreContainer `json:"adp"`
	} `json:"containers"`
}

type mitreContainer struct {
	Descriptions []mitreLangText    `json:"descriptions"`
	Metrics      []mitreMetric      `json:"metrics"`
	ProblemTypes []mitreProblemType `json:"problemTypes"`
	References   []mitreReference   `json:"references"`
}

type mitreLangText struct {
	Lang  string `json:"lang"`
	Value string `json:"value"`
}

type mitreMetric struct {
	CVSS31 *mitreCVSS `json:"cvssV3_1"`
	CVSS30 *mitreCVSS `json:"cvssV3_0"`
	CVSS40 *mitreCVSS `json:"cvssV4_0"`
}

type mitreCVSS struct {
	BaseScore    float64 `json:"baseScore"`
	BaseSeverity string  `json:"baseSeverity"`
	VectorString string  `json:"vectorString"`
}

type mitreProblemType struct {
	Descriptions []struct {
		Lang        string `json:"lang"`
		CWEID       string `json:"cweId"`
		Description string `json:"description"`
	} `json:"descriptions"`
}

type mitreReference struct {
	URL string `json:"url"`
}

func parseMITRERecord(raw mitreRecord, fallbackCVE string) nvdRecord {
	cve := strings.TrimSpace(raw.CVEMetadata.CVEID)
	if cve == "" {
		cve = fallbackCVE
	}
	rec := nvdRecord{
		CVE:          cve,
		RecordSource: "mitre_cve_awg",
		Published:    raw.CVEMetadata.DatePublished,
		LastModified: raw.CVEMetadata.DateUpdated,
		VulnStatus:   raw.CVEMetadata.State,
		FetchedAt:    time.Now().UTC(),
	}
	containers := append([]mitreContainer{raw.Containers.CNA}, raw.Containers.ADP...)
	for _, c := range containers {
		if rec.Description == "" {
			rec.Description = firstEnglishDescription(c.Descriptions)
		}
		if rec.CVSS31 == nil || rec.CVSS40 == nil {
			attachMITRECVSS(&rec, c.Metrics)
		}
		rec.Weaknesses = append(rec.Weaknesses, mitreWeaknesses(c.ProblemTypes)...)
		for _, r := range c.References {
			if r.URL != "" {
				rec.References = append(rec.References, r.URL)
			}
		}
	}
	rec.Weaknesses = dedupeStrings(rec.Weaknesses)
	rec.References = dedupeStrings(rec.References)
	if rec.CVE == "" {
		rec.NotFound = true
	}
	return rec
}

func firstEnglishDescription(rows []mitreLangText) string {
	for _, row := range rows {
		if strings.EqualFold(row.Lang, "en") && strings.TrimSpace(row.Value) != "" {
			return row.Value
		}
	}
	for _, row := range rows {
		if strings.TrimSpace(row.Value) != "" {
			return row.Value
		}
	}
	return ""
}

func attachMITRECVSS(rec *nvdRecord, metrics []mitreMetric) {
	for _, m := range metrics {
		if rec.CVSS31 == nil {
			if m.CVSS31 != nil {
				rec.CVSS31 = &cvssData{BaseScore: m.CVSS31.BaseScore, BaseSeverity: m.CVSS31.BaseSeverity, VectorString: m.CVSS31.VectorString}
			} else if m.CVSS30 != nil {
				rec.CVSS31 = &cvssData{BaseScore: m.CVSS30.BaseScore, BaseSeverity: m.CVSS30.BaseSeverity, VectorString: m.CVSS30.VectorString}
			}
		}
		if rec.CVSS40 == nil && m.CVSS40 != nil {
			rec.CVSS40 = &cvssData{BaseScore: m.CVSS40.BaseScore, BaseSeverity: m.CVSS40.BaseSeverity, VectorString: m.CVSS40.VectorString}
		}
	}
}

func mitreWeaknesses(rows []mitreProblemType) []string {
	out := []string{}
	for _, row := range rows {
		for _, d := range row.Descriptions {
			if strings.HasPrefix(d.CWEID, "CWE-") {
				out = append(out, d.CWEID)
			} else if strings.HasPrefix(d.Description, "CWE-") {
				out = append(out, d.Description)
			}
		}
	}
	return out
}

func dedupeStrings(xs []string) []string {
	out := xs[:0]
	seen := map[string]bool{}
	for _, x := range xs {
		x = strings.TrimSpace(x)
		if x == "" || seen[x] {
			continue
		}
		seen[x] = true
		out = append(out, x)
	}
	return out
}
