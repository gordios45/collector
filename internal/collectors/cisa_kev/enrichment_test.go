// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package cisa_kev

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseEPSSScoreDate(t *testing.T) {
	cases := map[string]string{
		"#model_version:v2025.03.14,score_date:2026-04-26T12:55:00Z\n": "2026-04-26",
		"#model_version:v1,score_date:2026-04-26\n":                    "2026-04-26",
		"":                                                             "",
		"# header without score_date\n":                                 "",
	}
	for in, want := range cases {
		got := parseEPSSScoreDate(in)
		if got != want {
			t.Errorf("parseEPSSScoreDate(%q): got %q want %q", in, got, want)
		}
	}
}

// Real subset of an NVD CVE 2.0 response, captured from
// https://services.nvd.nist.gov/rest/json/cves/2.0?cveId=CVE-2024-7399 on
// 2026-04-27. Confirms our parser pulls description, CVSS31 and refs.
const sampleNVD = `{
  "totalResults": 1,
  "vulnerabilities": [{
    "cve": {
      "id": "CVE-2024-7399",
      "published": "2024-08-12T13:38:41.550",
      "lastModified": "2026-04-24T20:23:57.990",
      "vulnStatus": "Analyzed",
      "descriptions": [
        {"lang": "en", "value": "Improper limitation of a pathname to a restricted directory vulnerability in Samsung MagicINFO 9 Server."},
        {"lang": "es", "value": "Una vulnerabilidad de limitación inadecuada..."}
      ],
      "metrics": {
        "cvssMetricV31": [{
          "source": "nvd@nist.gov",
          "type": "Primary",
          "cvssData": {
            "version": "3.1",
            "vectorString": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            "baseScore": 8.8,
            "baseSeverity": "HIGH"
          }
        }]
      },
      "weaknesses": [
        {"source": "nvd@nist.gov","type": "Primary","description": [{"lang": "en","value": "CWE-22"}]}
      ],
      "references": [
        {"url": "https://security.samsungtv.com/securityUpdates", "source": "samsung", "tags": ["Vendor Advisory"]}
      ]
    }
  }]
}`

func TestParseNVDRecord(t *testing.T) {
	var resp nvdResponse
	if err := json.Unmarshal([]byte(sampleNVD), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Vulnerabilities) != 1 {
		t.Fatalf("expected 1 vuln, got %d", len(resp.Vulnerabilities))
	}
	rec := parseNVDRecord(resp.Vulnerabilities[0].CVE)
	if rec.CVE != "CVE-2024-7399" {
		t.Errorf("cve: got %q", rec.CVE)
	}
	if !strings.HasPrefix(rec.Description, "Improper limitation") {
		t.Errorf("description: got %q", rec.Description)
	}
	if rec.CVSS31 == nil || rec.CVSS31.BaseScore != 8.8 {
		t.Errorf("cvss31: got %+v", rec.CVSS31)
	}
	if rec.CVSS31.BaseSeverity != "HIGH" {
		t.Errorf("severity: got %q", rec.CVSS31.BaseSeverity)
	}
	if len(rec.Weaknesses) != 1 || rec.Weaknesses[0] != "CWE-22" {
		t.Errorf("weaknesses: got %v", rec.Weaknesses)
	}
	if len(rec.References) != 1 {
		t.Errorf("references: got %v", rec.References)
	}
}

func TestVulnrichmentURL(t *testing.T) {
	cases := map[string]string{
		"CVE-2024-7399":  "https://raw.githubusercontent.com/cisagov/vulnrichment/develop/2024/7xxx/CVE-2024-7399.json",
		"CVE-2024-12345": "https://raw.githubusercontent.com/cisagov/vulnrichment/develop/2024/12xxx/CVE-2024-12345.json",
		"CVE-1999-0001":  "https://raw.githubusercontent.com/cisagov/vulnrichment/develop/1999/0xxx/CVE-1999-0001.json",
	}
	for cve, want := range cases {
		got, err := vulnrichmentURL(cve)
		if err != nil {
			t.Errorf("%s: %v", cve, err)
			continue
		}
		if got != want {
			t.Errorf("%s: got %q want %q", cve, got, want)
		}
	}
}

func TestVulnrichmentURL_BadInput(t *testing.T) {
	for _, bad := range []string{"NOT-A-CVE", "CVE-x", "", "CVE-2024-12"} {
		if _, err := vulnrichmentURL(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

// Real subset of CVE-2024-7399 vulnrichment file from
// https://raw.githubusercontent.com/cisagov/vulnrichment/develop/2024/7xxx/CVE-2024-7399.json.
const sampleVulnrichment = `{
  "dataType": "CVE_RECORD",
  "dataVersion": "5.1",
  "containers": {
    "adp": [{
      "providerMetadata": {"shortName": "CISA-ADP"},
      "metrics": [
        {
          "other": {
            "type": "ssvc",
            "content": {
              "id": "CVE-2024-7399",
              "role": "CISA Coordinator",
              "options": [
                {"Exploitation": "active"},
                {"Automatable": "no"},
                {"Technical Impact": "total"}
              ],
              "version": "2.0.3",
              "timestamp": "2026-04-24T17:37:21.854316Z"
            }
          }
        },
        {"other": {"type": "kev", "content": {}}},
        {
          "cvssV3_1": {
            "version": "3.1",
            "vectorString": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
            "baseScore": 9.1,
            "baseSeverity": "CRITICAL"
          }
        }
      ]
    }]
  }
}`

func TestParseVulnrichment(t *testing.T) {
	var raw vulnRaw
	if err := json.Unmarshal([]byte(sampleVulnrichment), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rec := parseVulnrichment("CVE-2024-7399", raw)
	if rec.SSVCRole != "CISA Coordinator" {
		t.Errorf("ssvc_role: got %q", rec.SSVCRole)
	}
	if rec.SSVC["Exploitation"] != "active" {
		t.Errorf("ssvc Exploitation: got %v", rec.SSVC)
	}
	if rec.SSVC["Automatable"] != "no" {
		t.Errorf("ssvc Automatable: got %v", rec.SSVC)
	}
	if rec.SSVC["Technical Impact"] != "total" {
		t.Errorf("ssvc Technical Impact: got %v", rec.SSVC)
	}
	if !rec.HasKEVTag {
		t.Errorf("expected kev tag")
	}
	if rec.CISACvss31 == nil || rec.CISACvss31.BaseScore != 9.1 {
		t.Errorf("cisa_cvss31: got %+v", rec.CISACvss31)
	}
	if rec.CISACvss31.BaseSeverity != "CRITICAL" {
		t.Errorf("cisa cvss severity: got %q", rec.CISACvss31.BaseSeverity)
	}
}
