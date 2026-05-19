// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// EPSS (Exploit Prediction Scoring System) — daily bulk CSV from FIRST.
//
// Format: gzipped CSV, ~330k rows, refreshed daily.
//
//	#model_version:vYYYY.MM.DD,score_date:...
//	cve,epss,percentile
//	CVE-1999-0001,0.0119,0.7888
//
// EPSS scores are recomputed every 24h from honeypot/IDS telemetry; values
// in [0,1]. The percentile column is the rank of this CVE's score among
// all CVEs in the dataset on the score date.
package cisa_kev

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	epssURL          = "https://epss.cyentia.com/epss_scores-current.csv.gz"
	epssFetchTimeout = 90 * time.Second
)

type epssEntry struct {
	Score      float64
	Percentile float64
	Date       string // YYYY-MM-DD when EPSS computed this score
}

func fetchEPSS(ctx context.Context, client *http.Client) (map[string]epssEntry, string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, epssURL, nil)
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept-Encoding", "gzip")
	r, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("epss %d", r.StatusCode)
	}

	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, "", fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

	// Header line is "#model_version:vYYYY.MM.DD,score_date:...". Read it
	// off the buffered reader before handing the rest to csv.NewReader.
	br := bufio.NewReader(gz)
	headerLine, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, "", fmt.Errorf("header: %w", err)
	}
	scoreDate := parseEPSSScoreDate(headerLine)

	rd := csv.NewReader(br)
	rd.FieldsPerRecord = -1
	colNames, err := rd.Read()
	if err != nil {
		return nil, "", fmt.Errorf("colnames: %w", err)
	}
	cveIdx, epssIdx, pctIdx := -1, -1, -1
	for i, c := range colNames {
		switch strings.ToLower(strings.TrimSpace(c)) {
		case "cve":
			cveIdx = i
		case "epss":
			epssIdx = i
		case "percentile":
			pctIdx = i
		}
	}
	if cveIdx < 0 || epssIdx < 0 || pctIdx < 0 {
		return nil, "", fmt.Errorf("epss columns missing in %v", colNames)
	}

	out := make(map[string]epssEntry, 350_000)
	for {
		row, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // tolerate one bad line
		}
		if len(row) <= pctIdx {
			continue
		}
		cve := strings.TrimSpace(row[cveIdx])
		if cve == "" {
			continue
		}
		score, _ := strconv.ParseFloat(strings.TrimSpace(row[epssIdx]), 64)
		pct, _ := strconv.ParseFloat(strings.TrimSpace(row[pctIdx]), 64)
		out[cve] = epssEntry{Score: score, Percentile: pct, Date: scoreDate}
	}
	return out, scoreDate, nil
}

// parseEPSSScoreDate extracts "YYYY-MM-DD" from the metadata header line.
//
//	"#model_version:v2025.03.14,score_date:2026-04-26T12:55:00Z"
func parseEPSSScoreDate(headerLine string) string {
	const key = "score_date:"
	i := strings.Index(headerLine, key)
	if i < 0 {
		return ""
	}
	rest := headerLine[i+len(key):]
	end := strings.IndexAny(rest, ",\r\n")
	if end < 0 {
		end = len(rest)
	}
	stamp := strings.TrimSpace(rest[:end])
	if t, err := time.Parse(time.RFC3339, stamp); err == nil {
		return t.Format("2006-01-02")
	}
	if len(stamp) >= 10 {
		return stamp[:10]
	}
	return stamp
}
