// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// CISA KEV — Known Exploited Vulnerabilities catalogue, enriched with
// EPSS exploit-probability scores, NVD CVE records, and CISA Vulnrichment
// SSVC decision points.
//
// The KEV catalogue is the spine: ~1,500 CVEs CISA has confirmed are
// being actively exploited in the wild. KEV alone tells you "this matters
// today"; the three enrichment sources tell you what about it matters:
//
//	EPSS         — daily-recomputed exploit-probability score (0..1).
//	               Values close to 1 mean "this is being exploited at
//	               scale right now".
//
//	NVD          — canonical CVE record: CVSS, CWE, references. NIST has
//	               slowed enrichment dramatically since 2024 so newer
//	               CVEs may have empty metrics here.
//
//	Vulnrichment — CISA's program to fill the gap NIST left. Provides
//	               SSVC decision points (Exploitation/Automatable/Technical
//	               Impact/Mission Prevalence) and CVSS scores for CVEs
//	               NIST hasn't analysed.
//
// Output: one event per KEV CVE (lat=lon=0, non-geospatial), with the raw
// CVE record from CISA plus whichever enrichment is available in
// in-memory caches at emit time. Caches are best-effort: KEV enrichment
// fills in over the first few ticks (NVD is rate-limited at 5 req/30s
// without an API key, ~50/30s with one).
//
// Env knobs:
//
//	NVD_API_KEY                — optional, lifts NVD rate limit ~10×
//	GORDIOS_KEV_NVD_BUDGET           — max NVD fetches per tick (default 200)
//	GORDIOS_KEV_VULN_BUDGET          — max Vulnrichment fetches per tick (default 400)
package cisa_kev

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"

	"github.com/jackc/pgx/v5/pgxpool"
)

const feedURL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"

// Cache TTLs. CVE records change rarely; SSVC decisions are relatively
// stable too. EPSS is refreshed every tick (no cache; bulk pull).
const (
	nvdCacheTTL  = 30 * 24 * time.Hour
	vulnCacheTTL = 7 * 24 * time.Hour
)

const (
	defaultNVDBudget  = 200
	defaultVulnBudget = 400
)

type Collector struct {
	pool       *pgxpool.Pool
	httpClient *http.Client
	nvdClient  *http.Client // longer timeout for NVD's slower API
	gzClient   *http.Client // for EPSS gzip download

	nvdAPIKey string

	mu        sync.Mutex
	nvdCache  map[string]nvdRecord
	vulnCache map[string]vulnRecord

	epssMu        sync.Mutex
	epssScores    map[string]epssEntry
	epssScoreDate string

	nvdBudget, vulnBudget int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if pool == nil {
		return nil, errors.New("nil pool")
	}
	nvdBudget := defaultNVDBudget
	if v := os.Getenv("GORDIOS_KEV_NVD_BUDGET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			nvdBudget = n
		}
	}
	vulnBudget := defaultVulnBudget
	if v := os.Getenv("GORDIOS_KEV_VULN_BUDGET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			vulnBudget = n
		}
	}
	c := &Collector{
		pool:       pool,
		httpClient: &http.Client{Timeout: 25 * time.Second},
		nvdClient:  &http.Client{Timeout: 30 * time.Second},
		gzClient:   &http.Client{Timeout: epssFetchTimeout},
		nvdAPIKey:  os.Getenv("NVD_API_KEY"),
		nvdCache:   map[string]nvdRecord{},
		vulnCache:  map[string]vulnRecord{},
		nvdBudget:  nvdBudget,
		vulnBudget: vulnBudget,
	}
	if err := c.hydrate(context.Background()); err != nil {
		// Hydrate failure is not fatal — caches just start cold.
		log.Printf("[cisa_kev] hydrate cache: %v (continuing with cold cache)", err)
	}
	return c, nil
}

// hydrate loads cached NVD/Vulnrichment records from
// kev_enrichment_cache into the in-memory maps so an ingester restart
// doesn't restart the rate-limited NVD bootstrap.
func (c *Collector) hydrate(ctx context.Context) error {
	rows, err := c.pool.Query(ctx,
		`SELECT cve, nvd, nvd_at, vulnrichment, vuln_at FROM kev_enrichment_cache`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var nvdLoaded, vulnLoaded int
	for rows.Next() {
		var cve string
		var nvdRaw, vulnRaw []byte
		var nvdAt, vulnAt *time.Time
		if err := rows.Scan(&cve, &nvdRaw, &nvdAt, &vulnRaw, &vulnAt); err != nil {
			continue
		}
		if len(nvdRaw) > 0 && nvdAt != nil {
			var rec nvdRecord
			if err := json.Unmarshal(nvdRaw, &rec); err == nil {
				rec.FetchedAt = *nvdAt
				c.nvdCache[cve] = rec
				nvdLoaded++
			}
		}
		if len(vulnRaw) > 0 && vulnAt != nil {
			var rec vulnRecord
			if err := json.Unmarshal(vulnRaw, &rec); err == nil {
				rec.FetchedAt = *vulnAt
				c.vulnCache[cve] = rec
				vulnLoaded++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	log.Printf("[cisa_kev] hydrated cache: %d NVD records, %d Vulnrichment records", nvdLoaded, vulnLoaded)
	return nil
}

func (c *Collector) persistNVD(ctx context.Context, cve string, rec nvdRecord) {
	raw, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = c.pool.Exec(ctx,
		`INSERT INTO kev_enrichment_cache (cve, nvd, nvd_at) VALUES ($1, $2::jsonb, $3)
		 ON CONFLICT (cve) DO UPDATE SET nvd = EXCLUDED.nvd, nvd_at = EXCLUDED.nvd_at`,
		cve, string(raw), rec.FetchedAt)
}

func (c *Collector) persistVuln(ctx context.Context, cve string, rec vulnRecord) {
	raw, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_, _ = c.pool.Exec(ctx,
		`INSERT INTO kev_enrichment_cache (cve, vulnrichment, vuln_at) VALUES ($1, $2::jsonb, $3)
		 ON CONFLICT (cve) DO UPDATE SET vulnrichment = EXCLUDED.vulnrichment, vuln_at = EXCLUDED.vuln_at`,
		cve, string(raw), rec.FetchedAt)
}

func (c *Collector) ID() string               { return "cisa_kev" }
func (c *Collector) PollEvery() time.Duration { return 6 * time.Hour }

type kevFeed struct {
	CatalogVersion  string    `json:"catalogVersion"`
	DateReleased    string    `json:"dateReleased"`
	Count           int       `json:"count"`
	Vulnerabilities []kevVuln `json:"vulnerabilities"`
}

type kevVuln struct {
	CVEID                      string   `json:"cveID"`
	VendorProject              string   `json:"vendorProject"`
	Product                    string   `json:"product"`
	VulnerabilityName          string   `json:"vulnerabilityName"`
	DateAdded                  string   `json:"dateAdded"`
	ShortDescription           string   `json:"shortDescription"`
	RequiredAction             string   `json:"requiredAction"`
	DueDate                    string   `json:"dueDate"`
	KnownRansomwareCampaignUse string   `json:"knownRansomwareCampaignUse"`
	Notes                      string   `json:"notes"`
	CWEs                       []string `json:"cwes"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var f kevFeed
	if err := httpx.GetJSON(ctx, feedURL, nil, &f); err != nil {
		return nil, err
	}

	// Refresh EPSS every tick — single ~5MB gzipped pull.
	c.refreshEPSS(ctx)

	// Decide which CVEs need fresh enrichment fetches and spend the budget.
	c.fillNVD(ctx, f.Vulnerabilities)
	c.fillVulnrichment(ctx, f.Vulnerabilities)

	now := time.Now().UTC()
	out := make([]events.Event, 0, len(f.Vulnerabilities))
	for _, v := range f.Vulnerabilities {
		ts := now
		if t, err := time.Parse("2006-01-02", v.DateAdded); err == nil {
			ts = t.UTC()
		}
		props := map[string]any{
			"cve":         v.CVEID,
			"vendor":      v.VendorProject,
			"product":     v.Product,
			"name":        v.VulnerabilityName,
			"date_added":  v.DateAdded,
			"description": v.ShortDescription,
			"required":    v.RequiredAction,
			"due_date":    v.DueDate,
			"ransomware":  v.KnownRansomwareCampaignUse,
			"cwes":        v.CWEs,
		}
		c.attachEPSS(props, v.CVEID)
		c.attachNVD(props, v.CVEID)
		c.attachVulnrichment(props, v.CVEID)

		out = append(out, events.Event{
			Ts:     ts,
			Source: "cisa_kev",
			ExtID:  v.CVEID,
			Lat:    0, Lon: 0,
			Props: props,
		})
	}
	return out, nil
}

// ---- EPSS ----

func (c *Collector) refreshEPSS(ctx context.Context) {
	scores, scoreDate, err := fetchEPSS(ctx, c.gzClient)
	if err != nil {
		return // best-effort: keep last good map
	}
	c.epssMu.Lock()
	c.epssScores = scores
	c.epssScoreDate = scoreDate
	c.epssMu.Unlock()
}

func (c *Collector) attachEPSS(props map[string]any, cve string) {
	c.epssMu.Lock()
	defer c.epssMu.Unlock()
	if e, ok := c.epssScores[cve]; ok {
		props["epss_score"] = e.Score
		props["epss_percentile"] = e.Percentile
		props["epss_date"] = e.Date
	}
}

// ---- NVD ----

func (c *Collector) fillNVD(ctx context.Context, vulns []kevVuln) {
	delay := nvdRateDelay(c.nvdAPIKey != "")
	budget := c.nvdBudget
	for _, v := range vulns {
		if budget <= 0 {
			return
		}
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		rec, ok := c.nvdCache[v.CVEID]
		fresh := ok && time.Since(rec.FetchedAt) < nvdCacheTTL
		c.mu.Unlock()
		if fresh {
			continue
		}
		newRec, err := fetchNVD(ctx, c.nvdClient, v.CVEID, c.nvdAPIKey)
		if err != nil || newRec.NotFound {
			mitreRec, mitreErr := fetchMITRECVE(ctx, c.httpClient, v.CVEID)
			if mitreErr == nil {
				newRec = mitreRec
			} else if err != nil {
				// Stop on rate-limit signals only if the MITRE fallback also failed.
				return
			}
		}
		c.mu.Lock()
		c.nvdCache[v.CVEID] = newRec
		c.mu.Unlock()
		c.persistNVD(ctx, v.CVEID, newRec)
		budget--
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (c *Collector) attachNVD(props map[string]any, cve string) {
	c.mu.Lock()
	rec, ok := c.nvdCache[cve]
	c.mu.Unlock()
	if !ok || rec.NotFound {
		return
	}
	if rec.RecordSource != "" {
		props["cve_record_source"] = rec.RecordSource
	}
	props["nvd_published"] = rec.Published
	props["nvd_last_modified"] = rec.LastModified
	props["nvd_status"] = rec.VulnStatus
	if rec.Description != "" {
		props["nvd_description"] = rec.Description
	}
	if rec.CVSS31 != nil {
		props["cvss31_score"] = rec.CVSS31.BaseScore
		props["cvss31_severity"] = rec.CVSS31.BaseSeverity
		props["cvss31_vector"] = rec.CVSS31.VectorString
	}
	if rec.CVSS40 != nil {
		props["cvss40_score"] = rec.CVSS40.BaseScore
		props["cvss40_severity"] = rec.CVSS40.BaseSeverity
		props["cvss40_vector"] = rec.CVSS40.VectorString
	}
	if len(rec.Weaknesses) > 0 {
		props["nvd_weaknesses"] = rec.Weaknesses
	}
	if len(rec.References) > 0 {
		props["nvd_references"] = rec.References
	}
}

// ---- Vulnrichment ----

func (c *Collector) fillVulnrichment(ctx context.Context, vulns []kevVuln) {
	type job struct {
		cve string
	}
	jobs := make(chan job)
	const workers = 8
	var wg sync.WaitGroup

	budget := c.vulnBudget
	var budgetMu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					return
				}
				newRec, err := fetchVulnrichment(ctx, c.httpClient, j.cve)
				if err != nil {
					continue
				}
				c.mu.Lock()
				c.vulnCache[j.cve] = newRec
				c.mu.Unlock()
				c.persistVuln(ctx, j.cve, newRec)
			}
		}()
	}

	for _, v := range vulns {
		if ctx.Err() != nil {
			break
		}
		c.mu.Lock()
		rec, ok := c.vulnCache[v.CVEID]
		fresh := ok && time.Since(rec.FetchedAt) < vulnCacheTTL
		c.mu.Unlock()
		if fresh {
			continue
		}
		budgetMu.Lock()
		if budget <= 0 {
			budgetMu.Unlock()
			break
		}
		budget--
		budgetMu.Unlock()
		jobs <- job{cve: v.CVEID}
	}
	close(jobs)
	wg.Wait()
}

func (c *Collector) attachVulnrichment(props map[string]any, cve string) {
	c.mu.Lock()
	rec, ok := c.vulnCache[cve]
	c.mu.Unlock()
	if !ok || rec.NotFound {
		return
	}
	if len(rec.SSVC) > 0 {
		props["ssvc"] = rec.SSVC
		props["ssvc_role"] = rec.SSVCRole
		props["ssvc_timestamp"] = rec.SSVCAt
	}
	if rec.CISACvss31 != nil {
		props["cisa_cvss31_score"] = rec.CISACvss31.BaseScore
		props["cisa_cvss31_severity"] = rec.CISACvss31.BaseSeverity
		props["cisa_cvss31_vector"] = rec.CISACvss31.VectorString
	}
}
