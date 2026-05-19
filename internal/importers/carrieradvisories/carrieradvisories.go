// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package carrieradvisories imports airline advisory workbooks.
// Reads every *.xlsx, extracts structured signals
// from the free-text descriptions, enriches missing ICAO codes against
// the OpenFlights airlines dataset (strict multi-field match, no
// speculation), and UPSERTs into carrier_advisories.
//
// The importer is idempotent — re-running against a fresher batch
// just refreshes the rows (ON CONFLICT DO UPDATE).
package carrieradvisories

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/fetchcache"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

const openFlightsURL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"

func Main(args []string) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	fs := flag.NewFlagSet("carrier-advisories", flag.ExitOnError)
	dir := fs.String("dir", "../tmp_resources", "directory with *.xlsx advisory workbooks")
	of := fs.String("openflights", "", "optional path to an OpenFlights airlines.dat; fetched from github if empty")
	cacheDir := fs.String("cache-dir", ".cache/importer", "Directory for downloaded source cache; empty disables cache")
	refresh := fs.Bool("refresh", false, "Re-download remote sources even when a cached copy exists")
	fs.Parse(args)

	db.LoadDotEnv("../.env", ".env")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	// -------- OpenFlights reference --------
	fetcher := fetchcache.CachedFetcher{Dir: *cacheDir, Refresh: *refresh, Logf: log.Printf}
	ofRows, err := loadOpenFlights(ctx, *of, fetcher)
	if err != nil {
		log.Fatalf("openflights: %v", err)
	}
	log.Printf("[openflights] %d airlines loaded", len(ofRows))

	// Index: IATA → []row, Name-normalized → []row
	byIATA := map[string][]ofAirline{}
	byName := map[string][]ofAirline{}
	for _, r := range ofRows {
		if r.IATA != "" && r.IATA != "-" && r.IATA != "N/A" {
			byIATA[strings.ToUpper(r.IATA)] = append(byIATA[strings.ToUpper(r.IATA)], r)
		}
		if n := normalizeName(r.Name); n != "" {
			byName[n] = append(byName[n], r)
		}
	}

	// -------- Read workbooks --------
	files, err := filepath.Glob(filepath.Join(*dir, "*.xlsx"))
	if err != nil {
		log.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		log.Fatalf("no xlsx files in %s", *dir)
	}

	var stats struct {
		imported, icaoFilled, icaoSkipped int
	}
	ctx2 := context.Background()
	for _, f := range files {
		region := deriveRegion(filepath.Base(f))
		rows, err := parseWorkbook(f)
		if err != nil {
			log.Printf("[%s] parse: %v", filepath.Base(f), err)
			continue
		}
		log.Printf("[%s] %d rows (region=%s)", filepath.Base(f), len(rows), region)
		for _, r := range rows {
			r.Region = region
			r.Content = extractContent(r.DescriptionEN)
			if r.ICAO == "" && r.IATA != "" && r.Country != "" {
				if icao, ok := resolveICAO(r, byIATA, byName); ok {
					r.ICAO = icao
					stats.icaoFilled++
				} else {
					stats.icaoSkipped++
				}
			}
			if err := upsert(ctx2, pool, r); err != nil {
				log.Printf("  upsert %s (%s): %v", r.Name, r.ID, err)
				continue
			}
			stats.imported++
		}
	}
	log.Printf("done — %d rows imported, ICAO filled=%d, ICAO skipped/ambiguous=%d",
		stats.imported, stats.icaoFilled, stats.icaoSkipped)
}

// ---------- OpenFlights CSV parsing ----------

type ofAirline struct {
	Name, Alias, IATA, ICAO, Callsign, Country, Active string
}

func loadOpenFlights(ctx context.Context, path string, fetcher fetchcache.BytesFetcher) ([]ofAirline, error) {
	var r io.Reader
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	} else {
		buf, err := fetcher.GetBytes(ctx, openFlightsURL, nil)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(buf)
	}
	cr := csv.NewReader(r)
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	var out []ofAirline
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(rec) < 8 {
			continue
		}
		out = append(out, ofAirline{
			Name: rec[1], Alias: rec[2], IATA: rec[3], ICAO: rec[4],
			Callsign: rec[5], Country: rec[6], Active: rec[7],
		})
	}
	return out, nil
}

// ---------- xlsx parsing ----------

type carrierRow struct {
	ID, Version, Name, IATA, ICAO, Country, Rating, OperationalStatus string
	Summary, DescriptionEN, Notes                                     string
	LastUpdatedAt                                                     time.Time
	Region                                                            string
	Content                                                           map[string]any
}

func parseWorkbook(path string) ([]carrierRow, error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("no sheets")
	}
	rows, err := f.GetRows(sheets[0])
	if err != nil {
		return nil, err
	}
	// Header row is the first row whose first cell is literally "ID".
	headerIdx := -1
	for i, r := range rows {
		if len(r) > 0 && strings.TrimSpace(r[0]) == "ID" {
			headerIdx = i
			break
		}
	}
	if headerIdx < 0 {
		return nil, fmt.Errorf("no header row")
	}
	var out []carrierRow
	for _, r := range rows[headerIdx+1:] {
		if len(r) < 14 {
			continue
		}
		id := strings.TrimSpace(r[0])
		if id == "" {
			continue
		}
		cr := carrierRow{
			ID: id, Version: r[1], Name: r[2],
			IATA:              strings.ToUpper(strings.TrimSpace(r[3])),
			ICAO:              strings.ToUpper(strings.TrimSpace(r[4])),
			Country:           strings.TrimSpace(r[5]),
			Rating:            strings.TrimSpace(r[6]),
			OperationalStatus: strings.TrimSpace(r[7]),
			Summary:           strings.TrimSpace(r[8]),
			DescriptionEN:     r[9],
			// r[10] = French description, r[12] = Updated By — intentionally dropped.
			Notes: r[11],
		}
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(r[13])); err == nil {
			cr.LastUpdatedAt = t
		}
		out = append(out, cr)
	}
	return out, nil
}

func deriveRegion(fn string) string {
	// Filenames look like: Airlines-2026Mar26 (AFRICAONLY).xlsx
	// One of the supplied files is missing its closing paren, so we
	// also accept an open-paren-to-end-or-dot fallback.
	var tok string
	if m := regexp.MustCompile(`\(([^)]+)\)`).FindStringSubmatch(fn); len(m) == 2 {
		tok = m[1]
	} else if m := regexp.MustCompile(`\(([^.]+)\.`).FindStringSubmatch(fn); len(m) == 2 {
		tok = m[1]
	} else {
		return "UNKNOWN"
	}
	r := strings.ToUpper(strings.TrimSpace(tok))
	r = strings.TrimSuffix(r, "ONLY")
	r = strings.TrimSpace(r)
	return r
}

// ---------- structured-signal extraction ----------

var (
	rxIOSA = regexp.MustCompile(`(?i)\b(IATA )?IOSA\b|Operational Safety Audit`)
	// Suspended / lapsed variants — match both the acronym and the
	// spelled-out "Operational Safety Audit … suspended/lapsed" form.
	rxIOSASusp = regexp.MustCompile(`(?i)(IOSA (?:suspend|laps|decertif|expir|revok|not (?:current|valid)|not renewed)|Operational Safety Audit (?:registration )?(?:suspend|laps|decertif|expir|revok))`)
	rxEUBan    = regexp.MustCompile(`(?i)(banned by EU|EU ban list|added to EU ban|EASA refused|EU safety list)`)
	rxEUTCO    = regexp.MustCompile(`(?i)(EU TCO|third country operators? approval|holds EU third country)`)
	rxUKTCO    = regexp.MustCompile(`(?i)(UK TCO|UK Third Country Operator)`)
	// FAA / IASA Cat flag — acronym forms AND the fully-spelled-out
	// "International Aviation Safety Assessment category N".
	rxFAACat2     = regexp.MustCompile(`(?i)(FAA (?:IASA )?(?:Cat|Category)\s*2|downgrad(?:e|ed) to (?:FAA )?(?:IASA )?(?:Cat|Category)\s*2|IASA category 2|International Aviation Safety Assessment (?:category )?2)`)
	rxFAACat1     = regexp.MustCompile(`(?i)(FAA (?:IASA )?(?:Cat|Category)\s*1\b|upgrad(?:e|ed) to (?:FAA )?(?:IASA )?(?:Cat|Category)\s*1|IASA category 1|International Aviation Safety Assessment (?:category )?1\b)`)
	rxGovt        = regexp.MustCompile(`(?i)(wholly owned by (?:the )?government|government owned|state[\- ]owned|flag carrier|majority government owned)`)
	rxFinStress   = regexp.MustCompile(`(?i)(financial (?:stress|challenges|issues|difficulties|problems)|bankruptcy|insolvenc)`)
	rxGrounded    = regexp.MustCompile(`(?i)(ground(?:ed|ing)|aircraft parked|ops suspend)`)
	rxFatal       = regexp.MustCompile(`(?i)(fatal (?:accident|crash)|crashed)`)
	rxFatalYear   = regexp.MustCompile(`(?i)fatal (?:accident|crash)[^.;]{0,40}(\d{4})`)
	rxCodeshare   = regexp.MustCompile(`(?i)codeshare[sd]?(?: partners?)? with ([^;.]+)`)
	rxParent      = regexp.MustCompile(`(?i)(?:subsidiary of|owned by|parent(?: company)?:?|owns (?:majority|minority) stake) ([A-Z][^,.;]{2,40})`)
	rxFleetBoeing = regexp.MustCompile(`(?i)\bBoeing\b`)
	rxFleetAirbus = regexp.MustCompile(`(?i)\bAirbus\b`)
	rxFleetATR    = regexp.MustCompile(`(?i)\bATR\b`)
	rxFleetEmbr   = regexp.MustCompile(`(?i)\bEmbraer\b`)
	rxFleetBomb   = regexp.MustCompile(`(?i)\bBombardier|CRJ|Dash\b`)
)

// extractContent turns the English description into a structured dict.
// Each field is only set when the source text contains a reasonably
// unambiguous pattern — fields stay absent otherwise so downstream
// "iosa=true" queries never silently pick up unknowns.
func extractContent(desc string) map[string]any {
	out := map[string]any{}
	if strings.TrimSpace(desc) == "" {
		return out
	}
	// IOSA is three-state: certified / suspended / absent. A description
	// that says "IOSA suspended" would otherwise trip a naive bool-true.
	if rxIOSASusp.MatchString(desc) {
		out["iosa"] = "suspended"
	} else if rxIOSA.MatchString(desc) {
		out["iosa"] = "certified"
	}
	if rxEUBan.MatchString(desc) {
		out["eu_ban"] = true
	}
	if rxEUTCO.MatchString(desc) {
		out["eu_tco"] = true
	}
	if rxUKTCO.MatchString(desc) {
		out["uk_tco"] = true
	}
	if rxFAACat2.MatchString(desc) {
		out["faa_iasa_cat"] = 2
	} else if rxFAACat1.MatchString(desc) {
		out["faa_iasa_cat"] = 1
	}
	if rxGovt.MatchString(desc) {
		out["government_owned"] = true
	}
	if rxFinStress.MatchString(desc) {
		out["financial_stress"] = true
	}
	if rxGrounded.MatchString(desc) {
		out["grounded_history"] = true
	}
	if rxFatal.MatchString(desc) {
		out["fatal_accident"] = true
		if m := rxFatalYear.FindStringSubmatch(desc); len(m) > 1 {
			out["fatal_year"] = m[1]
		}
	}
	if m := rxCodeshare.FindAllStringSubmatch(desc, -1); len(m) > 0 {
		var partners []string
		for _, hit := range m {
			cleaned := strings.TrimSpace(strings.ReplaceAll(hit[1], " and ", ", "))
			for _, p := range strings.Split(cleaned, ",") {
				if p = strings.TrimSpace(p); p != "" && len(p) < 40 {
					partners = append(partners, p)
				}
			}
		}
		if len(partners) > 0 {
			out["codeshare_partners"] = partners
		}
	}
	fleet := []string{}
	if rxFleetBoeing.MatchString(desc) {
		fleet = append(fleet, "Boeing")
	}
	if rxFleetAirbus.MatchString(desc) {
		fleet = append(fleet, "Airbus")
	}
	if rxFleetATR.MatchString(desc) {
		fleet = append(fleet, "ATR")
	}
	if rxFleetEmbr.MatchString(desc) {
		fleet = append(fleet, "Embraer")
	}
	if rxFleetBomb.MatchString(desc) {
		fleet = append(fleet, "Bombardier")
	}
	if len(fleet) > 0 {
		out["fleet_manufacturers"] = fleet
	}
	return out
}

// ---------- ICAO enrichment (strict, no speculation) ----------

var spaceNormRE = regexp.MustCompile(`[^a-z0-9]+`)

func normalizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = spaceNormRE.ReplaceAllString(s, "")
	return s
}

// resolveICAO fills in an ICAO code ONLY when we have a unique match on
// (IATA + country) AND the OpenFlights name agrees with the advisory
// name under token normalization. Any ambiguity → we return ok=false
// and the ICAO stays NULL.
func resolveICAO(r carrierRow, byIATA, byName map[string][]ofAirline) (string, bool) {
	cands := byIATA[strings.ToUpper(r.IATA)]
	if len(cands) == 0 {
		return "", false
	}
	// Filter to ACTIVE and matching country.
	country := strings.ToLower(strings.TrimSpace(r.Country))
	var matches []ofAirline
	for _, c := range cands {
		if c.ICAO == "" || c.ICAO == "-" || c.ICAO == "N/A" {
			continue
		}
		if c.Active != "Y" {
			continue
		}
		if country != "" && !sameCountry(c.Country, country) {
			continue
		}
		matches = append(matches, c)
	}
	if len(matches) == 1 {
		return strings.ToUpper(matches[0].ICAO), true
	}
	// Multiple IATA+country hits? Fall back to name match.
	if len(matches) > 1 {
		normTarget := normalizeName(r.Name)
		for _, c := range matches {
			if normalizeName(c.Name) == normTarget {
				return strings.ToUpper(c.ICAO), true
			}
		}
		return "", false // ambiguous
	}
	// Try name-only match as last resort (some OpenFlights rows have no
	// IATA — we've already consumed byIATA above, so this is searching
	// the name index directly for exact-normalized matches).
	if nm := byName[normalizeName(r.Name)]; len(nm) == 1 &&
		nm[0].ICAO != "" && nm[0].ICAO != "-" && nm[0].ICAO != "N/A" &&
		sameCountry(nm[0].Country, country) {
		return strings.ToUpper(nm[0].ICAO), true
	}
	return "", false
}

// sameCountry is a loose comparison — OpenFlights uses full names,
// Some advisory workbooks abbreviate country names. Exact match plus a few
// well-known aliases.
func sameCountry(a, b string) bool {
	al := strings.ToLower(strings.TrimSpace(a))
	bl := strings.ToLower(strings.TrimSpace(b))
	if al == bl {
		return true
	}
	alias := map[string]string{
		"usa":                         "united states",
		"us":                          "united states",
		"u.s.":                        "united states",
		"uk":                          "united kingdom",
		"u.k.":                        "united kingdom",
		"uae":                         "united arab emirates",
		"ivory coast":                 "cote d'ivoire",
		"côte d'ivoire":               "cote d'ivoire",
		"russia":                      "russian federation",
		"south korea":                 "korea, republic of",
		"korea (republic of)":         "korea, republic of",
		"taiwan":                      "taiwan, province of china",
		"vietnam":                     "viet nam",
		"congo (democratic republic)": "democratic republic of the congo",
	}
	if v, ok := alias[al]; ok && v == bl {
		return true
	}
	if v, ok := alias[bl]; ok && v == al {
		return true
	}
	return false
}

// ---------- DB upsert ----------

func upsert(ctx context.Context, pool *pgxpool.Pool, r carrierRow) error {
	contentRaw, _ := json.Marshal(r.Content)
	const stmt = `
		INSERT INTO carrier_advisories
			(id, version, name, iata, icao, country, rating, operational_status,
			 summary, description_en, notes,
			 last_updated_at, region, content, imported_at)
		VALUES ($1,$2,$3, NULLIF($4,''), NULLIF($5,''), NULLIF($6,''),
		        NULLIF($7,''), NULLIF($8,''),
		        NULLIF($9,''), NULLIF($10,''), NULLIF($11,''),
		        CASE WHEN $12::text <> '' THEN $12::timestamptz ELSE NULL END,
		        NULLIF($13,''),
		        $14::jsonb, now())
		ON CONFLICT (id) DO UPDATE SET
			version=EXCLUDED.version, name=EXCLUDED.name, iata=EXCLUDED.iata,
			icao=EXCLUDED.icao, country=EXCLUDED.country, rating=EXCLUDED.rating,
			operational_status=EXCLUDED.operational_status, summary=EXCLUDED.summary,
			description_en=EXCLUDED.description_en, notes=EXCLUDED.notes,
			last_updated_at=EXCLUDED.last_updated_at, region=EXCLUDED.region,
			content=EXCLUDED.content, imported_at=now()`
	tsStr := ""
	if !r.LastUpdatedAt.IsZero() {
		tsStr = r.LastUpdatedAt.Format(time.RFC3339)
	}
	_, err := pool.Exec(ctx, stmt,
		r.ID, r.Version, r.Name,
		r.IATA, r.ICAO, r.Country,
		r.Rating, r.OperationalStatus,
		r.Summary, r.DescriptionEN, r.Notes,
		tsStr, r.Region,
		string(contentRaw),
	)
	return err
}

// sanity compile anchor
var _ = pgx.ErrNoRows
