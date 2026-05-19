// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package airline_safety ingests public aviation safety, oversight, and carrier
// document sources used as raw evidence for airline assessment workflows.
package airline_safety

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	stdhtml "html"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"

	"github.com/xuri/excelize/v2"
	nethtml "golang.org/x/net/html"
)

const (
	iataIOSASourceID          = "iata_iosa_registry"
	iataISSASourceID          = "iata_issa_registry"
	euAirSafetyListSourceID   = "eu_air_safety_list"
	faaIASASourceID           = "faa_iasa"
	icaoUSOAPSourceID         = "icao_usoap"
	faaSDRSourceID            = "faa_sdr"
	ntsbAccidentsSourceID     = "ntsb_aviation_accidents"
	easaCZIBSourceID          = "easa_czib"
	faaRestrictionsSourceID   = "faa_flight_restrictions"
	dotCarriersSourceID       = "dot_certificated_carriers"
	airlineReportsSourceID    = "airline_reports_monitor"
	iataIOSAPageURL           = "https://www.iata.org/en/programs/safety/audit/iosa/registry/?ordering=Alphabetical"
	iataIOSARegistryURL       = "https://ic.iata.org/registry/iosa?page=1"
	iataISSAPageURL           = "https://www.iata.org/en/programs/safety/audit/issa/registry/"
	euAirSafetyListPageURL    = "https://transport.ec.europa.eu/transport-themes/eu-air-safety-list_en"
	faaIASAResultsURL         = "https://www.faa.gov/about/initiatives/iasa/iasa-program-results"
	icaoUSOAPViewerURL        = "https://www.icao.int/safety-audit-results-usoap-interactive-viewer"
	faaSDRYearURLPattern      = "https://external.apic4e.faa.gov/sdrs/retrieve/SDR-%d.csv"
	ntsbAvDataURL             = "https://data.ntsb.gov/avdata"
	easaCZIBJSONURL           = "https://www.easa.europa.eu/en/domains/air-operations/czibs/export-json?page&_format=json"
	faaFlightRestrictionsURL  = "https://www.faa.gov/air_traffic/publications/us_restrictions/"
	dotCertificatedCarrierURL = "https://www.transportation.gov/policy/aviation-policy/certificated-air-carriers-list"
)

type Collector struct {
	id        string
	pollEvery time.Duration
	client    *http.Client
	fetch     func(context.Context, *http.Client) ([]events.Event, error)
}

func (c *Collector) ID() string               { return c.id }
func (c *Collector) PollEvery() time.Duration { return c.pollEvery }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	client := c.client
	if client == nil {
		client = collectorutil.HTTPClient(60 * time.Second)
	}
	return c.fetch(ctx, client)
}

func NewIATAIOSARegistry() (*Collector, error) {
	return newCollector(iataIOSASourceID, 7*24*time.Hour, fetchIATAIOSARegistry), nil
}

func NewIATAISSARegistry() (*Collector, error) {
	return newCollector(iataISSASourceID, 7*24*time.Hour, fetchIATAISSARegistry), nil
}

func NewEUAirSafetyList() (*Collector, error) {
	return newCollector(euAirSafetyListSourceID, 24*time.Hour, fetchEUAirSafetyList), nil
}

func NewFAAIASA() (*Collector, error) {
	return newCollector(faaIASASourceID, 7*24*time.Hour, fetchFAAIASA), nil
}

func NewICAOUSOAP() (*Collector, error) {
	return newCollector(icaoUSOAPSourceID, 30*24*time.Hour, fetchICAOUSOAP), nil
}

func NewFAASDR() (*Collector, error) {
	return newCollector(faaSDRSourceID, 24*time.Hour, fetchFAASDR), nil
}

func NewNTSBAviationAccidents() (*Collector, error) {
	return newCollector(ntsbAccidentsSourceID, 24*time.Hour, fetchNTSBAviationAccidents), nil
}

func NewEASACZIB() (*Collector, error) {
	return newCollector(easaCZIBSourceID, 24*time.Hour, fetchEASACZIB), nil
}

func NewFAAFlightRestrictions() (*Collector, error) {
	return newCollector(faaRestrictionsSourceID, 24*time.Hour, fetchFAAFlightRestrictions), nil
}

func NewDOTCertificatedCarriers() (*Collector, error) {
	return newCollector(dotCarriersSourceID, 7*24*time.Hour, fetchDOTCertificatedCarriers), nil
}

func NewAirlineReportsMonitor() (*Collector, error) {
	return newCollector(airlineReportsSourceID, 7*24*time.Hour, fetchAirlineReportsMonitor), nil
}

func newCollector(id string, pollEvery time.Duration, fetch func(context.Context, *http.Client) ([]events.Event, error)) *Collector {
	return &Collector{
		id:        id,
		pollEvery: pollEvery,
		client:    collectorutil.HTTPClient(60 * time.Second),
		fetch:     fetch,
	}
}

func fetchIATAIOSARegistry(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, iataIOSAPageURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	text := htmlText(string(buf))
	ts := day(modified)
	props := baseProps("IATA Operational Safety Audit Registry", iataIOSAPageURL, ts, 14*24*time.Hour, "iata_iosa_registry_page")
	props["registry_url"] = iataIOSARegistryURL
	props["access_note"] = "The public IATA Connect registry is Cloudflare-protected from this runtime; this collector monitors the official IATA registry landing page."
	props["summary"] = firstMatch(text, `(?i)IOSA Registry.*?(?:airlines|platform)\.`)
	return []events.Event{{
		Ts:     ts,
		Source: iataIOSASourceID,
		ExtID:  "registry_page:" + ts.Format("2006-01-02"),
		Props:  props,
	}}, nil
}

func fetchIATAISSARegistry(ctx context.Context, client *http.Client) ([]events.Event, error) {
	first, modified, err := fetchBytes(ctx, client, iataISSAPageURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	pages := maxPaginationPage(first)
	if pages < 1 {
		pages = 1
	}
	if pages > 10 {
		pages = 10
	}
	ts := day(modified)
	allRows := []map[string]string{}
	for page := 1; page <= pages; page++ {
		buf := first
		if page > 1 {
			pageURL := iataISSAPageURL + "?page=" + strconv.Itoa(page)
			buf, _, err = fetchBytes(ctx, client, pageURL, "text/html,*/*")
			if err != nil {
				return nil, err
			}
		}
		allRows = append(allRows, tableRowsByHeader(buf, "airline_name", "registration_expiry")...)
	}
	out := make([]events.Event, 0, len(allRows))
	for _, row := range allRows {
		name := row["airline_name"]
		if name == "" {
			continue
		}
		props := baseProps("IATA Standard Safety Assessment Registry", iataISSAPageURL, ts, 14*24*time.Hour, "iata_issa_registry_page")
		props["airline_name"] = name
		props["region"] = row["region"]
		props["country"] = row["country_territory"]
		props["registration_expiry"] = row["registration_expiry"]
		props["registration_comments"] = row["registration_comments"]
		props["icao_code"] = row["icao_code"]
		props["iata_code"] = row["iata_code"]
		out = append(out, events.Event{
			Ts:     ts,
			Source: iataISSASourceID,
			ExtID:  "issa:" + stableID(name+"|"+row["icao_code"]+"|"+row["iata_code"]),
			Props:  props,
		})
	}
	if len(out) == 0 {
		return nil, errors.New("no ISSA registry rows found")
	}
	return out, nil
}

func fetchEUAirSafetyList(ctx context.Context, client *http.Client) ([]events.Event, error) {
	page, modified, err := fetchBytes(ctx, client, euAirSafetyListPageURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	xlsxURL := firstLinkMatching(page, euAirSafetyListPageURL, ".xlsx")
	if xlsxURL == "" {
		return nil, errors.New("EU air safety list Excel link not found")
	}
	buf, xlsxModified, err := fetchBytes(ctx, client, xlsxURL, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*")
	if err != nil {
		return nil, err
	}
	if !xlsxModified.IsZero() {
		modified = xlsxModified
	}
	return parseEUAirSafetyListXLSX(buf, day(modified), xlsxURL)
}

func parseEUAirSafetyListXLSX(buf []byte, ts time.Time, endpoint string) ([]events.Event, error) {
	f, err := excelize.OpenReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := []events.Event{}
	for _, sheet := range []string{"Annex A", "Annex B"} {
		rows, err := f.GetRows(sheet)
		if err != nil {
			continue
		}
		headerIdx, headers := findEUHeader(rows)
		if headerIdx < 0 {
			continue
		}
		for _, row := range rows[headerIdx+1:] {
			rec := rowMap(headers, row)
			name := firstNonEmpty(rec["name"], rec["name_of_the_legal_entity_of_the_air_carrier_as_indicated_on_its_aoc_and_its_trading_name_if_different"])
			if name == "" || strings.HasPrefix(strings.ToLower(name), "name of ") {
				continue
			}
			props := baseProps("EU Air Safety List", endpoint, ts, 14*24*time.Hour, "eu_air_safety_list_file")
			props["annex"] = sheet
			props["restriction_type"] = map[bool]string{true: "operating_ban", false: "operational_restriction"}[sheet == "Annex A"]
			props["airline_name"] = name
			props["aoc_or_operating_licence"] = firstNonEmpty(rec["air_operator_certificate_aoc_number_or_operating_licence_number"], rec["air_operator_certificate_aoc_number"])
			props["state_of_operator"] = rec["state_of_the_operator"]
			props["icao_code"] = rec["icao_three_letter_designator"]
			props["aircraft_type_restricted"] = rec["aircraft_type_restricted"]
			props["raw_row"] = rec
			out = append(out, events.Event{
				Ts:     ts,
				Source: euAirSafetyListSourceID,
				ExtID:  strings.ToLower(strings.ReplaceAll(sheet, " ", "_")) + ":" + stableID(name+"|"+fmt.Sprint(props["aoc_or_operating_licence"])+"|"+fmt.Sprint(props["state_of_operator"])),
				Props:  props,
			})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no EU air safety list rows found")
	}
	return out, nil
}

func findEUHeader(rows [][]string) (int, []string) {
	for i, row := range rows {
		normalized := make([]string, len(row))
		for j, cell := range row {
			key := normalizeKey(cell)
			switch {
			case strings.Contains(key, "legal_entity"):
				key = "name"
			case strings.Contains(key, "air_operator_certificate") || strings.Contains(key, "operating_licence"):
				if strings.Contains(key, "number") {
					key = "air_operator_certificate_aoc_number_or_operating_licence_number"
				}
			case strings.Contains(key, "state_of"):
				key = "state_of_the_operator"
			case strings.Contains(key, "icao"):
				key = "icao_three_letter_designator"
			case strings.Contains(key, "aircraft_type"):
				key = "aircraft_type_restricted"
			}
			normalized[j] = key
		}
		if contains(normalized, "name") && contains(normalized, "state_of_the_operator") {
			return i, normalized
		}
	}
	return -1, nil
}

func fetchFAAIASA(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, faaIASAResultsURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	ts := sourceDateOrModified(buf, modified)
	pdfURL := firstLinkMatching(buf, faaIASAResultsURL, "IASAWS")
	props := baseProps("FAA International Aviation Safety Assessment Program Results", faaIASAResultsURL, ts, 14*24*time.Hour, "faa_iasa_results_page")
	props["results_document_url"] = pdfURL
	props["scope"] = "Country civil aviation authority safety oversight category results, not airline-level certification."
	return []events.Event{{
		Ts:     ts,
		Source: faaIASASourceID,
		ExtID:  "results:" + stableID(pdfURL),
		Props:  props,
	}}, nil
}

func fetchICAOUSOAP(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, icaoUSOAPViewerURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	ts := day(modified)
	iframe := firstMatch(string(buf), `(?is)<iframe[^>]+src="([^"]+)"`)
	apiLink := firstLinkMatching(buf, icaoUSOAPViewerURL, "API-Data-Service")
	props := baseProps("ICAO USOAP Safety Audit Results interactive viewer", icaoUSOAPViewerURL, ts, 45*24*time.Hour, "icao_usoap_public_viewer_page")
	props["interactive_viewer_url"] = iframe
	props["api_service_url"] = apiLink
	props["access_note"] = "The public page references the ICAO iSTARS API Data Service; row-level API access requires an API key."
	return []events.Event{{
		Ts:     ts,
		Source: icaoUSOAPSourceID,
		ExtID:  "viewer:" + stableID(iframe+apiLink),
		Props:  props,
	}}, nil
}

func fetchFAASDR(ctx context.Context, client *http.Client) ([]events.Event, error) {
	year := time.Now().UTC().Year()
	if raw := strings.TrimSpace(os.Getenv("FAA_SDR_YEAR")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 1995 && v <= year {
			year = v
		}
	}
	maxRows := collectorutil.EnvInt("FAA_SDR_MAX_ROWS", 250, 1, 2000)
	endpoint := fmt.Sprintf(faaSDRYearURLPattern, year)
	req, err := newRequest(ctx, endpoint, "text/csv,*/*")
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, httpStatusError(endpoint, resp)
	}
	return parseFAASDRCSV(resp.Body, endpoint, maxRows)
}

func parseFAASDRCSV(r io.Reader, endpoint string, maxRows int) ([]events.Event, error) {
	cr := csv.NewReader(bufio.NewReader(r))
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	headerKeys := make([]string, len(header))
	for i, h := range header {
		headerKeys[i] = normalizeKey(h)
	}
	out := []events.Event{}
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		rec := rowMap(headerKeys, row)
		id := firstNonEmpty(rec["operator_control_number"], stableID(strings.Join(row, "|")))
		ts := parseAnyDate(firstNonEmpty(rec["difficulty_date"], rec["submission_date"]))
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		props := baseProps("FAA Service Difficulty Reports", endpoint, ts, 7*24*time.Hour, "faa_sdr_year_csv")
		props["operator_control_number"] = rec["operator_control_number"]
		props["difficulty_date"] = rec["difficulty_date"]
		props["submission_date"] = rec["submission_date"]
		props["operator_designator"] = rec["operator_designator"]
		props["registry_n_number"] = rec["registry_n_number"]
		props["aircraft_make"] = rec["aircraft_make"]
		props["aircraft_model"] = rec["aircraft_model"]
		props["part_name"] = rec["part_name"]
		props["part_condition"] = rec["part_condition"]
		props["stage_of_operation_code"] = rec["stage_of_operation_code"]
		props["discrepancy"] = rec["discrepancy"]
		props["raw_row"] = rec
		out = append(out, events.Event{
			Ts:     ts.UTC(),
			Source: faaSDRSourceID,
			ExtID:  "sdr:" + id,
			Props:  props,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Ts.After(out[j].Ts) })
	if len(out) > maxRows {
		out = out[:maxRows]
	}
	if len(out) == 0 {
		return nil, errors.New("no FAA SDR rows found")
	}
	return out, nil
}

func fetchNTSBAviationAccidents(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, ntsbAvDataURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	return parseNTSBDirectory(buf, day(modified), ntsbAvDataURL), nil
}

type ntsbFile struct {
	Name string
	Date time.Time
	Size int64
	URL  string
}

func parseNTSBDirectory(buf []byte, ts time.Time, endpoint string) []events.Event {
	files := parseNTSBFiles(buf, endpoint)
	sort.SliceStable(files, func(i, j int) bool { return files[i].Date.After(files[j].Date) })
	out := []events.Event{}
	includedUpdates := 0
	for _, f := range files {
		isWeekly := strings.HasPrefix(strings.ToLower(f.Name), "up") && strings.HasSuffix(strings.ToLower(f.Name), ".zip")
		if f.Name != "avall.zip" && !isWeekly {
			continue
		}
		if isWeekly {
			if includedUpdates >= 8 {
				continue
			}
			includedUpdates++
		}
		eventTs := f.Date
		if eventTs.IsZero() {
			eventTs = ts
		}
		props := baseProps("NTSB aviation accident data download directory", endpoint, eventTs, 14*24*time.Hour, "ntsb_avdata_directory")
		props["file_name"] = f.Name
		props["file_url"] = f.URL
		props["file_size_bytes"] = f.Size
		props["file_date"] = f.Date.Format(time.RFC3339)
		out = append(out, events.Event{
			Ts:     eventTs,
			Source: ntsbAccidentsSourceID,
			ExtID:  "file:" + f.Name,
			Props:  props,
		})
	}
	return out
}

func parseNTSBFiles(buf []byte, endpoint string) []ntsbFile {
	rowRe := regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
	cellRe := regexp.MustCompile(`(?is)<td[^>]*>(.*?)</td>`)
	linkRe := regexp.MustCompile(`(?is)<a[^>]+href="([^"]+)"[^>]*>`)
	out := []ntsbFile{}
	for _, row := range rowRe.FindAllSubmatch(buf, -1) {
		cells := cellRe.FindAllSubmatch(row[1], -1)
		if len(cells) < 4 {
			continue
		}
		name := cleanHTML(string(cells[0][1]))
		if name == "" || name == "File name" {
			continue
		}
		date := parseAnyDate(cleanHTML(string(cells[1][1])))
		size, _ := strconv.ParseInt(strings.ReplaceAll(cleanHTML(string(cells[2][1])), ",", ""), 10, 64)
		link := ""
		if m := linkRe.FindSubmatch(cells[3][1]); len(m) > 1 {
			link = absoluteURL(endpoint, stdhtml.UnescapeString(string(m[1])))
		}
		out = append(out, ntsbFile{Name: name, Date: date, Size: size, URL: link})
	}
	return out
}

func fetchEASACZIB(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, easaCZIBJSONURL, "application/json,*/*")
	if err != nil {
		return nil, err
	}
	return parseEASACZIBJSON(buf, day(modified), easaCZIBJSONURL)
}

type easaCZIBResponse struct {
	ConflictZones []struct {
		Nid             string `json:"Nid"`
		IssuedDate      string `json:"issued_date"`
		ValidUntilDate  string `json:"valid_until_date"`
		ValidUntilDescr string `json:"field_easa_valid_until_descr"`
		Name            string `json:"name"`
		Status          string `json:"status"`
		Country         string `json:"country"`
		Coordinates     string `json:"coordinates"`
		Updated         string `json:"updated"`
	} `json:"conflict_zones"`
}

func parseEASACZIBJSON(buf []byte, fallback time.Time, endpoint string) ([]events.Event, error) {
	var resp easaCZIBResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(resp.ConflictZones))
	for _, z := range resp.ConflictZones {
		ts := firstTime(parseTimeTag(z.Updated), parseAnyDate(z.IssuedDate), fallback)
		lat, lon := parseLatLon(z.Coordinates)
		props := baseProps("EASA Conflict Zone Information Bulletins", endpoint, ts, 48*time.Hour, "easa_czib_json_export")
		props["nid"] = z.Nid
		props["name"] = z.Name
		props["status"] = z.Status
		props["country"] = z.Country
		props["issued_date"] = z.IssuedDate
		props["valid_until_date"] = z.ValidUntilDate
		props["valid_until_description"] = cleanHTML(z.ValidUntilDescr)
		props["updated"] = cleanHTML(z.Updated)
		out = append(out, events.Event{
			Ts:     ts,
			Source: easaCZIBSourceID,
			ExtID:  "czib:" + firstNonEmpty(z.Nid, stableID(z.Name+"|"+z.Country)),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	if len(out) == 0 {
		return nil, errors.New("no EASA CZIB rows found")
	}
	return out, nil
}

func fetchFAAFlightRestrictions(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, faaFlightRestrictionsURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	return parseFAAFlightRestrictions(buf, sourceDateOrModified(buf, modified), faaFlightRestrictionsURL), nil
}

func parseFAAFlightRestrictions(buf []byte, ts time.Time, endpoint string) []events.Event {
	section := pagewatchSection(string(buf))
	tokens := htmlTokens(section)
	area := ""
	out := []events.Event{}
	for _, tok := range tokens {
		switch tok.kind {
		case "heading":
			if tok.text != "" {
				area = tok.text
			}
		case "link":
			if tok.href == "" || strings.EqualFold(tok.text, "Back to top") || strings.HasPrefix(tok.href, "#") {
				continue
			}
			docURL := absoluteURL(endpoint, tok.href)
			props := baseProps("FAA Prohibitions, Restrictions and Notices", endpoint, ts, 48*time.Hour, "faa_flight_restrictions_page")
			props["restriction_area"] = area
			props["document_title"] = tok.text
			props["document_url"] = docURL
			out = append(out, events.Event{
				Ts:     ts,
				Source: faaRestrictionsSourceID,
				ExtID:  "restriction:" + stableID(area+"|"+docURL),
				Props:  props,
			})
		}
	}
	return out
}

func fetchDOTCertificatedCarriers(ctx context.Context, client *http.Client) ([]events.Event, error) {
	buf, modified, err := fetchBytes(ctx, client, dotCertificatedCarrierURL, "text/html,*/*")
	if err != nil {
		return nil, err
	}
	ts := sourceDateOrModified(buf, modified)
	pdfURL := firstLinkMatching(buf, dotCertificatedCarrierURL, "Cert Carrier")
	props := baseProps("U.S. DOT Certificated Air Carriers List", dotCertificatedCarrierURL, ts, 14*24*time.Hour, "dot_certificated_carriers_page")
	props["document_url"] = pdfURL
	props["description"] = firstMatch(htmlText(string(buf)), `(?i)List of Certificated Air Carriers includes[^.]+\.`)
	return []events.Event{{
		Ts:     ts,
		Source: dotCarriersSourceID,
		ExtID:  "document:" + stableID(pdfURL),
		Props:  props,
	}}, nil
}

func fetchAirlineReportsMonitor(ctx context.Context, client *http.Client) ([]events.Event, error) {
	urls := collectorutil.SplitCSV(os.Getenv("AIRLINE_REPORT_URLS"))
	out := []events.Event{}
	for _, rawURL := range urls {
		ev, ok := monitorURL(ctx, client, rawURL)
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func monitorURL(ctx context.Context, client *http.Client, rawURL string) (events.Event, bool) {
	req, err := newRequest(ctx, rawURL, "text/html,application/pdf,application/json,*/*")
	if err != nil {
		return events.Event{}, false
	}
	req.Method = http.MethodHead
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode >= 500 {
		if resp != nil {
			resp.Body.Close()
		}
		req, err = newRequest(ctx, rawURL, "text/html,application/pdf,application/json,*/*")
		if err != nil {
			return events.Event{}, false
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		return events.Event{}, false
	}
	defer resp.Body.Close()
	modified := time.Now().UTC()
	if h := strings.TrimSpace(resp.Header.Get("Last-Modified")); h != "" {
		if t, err := http.ParseTime(h); err == nil {
			modified = t.UTC()
		}
	}
	title := ""
	if resp.Request.Method != http.MethodHead && strings.Contains(resp.Header.Get("Content-Type"), "html") {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
		title = firstMatch(string(body), `(?is)<title[^>]*>(.*?)</title>`)
		title = cleanHTML(title)
	}
	ts := day(modified)
	props := baseProps("Configured airline report monitor", rawURL, ts, 14*24*time.Hour, "airline_reports_configured_url")
	props["url"] = rawURL
	props["title"] = title
	props["status_code"] = resp.StatusCode
	props["content_type"] = resp.Header.Get("Content-Type")
	props["content_length"] = resp.Header.Get("Content-Length")
	props["etag"] = resp.Header.Get("ETag")
	props["last_modified"] = resp.Header.Get("Last-Modified")
	return events.Event{
		Ts:     ts,
		Source: airlineReportsSourceID,
		ExtID:  "url:" + stableID(rawURL+"|"+resp.Header.Get("ETag")+"|"+resp.Header.Get("Last-Modified")),
		Props:  props,
	}, true
}

type token struct {
	kind string
	text string
	href string
	pos  int
}

func htmlTokens(s string) []token {
	headingRe := regexp.MustCompile(`(?is)<h[2-4][^>]*>(.*?)</h[2-4]>`)
	linkRe := regexp.MustCompile(`(?is)<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>`)
	out := []token{}
	for _, m := range headingRe.FindAllStringSubmatchIndex(s, -1) {
		out = append(out, token{kind: "heading", text: cleanHTML(s[m[2]:m[3]]), pos: m[0]})
	}
	for _, m := range linkRe.FindAllStringSubmatchIndex(s, -1) {
		out = append(out, token{kind: "link", href: stdhtml.UnescapeString(s[m[2]:m[3]]), text: cleanHTML(s[m[4]:m[5]]), pos: m[0]})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

func pagewatchSection(s string) string {
	start := strings.Index(s, "<!--PAGEWATCH-->")
	end := strings.Index(s, "<!--/PAGEWATCH-->")
	if start >= 0 && end > start {
		return s[start:end]
	}
	return s
}

func tableRowsByHeader(buf []byte, required ...string) []map[string]string {
	tables := extractTables(buf)
	for _, rows := range tables {
		if len(rows) < 2 {
			continue
		}
		headers := make([]string, len(rows[0]))
		for i, h := range rows[0] {
			headers[i] = normalizeKey(h)
		}
		ok := true
		for _, key := range required {
			if !contains(headers, key) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		out := make([]map[string]string, 0, len(rows)-1)
		for _, row := range rows[1:] {
			rec := rowMap(headers, row)
			if nonEmptyValues(rec) > 0 {
				out = append(out, rec)
			}
		}
		return out
	}
	return nil
}

func extractTables(buf []byte) [][][]string {
	root, err := nethtml.Parse(bytes.NewReader(buf))
	if err != nil {
		return nil
	}
	var tables [][][]string
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n.Type == nethtml.ElementNode && n.Data == "table" {
			tables = append(tables, extractTable(n))
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return tables
}

func extractTable(table *nethtml.Node) [][]string {
	var rows [][]string
	var walkRows func(*nethtml.Node)
	walkRows = func(n *nethtml.Node) {
		if n.Type == nethtml.ElementNode && n.Data == "tr" {
			row := extractCells(n)
			if len(row) > 0 {
				rows = append(rows, row)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walkRows(c)
		}
	}
	walkRows(table)
	return rows
}

func extractCells(row *nethtml.Node) []string {
	cells := []string{}
	for c := row.FirstChild; c != nil; c = c.NextSibling {
		if c.Type != nethtml.ElementNode || (c.Data != "td" && c.Data != "th") {
			continue
		}
		cells = append(cells, strings.Join(strings.Fields(nodeText(c)), " "))
	}
	return cells
}

func nodeText(n *nethtml.Node) string {
	if n.Type == nethtml.TextNode {
		return n.Data
	}
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		b.WriteString(" ")
		b.WriteString(nodeText(c))
	}
	return b.String()
}

func fetchBytes(ctx context.Context, client *http.Client, rawURL, accept string) ([]byte, time.Time, error) {
	req, err := newRequest(ctx, rawURL, accept)
	if err != nil {
		return nil, time.Time{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		statusErr := httpStatusError(rawURL, resp)
		if buf, err := curlFetchBytes(ctx, rawURL, accept); err == nil {
			return buf, time.Now().UTC(), nil
		}
		return nil, time.Time{}, statusErr
	}
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, time.Time{}, err
	}
	modified := time.Now().UTC()
	if h := strings.TrimSpace(resp.Header.Get("Last-Modified")); h != "" {
		if t, err := http.ParseTime(h); err == nil {
			modified = t.UTC()
		}
	}
	return buf, modified, nil
}

func curlFetchBytes(ctx context.Context, rawURL, accept string) ([]byte, error) {
	args := []string{
		"-fsSL",
		"--connect-timeout", "30",
		"--max-time", "120",
		"-A", "gordios-source-check/0.1",
		"-H", "Accept: " + accept,
		"-H", "Accept-Language: en-US,en;q=0.9",
		rawURL,
	}
	return exec.CommandContext(ctx, "curl", args...).Output()
}

func newRequest(ctx context.Context, rawURL, accept string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("User-Agent", "gordios-source-check/0.1")
	return req, nil
}

func httpStatusError(rawURL string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 400))
	return fmt.Errorf("%s -> %d: %s", rawURL, resp.StatusCode, strings.TrimSpace(string(body)))
}

func baseProps(originalName, endpoint string, ts time.Time, validFor time.Duration, basis string) map[string]any {
	if validFor <= 0 {
		validFor = 24 * time.Hour
	}
	return map[string]any{
		"source_original_name":      originalName,
		"source_api_endpoint":       endpoint,
		"source_payload_validity":   validity(ts, validFor, basis),
		"source_terms_note":         "Verify upstream source terms before redistributing source payloads.",
		"collected_as_raw_evidence": true,
	}
}

func validity(ts time.Time, d time.Duration, basis string) map[string]any {
	return map[string]any{
		"valid_start":    ts.UTC().Format(time.RFC3339),
		"valid_end":      ts.UTC().Add(d).Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func rowMap(headers, row []string) map[string]string {
	out := map[string]string{}
	for i, h := range headers {
		if h == "" || i >= len(row) {
			continue
		}
		out[h] = strings.Join(strings.Fields(strings.TrimSpace(row[i])), " ")
	}
	return out
}

func normalizeKey(s string) string {
	s = cleanHTML(s)
	s = regexp.MustCompile(`([a-z0-9])([A-Z])`).ReplaceAllString(s, `${1}_${2}`)
	s = strings.ToLower(s)
	var b strings.Builder
	prevUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

func htmlText(s string) string {
	s = regexp.MustCompile(`(?is)<script\b.*?</script>`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?is)<style\b.*?</style>`).ReplaceAllString(s, " ")
	return cleanHTML(s)
}

func cleanHTML(s string) string {
	s = regexp.MustCompile(`(?is)<script\b.*?</script>`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?is)<style\b.*?</style>`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?s)<[^>]+>`).ReplaceAllString(s, " ")
	s = stdhtml.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

func firstLinkMatching(buf []byte, base, needle string) string {
	links := extractLinks(buf, base)
	needle = strings.ToLower(needle)
	for _, l := range links {
		if strings.Contains(strings.ToLower(l.Text), needle) || strings.Contains(strings.ToLower(l.Href), needle) {
			return l.Href
		}
	}
	return ""
}

type pageLink struct {
	Text string
	Href string
}

func extractLinks(buf []byte, base string) []pageLink {
	root, err := nethtml.Parse(bytes.NewReader(buf))
	if err != nil {
		return nil
	}
	out := []pageLink{}
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n.Type == nethtml.ElementNode && n.Data == "a" {
			href := ""
			for _, a := range n.Attr {
				if strings.EqualFold(a.Key, "href") {
					href = a.Val
					break
				}
			}
			if href != "" {
				out = append(out, pageLink{Text: strings.Join(strings.Fields(nodeText(n)), " "), Href: absoluteURL(base, href)})
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return out
}

func absoluteURL(base, raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.IsAbs() {
		return u.String()
	}
	b, err := url.Parse(base)
	if err != nil {
		return raw
	}
	return b.ResolveReference(u).String()
}

func sourceDateOrModified(buf []byte, modified time.Time) time.Time {
	text := htmlText(string(buf))
	if m := regexp.MustCompile(`(?i)Last updated:\s*([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4})`).FindStringSubmatch(text); len(m) > 1 {
		if t := parseAnyDate(m[1]); !t.IsZero() {
			return day(t)
		}
	}
	if m := regexp.MustCompile(`(?i)\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b`).FindStringSubmatch(text); len(m) > 1 {
		if t := parseAnyDate(m[1]); !t.IsZero() {
			return day(t)
		}
	}
	return day(modified)
}

func firstMatch(s, pattern string) string {
	m := regexp.MustCompile(pattern).FindStringSubmatch(s)
	if len(m) == 0 {
		return ""
	}
	if len(m) < 2 {
		return cleanHTML(m[0])
	}
	return cleanHTML(m[1])
}

func maxPaginationPage(buf []byte) int {
	re := regexp.MustCompile(`(?i)[?&]page=(\d+)`)
	maxPage := 1
	for _, m := range re.FindAllSubmatch(buf, -1) {
		n, _ := strconv.Atoi(string(m[1]))
		if n > maxPage {
			maxPage = n
		}
	}
	return maxPage
}

func parseAnyDate(s string) time.Time {
	s = strings.TrimSpace(cleanHTML(s))
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000-07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05Z0700",
		"2006-01-02",
		"01/02/2006",
		"1/2/2006",
		"1/2/2006 3:04:05 PM",
		"2 January 2006",
		"02 January 2006",
		"Monday, January 2, 2006",
		"Monday, January 02, 2006",
		"January 2, 2006",
		"January 02, 2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func parseTimeTag(s string) time.Time {
	if m := regexp.MustCompile(`(?i)datetime="([^"]+)"`).FindStringSubmatch(s); len(m) > 1 {
		return parseAnyDate(m[1])
	}
	return parseAnyDate(s)
}

func parseLatLon(s string) (float64, float64) {
	parts := strings.Split(s, ",")
	if len(parts) < 2 {
		return 0, 0
	}
	lat, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lon, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil || !collectorutil.ValidLatLon(lat, lon) {
		return 0, 0
	}
	return lat, lon
}

func day(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func firstTime(values ...time.Time) time.Time {
	for _, v := range values {
		if !v.IsZero() {
			return v.UTC()
		}
	}
	return time.Now().UTC()
}

func contains(vals []string, want string) bool {
	for _, v := range vals {
		if v == want {
			return true
		}
	}
	return false
}

func nonEmptyValues(m map[string]string) int {
	n := 0
	for _, v := range m {
		if strings.TrimSpace(v) != "" {
			n++
		}
	}
	return n
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func stableID(s string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(s))))
	return fmt.Sprintf("%08x", h.Sum32())
}
