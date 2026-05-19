// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package global_disaster_reports ingests no-key global disaster report and
// activation feeds that add humanitarian, satellite-assessment, and volcano
// context outside the United States.
package global_disaster_reports

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"html"
	"math"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	propx "github.com/gordios45/collector/internal/props"
)

const (
	sourceID         = "global_disaster_reports"
	reliefWebCSV     = "https://data.humdata.org/dataset/7117aece-abc5-47b8-b5a1-6308db53e7f8/resource/7303ffbd-c0a9-47f7-9664-2d728a25288b/download/reliefweb-disasters-list.csv"
	unosatSearch     = "https://data.humdata.org/api/3/action/package_search?fq=organization:unosat&sort=metadata_modified%20desc&rows=25"
	charterURL       = "https://disasterscharter.org/activations"
	smithsonianRSS   = "https://volcano.si.edu/news/WeeklyVolcanoRSS.xml"
	countriesURL     = "https://goadmin.ifrc.org/api/v2/country/?limit=400"
	defaultPublicURL = "https://data.humdata.org/"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, fetchSmithsonian(ctx)...)
	out = append(out, fetchCharter(ctx)...)
	out = append(out, fetchReliefWeb(ctx)...)
	countries, _ := fetchCountryCentroids(ctx)
	out = append(out, fetchUNOSAT(ctx, countries)...)
	return dedupe(out), nil
}

func fetchReliefWeb(ctx context.Context) []events.Event {
	buf, err := getBytes(ctx, reliefWebCSV)
	if err != nil {
		return nil
	}
	r := csv.NewReader(bytes.NewReader(buf))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil || len(rows) < 2 {
		return nil
	}
	return reliefWebEventsFromRows(rows, time.Now().UTC(), 180)
}

type reliefWebCSVRow struct {
	row       []string
	changed   time.Time
	eventDate time.Time
	sortTime  time.Time
}

func reliefWebEventsFromRows(rows [][]string, now time.Time, limit int) []events.Event {
	if len(rows) < 2 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	header := rows[0]
	idx := map[string]int{}
	for i, h := range header {
		idx[h] = i
	}

	candidates := make([]reliefWebCSVRow, 0, len(rows)-1)
	for _, row := range rows[1:] {
		status := strings.ToLower(cell(row, idx, "status"))
		changed := parseTime(cell(row, idx, "date-changed"))
		eventDate := firstTime(cell(row, idx, "date-event"), cell(row, idx, "date-created"), cell(row, idx, "date-changed"))
		if status == "past" && changed.Before(now.AddDate(-1, 0, 0)) && eventDate.Before(now.AddDate(-1, 0, 0)) {
			continue
		}
		sortTime := firstNonZero(changed, eventDate)
		if sortTime.IsZero() {
			sortTime = now
		}
		candidates = append(candidates, reliefWebCSVRow{row: row, changed: changed, eventDate: eventDate, sortTime: sortTime})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].sortTime.After(candidates[j].sortTime)
	})

	if limit <= 0 {
		limit = 180
	}
	out := make([]events.Event, 0, minInt(limit, len(candidates)))
	for _, candidate := range candidates {
		row := candidate.row
		changed := candidate.changed
		eventDate := candidate.eventDate
		status := strings.ToLower(cell(row, idx, "status"))
		lat, latOK := parseFloat(cell(row, idx, "country-location-lat"))
		lon, lonOK := parseFloat(cell(row, idx, "country-location-lon"))
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			lat, latOK = parseFloat(cell(row, idx, "primary_country-location-lat"))
			lon, lonOK = parseFloat(cell(row, idx, "primary_country-location-lon"))
		}
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		ts := firstNonZero(changed, eventDate, now)
		typ := firstNonEmpty(cell(row, idx, "type-name"), cell(row, idx, "primary_type-name"), "disaster")
		name := firstNonEmpty(cell(row, idx, "name"), typ)
		country := firstNonEmpty(cell(row, idx, "country-name"), cell(row, idx, "primary_country-name"))
		iso3 := firstNonEmpty(cell(row, idx, "country-iso3"), cell(row, idx, "primary_country-iso3"))
		props := map[string]any{
			"source_provider":           "ReliefWeb HDX disaster list",
			"source_api_endpoint":       reliefWebCSV,
			"source_public_url":         firstNonEmpty(cell(row, idx, "url_alias"), cell(row, idx, "url"), "https://reliefweb.int/disasters"),
			"source_provider_kind":      "humanitarian_disaster_report_index",
			"report_kind":               "reliefweb_hdx_disaster",
			"report_id":                 cell(row, idx, "id"),
			"title":                     name,
			"description":               cell(row, idx, "description"),
			"status":                    status,
			"glide":                     cell(row, idx, "glide"),
			"disaster_type":             typ,
			"disaster_type_code":        firstNonEmpty(cell(row, idx, "type-code"), cell(row, idx, "primary_type-code")),
			"country":                   country,
			"country_iso3":              iso3,
			"date_changed":              cell(row, idx, "date-changed"),
			"date_created":              cell(row, idx, "date-created"),
			"date_event":                cell(row, idx, "date-event"),
			"labels":                    labelsFor(typ + " " + name + " " + cell(row, idx, "description")),
			"humanitarian_report_score": humanitarianReportScore(status, typ, changed, eventDate),
			"source_payload_validity":   validity(ts, ts.Add(30*24*time.Hour), "reliefweb_disaster_report_context_window"),
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: sourceID,
			ExtID:  "reliefweb:" + firstNonEmpty(cell(row, idx, "id"), stableID(name+iso3)),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func fetchCharter(ctx context.Context) []events.Event {
	buf, err := getBytes(ctx, charterURL)
	if err != nil {
		return nil
	}
	return parseCharterActivations(string(buf), 80)
}

var activationIDRe = regexp.MustCompile(`activationId":"([^"]+)"`)

func parseCharterActivations(raw string, limit int) []events.Event {
	raw = strings.ReplaceAll(raw, `\"`, `"`)
	idxs := activationIDRe.FindAllStringSubmatchIndex(raw, -1)
	out := make([]events.Event, 0, minInt(len(idxs), limit))
	for i, idx := range idxs {
		if len(out) >= limit {
			break
		}
		start := idx[0]
		end := len(raw)
		if i+1 < len(idxs) {
			end = idxs[i+1][0]
		} else if start+4000 < end {
			end = start + 4000
		}
		seg := raw[start:end]
		id := stringField(seg, "activationId")
		title := stringField(seg, "title")
		lat, latOK := parseFloat(stringField(seg, "centerPointLatitude"))
		lon, lonOK := parseFloat(stringField(seg, "centerPointLongitude"))
		if id == "" || !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		ts := parseMillisField(seg, "dateAsTimestamp")
		if ts.IsZero() {
			ts = parseTime(stringField(seg, "date"))
		}
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		disasterType := firstNonEmpty(disasterTypeFromCharter(seg), keywordDisasterType(title), "disaster")
		country := stringField(seg, "country")
		slug := stringField(seg, "slug")
		props := map[string]any{
			"source_provider":             "International Charter Space and Major Disasters",
			"source_api_endpoint":         charterURL,
			"source_public_url":           charterURL,
			"source_provider_kind":        "satellite_disaster_activation_index",
			"report_kind":                 "disaster_charter_activation",
			"activation_id":               id,
			"external_reference_code":     stringField(seg, "externalReferenceCode"),
			"title":                       title,
			"country":                     country,
			"slug":                        slug,
			"disaster_type":               disasterType,
			"date":                        stringField(seg, "date"),
			"date_as_timestamp":           stringField(seg, "dateAsTimestamp"),
			"labels":                      labelsFor(disasterType + " " + title),
			"satellite_activation_score":  1.8,
			"humanitarian_report_score":   1.2,
			"source_payload_validity":     validity(ts, ts.Add(21*24*time.Hour), "disaster_charter_activation_context_window"),
			"satellite_context_available": true,
		}
		out = append(out, events.Event{Ts: ts, Source: sourceID, ExtID: "charter:" + id, Lat: lat, Lon: lon, Props: props})
	}
	return out
}

func fetchUNOSAT(ctx context.Context, countries map[string]countryPoint) []events.Event {
	var raw ckanSearch
	if err := getJSON(ctx, unosatSearch, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Result.Results))
	for _, pkg := range raw.Result.Results {
		countryName, iso3 := packageCountry(pkg)
		pt, ok := lookupCountry(countries, countryName, iso3)
		if !ok {
			continue
		}
		ts := firstTime(pkg.LastModified, pkg.MetadataModified, pkg.DatasetDateStart(), pkg.MetadataCreated)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		text := pkg.Title + " " + pkg.Notes
		typ := keywordDisasterType(text)
		if typ == "" {
			typ = "satellite damage assessment"
		}
		props := map[string]any{
			"source_provider":                    "HDX UNOSAT",
			"source_api_endpoint":                unosatSearch,
			"source_public_url":                  "https://data.humdata.org/dataset/" + pkg.Name,
			"source_provider_kind":               "satellite_damage_assessment_catalog",
			"report_kind":                        "unosat_hdx_dataset",
			"dataset_id":                         pkg.ID,
			"dataset_name":                       pkg.Name,
			"title":                              pkg.Title,
			"description":                        pkg.Notes,
			"country":                            countryName,
			"country_iso3":                       strings.ToUpper(iso3),
			"disaster_type":                      typ,
			"dataset_date":                       pkg.DatasetDate,
			"last_modified":                      pkg.LastModified,
			"metadata_modified":                  pkg.MetadataModified,
			"resource_count":                     len(pkg.Resources),
			"labels":                             labelsFor(text),
			"satellite_damage_assessment_score":  satelliteAssessmentScore(text),
			"humanitarian_report_score":          0.9,
			"source_payload_validity":            validity(ts, ts.Add(30*24*time.Hour), "unosat_hdx_dataset_context_window"),
			"satellite_context_available":        true,
			"official_remote_sensing_assessment": true,
		}
		out = append(out, events.Event{Ts: ts, Source: sourceID, ExtID: "unosat:" + firstNonEmpty(pkg.ID, pkg.Name), Lat: pt.Lat, Lon: pt.Lon, Props: props})
	}
	return out
}

type ckanSearch struct {
	Success bool `json:"success"`
	Result  struct {
		Results []ckanPackage `json:"results"`
	} `json:"result"`
}

type ckanPackage struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Title            string         `json:"title"`
	Notes            string         `json:"notes"`
	DatasetDate      string         `json:"dataset_date"`
	LastModified     string         `json:"last_modified"`
	MetadataCreated  string         `json:"metadata_created"`
	MetadataModified string         `json:"metadata_modified"`
	Groups           []ckanGroup    `json:"groups"`
	Resources        []ckanResource `json:"resources"`
}

func (p ckanPackage) DatasetDateStart() string {
	s := strings.TrimSpace(p.DatasetDate)
	if strings.HasPrefix(s, "[") && strings.Contains(s, " TO ") {
		s = strings.TrimPrefix(s, "[")
		return strings.TrimSpace(strings.SplitN(s, " TO ", 2)[0])
	}
	return s
}

type ckanGroup struct {
	Name  string `json:"name"`
	Title string `json:"title"`
}

type ckanResource struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	DownloadURL string `json:"download_url"`
	Format      string `json:"format"`
}

func packageCountry(pkg ckanPackage) (string, string) {
	for _, g := range pkg.Groups {
		name := firstNonEmpty(g.Title, g.Name)
		if name != "" {
			return name, strings.ToUpper(g.Name)
		}
	}
	return "", ""
}

func fetchSmithsonian(ctx context.Context) []events.Event {
	buf, err := getBytes(ctx, smithsonianRSS)
	if err != nil {
		return nil
	}
	var raw volcanoRSS
	if err := xml.Unmarshal(buf, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Items))
	for _, it := range raw.Items {
		lat, lon, ok := parsePoint(it.Point)
		if !ok {
			continue
		}
		ts := parseTime(it.PubDate)
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		desc := stripHTML(it.Description)
		score := volcanoActivityScore(it.Title + " " + desc)
		props := map[string]any{
			"source_provider":           "Smithsonian / USGS Weekly Volcanic Activity Report",
			"source_api_endpoint":       smithsonianRSS,
			"source_public_url":         firstNonEmpty(it.Link, "https://volcano.si.edu/reports_weekly.cfm"),
			"source_provider_kind":      "official_volcano_activity_report",
			"report_kind":               "smithsonian_usgs_weekly_volcano",
			"title":                     strings.TrimSpace(it.Title),
			"description":               desc,
			"guid":                      it.GUID,
			"pub_date":                  it.PubDate,
			"disaster_type":             "volcano",
			"labels":                    labelsFor("volcano eruption ash " + it.Title + " " + desc),
			"volcano_activity_score":    score,
			"humanitarian_report_score": 0.4,
			"source_payload_validity":   validity(ts, ts.Add(14*24*time.Hour), "weekly_volcano_report_context_window"),
		}
		out = append(out, events.Event{Ts: ts, Source: sourceID, ExtID: "smithsonian:" + firstNonEmpty(it.GUID, stableID(it.Title)), Lat: lat, Lon: lon, Props: props})
	}
	return out
}

type volcanoRSS struct {
	Items []volcanoItem `xml:"channel>item"`
}

type volcanoItem struct {
	Title       string `xml:"title"`
	Description string `xml:"description"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	PubDate     string `xml:"pubDate"`
	Point       string `xml:"http://www.georss.org/georss point"`
}

func fetchCountryCentroids(ctx context.Context) (map[string]countryPoint, error) {
	var raw countryResponse
	if err := getJSON(ctx, countriesURL, &raw); err != nil {
		return fallbackCountries(), err
	}
	out := fallbackCountries()
	for _, c := range raw.Results {
		if len(c.Centroid.Coordinates) < 2 {
			continue
		}
		lon, lat := c.Centroid.Coordinates[0], c.Centroid.Coordinates[1]
		if !validLatLon(lat, lon) {
			continue
		}
		pt := countryPoint{Lat: lat, Lon: lon, Name: c.Name}
		if c.ISO3 != "" {
			out[strings.ToUpper(c.ISO3)] = pt
		}
		if c.ISO != "" {
			out[strings.ToUpper(c.ISO)] = pt
		}
		if c.Name != "" {
			out[strings.ToLower(c.Name)] = pt
		}
	}
	return out, nil
}

func getBytes(ctx context.Context, rawURL string) ([]byte, error) {
	return exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "45", "-A", "gordios/0.1", rawURL).Output()
}

func getJSON(ctx context.Context, rawURL string, out any) error {
	buf, err := getBytes(ctx, rawURL)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

type countryResponse struct {
	Results []countryRecord `json:"results"`
}

type countryRecord struct {
	ISO      string `json:"iso"`
	ISO3     string `json:"iso3"`
	Name     string `json:"name"`
	Centroid struct {
		Coordinates []float64 `json:"coordinates"`
	} `json:"centroid"`
}

type countryPoint struct {
	Lat  float64
	Lon  float64
	Name string
}

func lookupCountry(countries map[string]countryPoint, name, iso3 string) (countryPoint, bool) {
	for _, key := range []string{strings.ToUpper(iso3), strings.ToLower(name), strings.ToUpper(name)} {
		if pt, ok := countries[key]; ok {
			return pt, true
		}
	}
	return countryPoint{}, false
}

func fallbackCountries() map[string]countryPoint {
	return map[string]countryPoint{
		"SLB":             {-9.65, 160.16, "Solomon Islands"},
		"solomon islands": {-9.65, 160.16, "Solomon Islands"},
		"PHL":             {12.88, 121.77, "Philippines"},
		"philippines":     {12.88, 121.77, "Philippines"},
		"IDN":             {-2.55, 118.01, "Indonesia"},
		"indonesia":       {-2.55, 118.01, "Indonesia"},
		"PER":             {-9.19, -75.02, "Peru"},
		"peru":            {-9.19, -75.02, "Peru"},
		"ECU":             {-1.83, -78.18, "Ecuador"},
		"ecuador":         {-1.83, -78.18, "Ecuador"},
	}
}

func labelsFor(text string) []string {
	l := strings.ToLower(text)
	out := []string{"disaster_report"}
	if containsAny(l, "flood", "inundation", "flash flood") {
		out = append(out, "flood")
	}
	if containsAny(l, "cyclone", "hurricane", "typhoon", "storm") {
		out = append(out, "tropical_cyclone", "severe_weather")
	}
	if containsAny(l, "wildfire", "forest fire", "bushfire") {
		out = append(out, "wildfire")
	}
	if containsAny(l, "earthquake", "tsunami", "volcano", "eruption", "ash") {
		out = append(out, "geological_hazard")
	}
	if containsAny(l, "damage assessment", "damaged building", "satellite", "unosat") {
		out = append(out, "satellite_assessment")
	}
	if containsAny(l, "outbreak", "epidemic", "cholera", "dengue") {
		out = append(out, "health_outbreak")
	}
	return unique(out)
}

func humanitarianReportScore(status, typ string, changed, eventDate time.Time) float64 {
	score := 0.4
	switch strings.ToLower(status) {
	case "alert":
		score += 1.4
	case "current", "ongoing":
		score += 1.0
	case "past":
		score += 0.1
	}
	l := strings.ToLower(typ)
	if containsAny(l, "flood", "earthquake", "cyclone", "typhoon", "hurricane", "wildfire", "volcano") {
		score += 0.4
	}
	recent := firstNonZero(changed, eventDate)
	if !recent.IsZero() && recent.After(time.Now().UTC().AddDate(0, -1, 0)) {
		score += 0.5
	}
	return propx.ClampFloat(score, 0, 3)
}

func satelliteAssessmentScore(text string) float64 {
	l := strings.ToLower(text)
	score := 1.0
	if containsAny(l, "damage assessment", "damaged building", "destroyed", "potentially damaged") {
		score += 1.0
	}
	if containsAny(l, "flood", "water extent", "inundation") {
		score += 0.5
	}
	if containsAny(l, "as of", "preliminary") {
		score += 0.2
	}
	return propx.ClampFloat(score, 0, 3)
}

func volcanoActivityScore(text string) float64 {
	l := strings.ToLower(text)
	score := 0.8
	if containsAny(l, "new eruptive activity", "ash plume", "eruption", "explosive") {
		score += 1.0
	}
	if containsAny(l, "continuing eruptive activity", "alert level", "vaac") {
		score += 0.5
	}
	return propx.ClampFloat(score, 0, 3)
}

func keywordDisasterType(text string) string {
	l := strings.ToLower(text)
	switch {
	case containsAny(l, "flood", "inundation"):
		return "flood"
	case containsAny(l, "cyclone", "hurricane", "typhoon"):
		return "tropical cyclone"
	case containsAny(l, "wildfire", "forest fire", "bushfire"):
		return "wildfire"
	case containsAny(l, "earthquake"):
		return "earthquake"
	case containsAny(l, "volcano", "volcanic", "eruption", "ash"):
		return "volcano"
	case containsAny(l, "landslide", "mudslide"):
		return "landslide"
	case containsAny(l, "conflict", "explosion"):
		return "complex emergency"
	default:
		return ""
	}
}

func disasterTypeFromCharter(seg string) string {
	if m := regexp.MustCompile(`"disasterTypes":\[(.*?)\]`).FindStringSubmatch(seg); len(m) > 1 {
		if title := stringField(m[1], "title"); title != "" {
			return title
		}
	}
	return ""
}

func stringField(seg, key string) string {
	re := regexp.MustCompile(`"?` + regexp.QuoteMeta(key) + `"?\s*:\s*"([^"]*)"`)
	if m := re.FindStringSubmatch(seg); len(m) > 1 {
		return html.UnescapeString(strings.TrimSpace(m[1]))
	}
	re = regexp.MustCompile(`"?` + regexp.QuoteMeta(key) + `"?\s*:\s*([^,}\]]+)`)
	if m := re.FindStringSubmatch(seg); len(m) > 1 {
		return strings.Trim(strings.TrimSpace(m[1]), `"`)
	}
	return ""
}

func parseMillisField(seg, key string) time.Time {
	raw := stringField(seg, key)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(n).UTC()
}

func cell(row []string, idx map[string]int, key string) string {
	i, ok := idx[key]
	if !ok || i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func parsePoint(s string) (float64, float64, bool) {
	parts := strings.Fields(strings.ReplaceAll(strings.TrimSpace(s), ",", " "))
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(parts[0], 64)
	lon, err2 := strconv.ParseFloat(parts[1], 64)
	return lat, lon, err1 == nil && err2 == nil && validLatLon(lat, lon)
}

func parseFloat(s string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f, err == nil
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") {
		return time.Time{}
	}
	if strings.HasPrefix(s, "[") && strings.Contains(s, " TO ") {
		s = strings.TrimPrefix(strings.SplitN(s, " TO ", 2)[0], "[")
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 100000000000 {
			return time.UnixMilli(n).UTC()
		}
		if n > 1000000000 {
			return time.Unix(n, 0).UTC()
		}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339, time.RFC1123Z, time.RFC1123,
		"2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstTime(vals ...string) time.Time {
	for _, v := range vals {
		if t := parseTime(v); !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func firstNonZero(vals ...time.Time) time.Time {
	for _, t := range vals {
		if !t.IsZero() {
			return t.UTC()
		}
	}
	return time.Time{}
}

func validity(start, end time.Time, basis string) map[string]any {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	if end.IsZero() || end.Before(start) {
		end = start.Add(14 * 24 * time.Hour)
	}
	return map[string]any{
		"valid_start":    start.Format(time.RFC3339),
		"valid_end":      end.Format(time.RFC3339),
		"validity_basis": basis,
	}
}

func stripHTML(s string) string {
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(tagRe.ReplaceAllString(s, " ")), " ")
}

var tagRe = regexp.MustCompile(`<[^>]+>`)

func containsAny(s string, subs ...string) bool {
	s = strings.ToLower(s)
	for _, sub := range subs {
		if strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func unique(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.Source == "" || e.ExtID == "" || !e.HasPoint() {
			continue
		}
		key := e.Source + ":" + e.ExtID
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, e)
	}
	return out
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.ToLower(strings.TrimSpace(s))))
	return hex.EncodeToString(h[:])
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
