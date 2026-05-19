// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package official_advisories ingests no-token official security advisory
// streams that are narrower and fresher than country-level travel pages:
// US embassy alert RSS feeds and Australia's Smartraveller destination API.
package official_advisories

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const smartravellerURL = "https://www.smartraveller.gov.au/destinations-export"

type feedSpec struct {
	Name          string
	URL           string
	SourceCountry string
	TargetCountry string
	DefaultCity   string
}

var embassyFeeds = []feedSpec{
	{Name: "US Embassy Thailand", SourceCountry: "US", TargetCountry: "TH", DefaultCity: "Bangkok", URL: "https://th.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy UAE", SourceCountry: "US", TargetCountry: "AE", DefaultCity: "Abu Dhabi", URL: "https://ae.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Germany", SourceCountry: "US", TargetCountry: "DE", DefaultCity: "Berlin", URL: "https://de.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Ukraine", SourceCountry: "US", TargetCountry: "UA", DefaultCity: "Kyiv", URL: "https://ua.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Mexico", SourceCountry: "US", TargetCountry: "MX", DefaultCity: "Mexico City", URL: "https://mx.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy India", SourceCountry: "US", TargetCountry: "IN", DefaultCity: "New Delhi", URL: "https://in.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Pakistan", SourceCountry: "US", TargetCountry: "PK", DefaultCity: "Islamabad", URL: "https://pk.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Colombia", SourceCountry: "US", TargetCountry: "CO", DefaultCity: "Bogota", URL: "https://co.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Poland", SourceCountry: "US", TargetCountry: "PL", DefaultCity: "Warsaw", URL: "https://pl.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Bangladesh", SourceCountry: "US", TargetCountry: "BD", DefaultCity: "Dhaka", URL: "https://bd.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Italy", SourceCountry: "US", TargetCountry: "IT", DefaultCity: "Rome", URL: "https://it.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Dominican Republic", SourceCountry: "US", TargetCountry: "DO", DefaultCity: "Santo Domingo", URL: "https://do.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Myanmar", SourceCountry: "US", TargetCountry: "MM", DefaultCity: "Yangon", URL: "https://mm.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Israel", SourceCountry: "US", TargetCountry: "IL", DefaultCity: "Jerusalem", URL: "https://il.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Lebanon", SourceCountry: "US", TargetCountry: "LB", DefaultCity: "Beirut", URL: "https://lb.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Iraq", SourceCountry: "US", TargetCountry: "IQ", DefaultCity: "Baghdad", URL: "https://iq.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Jordan", SourceCountry: "US", TargetCountry: "JO", DefaultCity: "Amman", URL: "https://jo.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Egypt", SourceCountry: "US", TargetCountry: "EG", DefaultCity: "Cairo", URL: "https://eg.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Kenya", SourceCountry: "US", TargetCountry: "KE", DefaultCity: "Nairobi", URL: "https://ke.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Nigeria", SourceCountry: "US", TargetCountry: "NG", DefaultCity: "Abuja", URL: "https://ng.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy South Africa", SourceCountry: "US", TargetCountry: "ZA", DefaultCity: "Pretoria", URL: "https://za.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Philippines", SourceCountry: "US", TargetCountry: "PH", DefaultCity: "Manila", URL: "https://ph.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy South Korea", SourceCountry: "US", TargetCountry: "KR", DefaultCity: "Seoul", URL: "https://kr.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Japan", SourceCountry: "US", TargetCountry: "JP", DefaultCity: "Tokyo", URL: "https://jp.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy France", SourceCountry: "US", TargetCountry: "FR", DefaultCity: "Paris", URL: "https://fr.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy United Kingdom", SourceCountry: "US", TargetCountry: "GB", DefaultCity: "London", URL: "https://uk.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Turkey", SourceCountry: "US", TargetCountry: "TR", DefaultCity: "Ankara", URL: "https://tr.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Spain", SourceCountry: "US", TargetCountry: "ES", DefaultCity: "Madrid", URL: "https://es.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Greece", SourceCountry: "US", TargetCountry: "GR", DefaultCity: "Athens", URL: "https://gr.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Peru", SourceCountry: "US", TargetCountry: "PE", DefaultCity: "Lima", URL: "https://pe.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Ecuador", SourceCountry: "US", TargetCountry: "EC", DefaultCity: "Quito", URL: "https://ec.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Haiti", SourceCountry: "US", TargetCountry: "HT", DefaultCity: "Port-au-Prince", URL: "https://ht.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Guatemala", SourceCountry: "US", TargetCountry: "GT", DefaultCity: "Guatemala City", URL: "https://gt.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy El Salvador", SourceCountry: "US", TargetCountry: "SV", DefaultCity: "San Salvador", URL: "https://sv.usembassy.gov/category/alert/feed/"},
	{Name: "US Embassy Venezuela", SourceCountry: "US", TargetCountry: "VE", DefaultCity: "Caracas", URL: "https://ve.usembassy.gov/category/alert/feed/"},
}

var htmlTagRE = regexp.MustCompile(`<[^>]+>`)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "official_advisories" }
func (c *Collector) PollEvery() time.Duration { return 2 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	out := []events.Event{}
	out = append(out, c.fetchSmartraveller(ctx)...)
	for _, f := range append(embassyFeeds, extraEmbassyFeeds()...) {
		out = append(out, c.fetchFeed(ctx, f)...)
	}
	return dedupe(out), nil
}

func (c *Collector) fetchFeed(ctx context.Context, f feedSpec) []events.Event {
	buf, err := httpx.GetBytes(ctx, f.URL, map[string]string{
		"Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
	})
	if err != nil {
		return nil
	}
	var env feedEnv
	if err := xml.Unmarshal(buf, &env); err != nil {
		return nil
	}
	out := []events.Event{}
	for _, it := range env.Items {
		out = append(out, eventFromFeedItem(f, it.Title, strip(it.Description), it.Link, firstNonEmpty(it.PubDate, it.Date), it.GUID))
	}
	for _, e := range env.Entries {
		body := e.Summary
		if body == "" {
			body = e.Content
		}
		out = append(out, eventFromFeedItem(f, e.Title, strip(body), firstNonEmpty(e.Link.Href, e.ID), firstNonEmpty(e.Updated, e.Published), e.ID))
	}
	return out
}

func eventFromFeedItem(f feedSpec, title, desc, link, date, guid string) events.Event {
	cc := f.TargetCountry
	lat, lon, country, ok := countryForCode(cc)
	if !ok {
		return events.Event{}
	}
	extracted := extractEmbassyAlert(f, country, title, desc)
	if extracted.Lat != 0 || extracted.Lon != 0 {
		lat, lon = extracted.Lat, extracted.Lon
	}
	ts := parseTime(date)
	id := firstNonEmpty(guid, link, f.Name+":"+title)
	props := map[string]any{
		"title":                           strings.TrimSpace(title),
		"description":                     desc,
		"link":                            strings.TrimSpace(link),
		"source":                          f.Name,
		"source_country":                  f.SourceCountry,
		"country":                         country,
		"country_code":                    cc,
		"date":                            date,
		"advisory_type":                   "embassy_alert",
		"alert_kind":                      extracted.Kind,
		"severity":                        extracted.Severity,
		"severity_score":                  extracted.Score,
		"city":                            extracted.City,
		"location_text":                   extracted.LocationText,
		"action_flags":                    extracted.Actions,
		"hazard_flags":                    extracted.Hazards,
		"deterministic_extractor_version": "embassy_alert_rules_v1",
		"source_api_endpoint":             f.URL,
		"source_payload_validity": map[string]any{
			"valid_start":    ts.Format(time.RFC3339),
			"valid_end":      ts.Add(72 * time.Hour).Format(time.RFC3339),
			"validity_basis": "embassy_alert_publication_time",
		},
	}
	return events.Event{Ts: ts, Source: "official_advisories", ExtID: stableID("embassy:" + id), Lat: lat, Lon: lon, Props: props}
}

func (c *Collector) fetchSmartraveller(ctx context.Context) []events.Event {
	buf, err := httpx.GetBytes(ctx, smartravellerURL, map[string]string{"Accept": "application/json,*/*"})
	if err != nil {
		return nil
	}
	records, err := parseSmartraveller(buf)
	if err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(records))
	for _, r := range records {
		lat, lon, country, ok := findCountry(r.Title)
		if !ok {
			continue
		}
		ts := parseTime(firstNonEmpty(r.Changed, r.Updated))
		link := r.URL
		if link != "" && !strings.HasPrefix(link, "http") {
			link = "https://www.smartraveller.gov.au" + link
		}
		level := strings.TrimSpace(firstNonEmpty(r.OverallAdviceLevel, r.AdviceLevel, r.Level))
		if level == "" {
			level = classifyLevel(r.Title + " " + r.Summary)
		}
		props := map[string]any{
			"title":               strings.TrimSpace(r.Title),
			"description":         strings.TrimSpace(strip(r.Summary)),
			"link":                link,
			"source":              "Australia Smartraveller",
			"source_country":      "AU",
			"country":             country,
			"advisory_level":      level,
			"date":                firstNonEmpty(r.Changed, r.Updated),
			"advisory_type":       "travel_security_advice",
			"source_api_endpoint": smartravellerURL,
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "official_advisories",
			ExtID:  stableID("smartraveller:" + firstNonEmpty(r.ID, r.URL, r.Title)),
			Lat:    lat,
			Lon:    lon,
			Props:  props,
		})
	}
	return out
}

type smartRecord struct {
	ID                 string `json:"id"`
	Title              string `json:"title"`
	URL                string `json:"url"`
	Summary            string `json:"summary"`
	Changed            string `json:"changed"`
	Updated            string `json:"updated"`
	OverallAdviceLevel string `json:"overall_advice_level"`
	AdviceLevel        string `json:"advice_level"`
	Level              string `json:"level"`
}

func parseSmartraveller(buf []byte) ([]smartRecord, error) {
	var arr []smartRecord
	if err := json.Unmarshal(buf, &arr); err == nil {
		return arr, nil
	}
	var wrapped struct {
		Data []smartRecord `json:"data"`
	}
	if err := json.Unmarshal(buf, &wrapped); err != nil {
		return nil, err
	}
	return wrapped.Data, nil
}

type rssItem struct {
	XMLName     xml.Name `xml:"item"`
	Title       string   `xml:"title"`
	Link        string   `xml:"link"`
	Description string   `xml:"description"`
	PubDate     string   `xml:"pubDate"`
	Date        string   `xml:"date"`
	GUID        string   `xml:"guid"`
}

type atomEntry struct {
	XMLName   xml.Name `xml:"entry"`
	Title     string   `xml:"title"`
	Updated   string   `xml:"updated"`
	Published string   `xml:"published"`
	ID        string   `xml:"id"`
	Summary   string   `xml:"summary"`
	Content   string   `xml:"content"`
	Link      struct {
		Href string `xml:"href,attr"`
	} `xml:"link"`
}

type feedEnv struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.ExtID == "" || e.Source == "" || !e.HasPoint() {
			continue
		}
		k := e.Source + "|" + e.ExtID
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, e)
	}
	return out
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	for _, layout := range []string{
		time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"2006-01-02 15:04:05", "2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func classifyLevel(text string) string {
	t := strings.ToLower(text)
	switch {
	case strings.Contains(t, "do not travel"):
		return "do_not_travel"
	case strings.Contains(t, "reconsider"):
		return "reconsider"
	case strings.Contains(t, "exercise") || strings.Contains(t, "high degree of caution"):
		return "caution"
	default:
		return "info"
	}
}

type extractedAlert struct {
	Kind         string
	Severity     string
	Score        float64
	City         string
	Lat          float64
	Lon          float64
	LocationText string
	Actions      []string
	Hazards      []string
}

func extractEmbassyAlert(f feedSpec, country, title, desc string) extractedAlert {
	full := normalizeSpace(title + " " + desc)
	locationText := labeledSection(full, "location")
	searchText := full
	if locationText != "" {
		searchText = locationText + " " + title
	}
	city, lat, lon := matchCity(f.TargetCountry, searchText)
	if city == "" && f.DefaultCity != "" {
		if c, ok := cityByCountry[strings.ToUpper(f.TargetCountry)][strings.ToLower(f.DefaultCity)]; ok {
			city, lat, lon = c.Name, c.Lat, c.Lon
		}
	}
	kind := alertKind(title + " " + desc)
	actions := matchedTerms(full, actionRules)
	hazards := matchedTerms(full, hazardRules)
	score, severity := severityFor(kind, actions, hazards, full)
	return extractedAlert{
		Kind: kind, Severity: severity, Score: score,
		City: city, Lat: lat, Lon: lon, LocationText: locationText,
		Actions: actions, Hazards: hazards,
	}
}

func alertKind(text string) string {
	t := strings.ToLower(text)
	switch {
	case strings.Contains(t, "demonstration alert") || strings.Contains(t, "demonstration"):
		return "demonstration"
	case strings.Contains(t, "security alert") || strings.Contains(t, "security message"):
		return "security"
	case strings.Contains(t, "weather alert") || strings.Contains(t, "natural disaster"):
		return "weather"
	case strings.Contains(t, "health alert"):
		return "health"
	case strings.Contains(t, "worldwide caution"):
		return "worldwide_caution"
	default:
		return "message"
	}
}

func severityFor(kind string, actions, hazards []string, text string) (float64, string) {
	score := 0.35
	switch kind {
	case "security":
		score += 0.75
	case "demonstration":
		score += 0.55
	case "weather", "health":
		score += 0.35
	case "worldwide_caution":
		score += 0.9
	}
	if contains(actions, "shelter_in_place") || contains(actions, "evacuate") || contains(actions, "ordered_departure") {
		score += 1.7
	}
	if contains(actions, "avoid_area") || contains(actions, "limit_movement") || contains(actions, "curfew") {
		score += 0.9
	}
	if containsAny(strings.ToLower(text), "u.s. government personnel", "government personnel are prohibited", "ordered to") {
		score += 0.5
	}
	for _, h := range hazards {
		switch h {
		case "missile_or_drone", "terrorism", "explosion", "armed_conflict":
			score += 0.8
		case "security_operations", "civil_unrest", "crime", "roadblocks":
			if kind == "demonstration" && h == "civil_unrest" {
				continue
			}
			score += 0.45
		case "weather_hazard":
			score += 0.25
		}
	}
	score = clamp(score, 0, 3)
	switch {
	case score >= 2.4:
		return score, "severe"
	case score >= 1.5:
		return score, "elevated"
	case score >= 0.8:
		return score, "caution"
	default:
		return score, "info"
	}
}

type termRule struct {
	ID    string
	Terms []string
}

var actionRules = []termRule{
	{ID: "shelter_in_place", Terms: []string{"shelter in place", "remain indoors", "stay indoors"}},
	{ID: "evacuate", Terms: []string{"evacuate", "evacuation", "depart immediately"}},
	{ID: "ordered_departure", Terms: []string{"ordered departure", "authorized departure"}},
	{ID: "avoid_area", Terms: []string{"avoid the area", "avoid areas", "avoid demonstrations", "avoid crowds", "do not go to"}},
	{ID: "limit_movement", Terms: []string{"limit movement", "restrict movement", "avoid non-essential travel", "avoid nonessential travel"}},
	{ID: "curfew", Terms: []string{"curfew"}},
	{ID: "monitor_local_media", Terms: []string{"monitor local media", "monitor the media"}},
}

var hazardRules = []termRule{
	{ID: "missile_or_drone", Terms: []string{"missile", "rocket", "drone", "uav", "projectile"}},
	{ID: "terrorism", Terms: []string{"terrorist", "terrorism", "terror attack", "attack is highly likely"}},
	{ID: "explosion", Terms: []string{"explosion", "blast", "bombing"}},
	{ID: "armed_conflict", Terms: []string{"armed conflict", "airstrike", "gunfire", "shooting", "hostilities"}},
	{ID: "civil_unrest", Terms: []string{"demonstration", "protest", "civil unrest", "riot", "unrest"}},
	{ID: "security_operations", Terms: []string{"security operation", "military operation", "police operation"}},
	{ID: "roadblocks", Terms: []string{"roadblock", "road block", "checkpoints", "checkpoint"}},
	{ID: "crime", Terms: []string{"crime", "kidnapping", "carjacking", "cartel"}},
	{ID: "weather_hazard", Terms: []string{"hurricane", "typhoon", "tropical storm", "flood", "wildfire", "earthquake"}},
}

func matchedTerms(text string, rules []termRule) []string {
	t := strings.ToLower(text)
	out := []string{}
	for _, r := range rules {
		for _, term := range r.Terms {
			if strings.Contains(t, term) {
				out = append(out, r.ID)
				break
			}
		}
	}
	return out
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func containsAny(text string, terms ...string) bool {
	for _, term := range terms {
		if strings.Contains(text, term) {
			return true
		}
	}
	return false
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func labeledSection(text, label string) string {
	t := normalizeSpace(text)
	low := strings.ToLower(t)
	start := strings.Index(low, label+":")
	prefixLen := len(label) + 1
	if start < 0 && label == "location" {
		start = strings.Index(low, "locations:")
		prefixLen = len("locations:")
	}
	if start < 0 {
		return ""
	}
	rest := t[start+prefixLen:]
	lowRest := strings.ToLower(rest)
	stop := len(rest)
	for _, next := range []string{" event:", " actions to take:", " action to take:", " assistance:", " message:", " background:"} {
		if idx := strings.Index(lowRest, next); idx >= 0 && idx < stop {
			stop = idx
		}
	}
	return strings.Trim(strings.TrimSpace(rest[:stop]), " .;")
}

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func extraEmbassyFeeds() []feedSpec {
	raw := strings.TrimSpace(os.Getenv("OFFICIAL_ADVISORY_EXTRA_FEEDS"))
	if raw == "" {
		return nil
	}
	var out []feedSpec
	for _, item := range strings.Split(raw, ",") {
		parts := strings.Split(item, "|")
		if len(parts) < 3 {
			continue
		}
		spec := feedSpec{
			Name: strings.TrimSpace(parts[0]), TargetCountry: strings.ToUpper(strings.TrimSpace(parts[1])),
			URL: strings.TrimSpace(parts[2]), SourceCountry: "US",
		}
		if len(parts) >= 4 {
			spec.DefaultCity = strings.TrimSpace(parts[3])
		}
		if spec.Name != "" && spec.TargetCountry != "" && spec.URL != "" {
			out = append(out, spec)
		}
	}
	return out
}

type cityPoint struct {
	Name     string
	Lat, Lon float64
}

func city(name string, lat, lon float64) cityPoint {
	return cityPoint{Name: name, Lat: lat, Lon: lon}
}

func matchCity(countryCode, text string) (name string, lat, lon float64) {
	cities := cityByCountry[strings.ToUpper(strings.TrimSpace(countryCode))]
	if len(cities) == 0 {
		return "", 0, 0
	}
	bestIdx := len(text) + 1
	for key, c := range cities {
		if idx := boundedIndex(text, key); idx >= 0 && idx < bestIdx {
			bestIdx = idx
			name, lat, lon = c.Name, c.Lat, c.Lon
		}
	}
	return name, lat, lon
}

func boundedIndex(text, term string) int {
	t := strings.ToLower(text)
	needle := strings.ToLower(strings.TrimSpace(term))
	if needle == "" {
		return -1
	}
	offset := 0
	for {
		idx := strings.Index(t[offset:], needle)
		if idx < 0 {
			return -1
		}
		start := offset + idx
		end := start + len(needle)
		if isBoundary(t, start-1) && isBoundary(t, end) {
			return start
		}
		offset = end
		if offset >= len(t) {
			return -1
		}
	}
}

func isBoundary(s string, idx int) bool {
	if idx < 0 || idx >= len(s) {
		return true
	}
	ch := s[idx]
	return !(ch >= 'a' && ch <= 'z') && !(ch >= '0' && ch <= '9')
}

var cityByCountry = map[string]map[string]cityPoint{
	"AE": {"abu dhabi": city("Abu Dhabi", 24.4539, 54.3773), "dubai": city("Dubai", 25.2048, 55.2708)},
	"BD": {"dhaka": city("Dhaka", 23.8103, 90.4125), "chittagong": city("Chittagong", 22.3569, 91.7832)},
	"CO": {"bogota": city("Bogota", 4.7110, -74.0721), "bogotá": city("Bogota", 4.7110, -74.0721), "medellin": city("Medellin", 6.2442, -75.5812), "medellín": city("Medellin", 6.2442, -75.5812), "cali": city("Cali", 3.4516, -76.5320), "cartagena": city("Cartagena", 10.3910, -75.4794)},
	"DE": {"berlin": city("Berlin", 52.5200, 13.4050), "frankfurt": city("Frankfurt", 50.1109, 8.6821), "munich": city("Munich", 48.1351, 11.5820), "hamburg": city("Hamburg", 53.5511, 9.9937)},
	"DO": {"santo domingo": city("Santo Domingo", 18.4861, -69.9312), "punta cana": city("Punta Cana", 18.5601, -68.3725)},
	"EC": {"quito": city("Quito", -0.1807, -78.4678), "guayaquil": city("Guayaquil", -2.1700, -79.9224)},
	"EG": {"cairo": city("Cairo", 30.0444, 31.2357), "alexandria": city("Alexandria", 31.2001, 29.9187)},
	"ES": {"madrid": city("Madrid", 40.4168, -3.7038), "barcelona": city("Barcelona", 41.3874, 2.1686)},
	"FR": {"paris": city("Paris", 48.8566, 2.3522), "marseille": city("Marseille", 43.2965, 5.3698), "lyon": city("Lyon", 45.7640, 4.8357)},
	"GB": {"london": city("London", 51.5074, -0.1278), "belfast": city("Belfast", 54.5973, -5.9301), "edinburgh": city("Edinburgh", 55.9533, -3.1883)},
	"GR": {"athens": city("Athens", 37.9838, 23.7275), "thessaloniki": city("Thessaloniki", 40.6401, 22.9444)},
	"GT": {"guatemala city": city("Guatemala City", 14.6349, -90.5069)},
	"HT": {"port-au-prince": city("Port-au-Prince", 18.5944, -72.3074), "port au prince": city("Port-au-Prince", 18.5944, -72.3074)},
	"IL": {"jerusalem": city("Jerusalem", 31.7683, 35.2137), "tel aviv": city("Tel Aviv", 32.0853, 34.7818), "haifa": city("Haifa", 32.7940, 34.9896)},
	"IN": {"new delhi": city("New Delhi", 28.6139, 77.2090), "delhi": city("New Delhi", 28.6139, 77.2090), "mumbai": city("Mumbai", 19.0760, 72.8777), "chennai": city("Chennai", 13.0827, 80.2707), "hyderabad": city("Hyderabad", 17.3850, 78.4867), "kolkata": city("Kolkata", 22.5726, 88.3639)},
	"IQ": {"baghdad": city("Baghdad", 33.3152, 44.3661), "erbil": city("Erbil", 36.1901, 43.9930), "basra": city("Basra", 30.5085, 47.7804)},
	"IT": {"rome": city("Rome", 41.9028, 12.4964), "milan": city("Milan", 45.4642, 9.1900), "naples": city("Naples", 40.8518, 14.2681), "florence": city("Florence", 43.7696, 11.2558)},
	"JO": {"amman": city("Amman", 31.9539, 35.9106)},
	"JP": {"tokyo": city("Tokyo", 35.6762, 139.6503), "osaka": city("Osaka", 34.6937, 135.5023), "okinawa": city("Okinawa", 26.2124, 127.6792)},
	"KE": {"nairobi": city("Nairobi", -1.2921, 36.8219), "mombasa": city("Mombasa", -4.0435, 39.6682)},
	"KR": {"seoul": city("Seoul", 37.5665, 126.9780), "busan": city("Busan", 35.1796, 129.0756)},
	"LB": {"beirut": city("Beirut", 33.8938, 35.5018), "tripoli": city("Tripoli", 34.4367, 35.8497)},
	"MM": {"yangon": city("Yangon", 16.8409, 96.1735), "mandalay": city("Mandalay", 21.9588, 96.0891), "naypyidaw": city("Naypyidaw", 19.7633, 96.0785)},
	"MX": {"mexico city": city("Mexico City", 19.4326, -99.1332), "tijuana": city("Tijuana", 32.5149, -117.0382), "ciudad juarez": city("Ciudad Juarez", 31.6904, -106.4245), "ciudad juárez": city("Ciudad Juarez", 31.6904, -106.4245), "matamoros": city("Matamoros", 25.8690, -97.5027), "reynosa": city("Reynosa", 26.0922, -98.2770), "nuevo laredo": city("Nuevo Laredo", 27.4779, -99.5496), "monterrey": city("Monterrey", 25.6866, -100.3161), "cancun": city("Cancun", 21.1619, -86.8515), "cancún": city("Cancun", 21.1619, -86.8515), "guadalajara": city("Guadalajara", 20.6597, -103.3496)},
	"NG": {"abuja": city("Abuja", 9.0765, 7.3986), "lagos": city("Lagos", 6.5244, 3.3792)},
	"PE": {"lima": city("Lima", -12.0464, -77.0428), "cusco": city("Cusco", -13.5319, -71.9675)},
	"PH": {"manila": city("Manila", 14.5995, 120.9842), "cebu": city("Cebu", 10.3157, 123.8854), "davao": city("Davao", 7.1907, 125.4553)},
	"PK": {"islamabad": city("Islamabad", 33.6844, 73.0479), "karachi": city("Karachi", 24.8607, 67.0011), "lahore": city("Lahore", 31.5204, 74.3587), "peshawar": city("Peshawar", 34.0151, 71.5249)},
	"PL": {"warsaw": city("Warsaw", 52.2297, 21.0122), "krakow": city("Krakow", 50.0647, 19.9450), "kraków": city("Krakow", 50.0647, 19.9450)},
	"SV": {"san salvador": city("San Salvador", 13.6929, -89.2182)},
	"TH": {"bangkok": city("Bangkok", 13.7563, 100.5018), "chiang mai": city("Chiang Mai", 18.7883, 98.9853), "phuket": city("Phuket", 7.8804, 98.3923)},
	"TR": {"ankara": city("Ankara", 39.9334, 32.8597), "istanbul": city("Istanbul", 41.0082, 28.9784), "izmir": city("Izmir", 38.4237, 27.1428)},
	"UA": {"kyiv": city("Kyiv", 50.4501, 30.5234), "kiev": city("Kyiv", 50.4501, 30.5234), "odesa": city("Odesa", 46.4825, 30.7233), "odessa": city("Odesa", 46.4825, 30.7233), "lviv": city("Lviv", 49.8397, 24.0297), "kharkiv": city("Kharkiv", 49.9935, 36.2304), "dnipro": city("Dnipro", 48.4647, 35.0462)},
	"VE": {"caracas": city("Caracas", 10.4806, -66.9036)},
	"ZA": {"pretoria": city("Pretoria", -25.7479, 28.2293), "johannesburg": city("Johannesburg", -26.2041, 28.0473), "cape town": city("Cape Town", -33.9249, 18.4241), "durban": city("Durban", -29.8587, 31.0218)},
}

func strip(s string) string {
	s = html.UnescapeString(s)
	return strings.TrimSpace(htmlTagRE.ReplaceAllString(s, " "))
}

func stableID(s string) string {
	h := sha1.Sum([]byte(strings.TrimSpace(strings.ToLower(s))))
	return hex.EncodeToString(h[:])
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

var countryCentroids = map[string][2]float64{
	"Afghanistan": {67, 33}, "Albania": {20, 41}, "Algeria": {3, 28}, "Argentina": {-64, -34},
	"Australia": {133, -25}, "Bangladesh": {90, 24}, "Belarus": {28, 53}, "Brazil": {-52, -14},
	"Burkina Faso": {-2, 12}, "Cameroon": {12, 6}, "Canada": {-106, 56}, "Central African Republic": {21, 7},
	"Chad": {19, 15}, "China": {104, 35}, "Colombia": {-72, 4}, "Congo": {-15, 1},
	"Cuba": {-80, 22}, "DRC": {24, -3}, "Dominican Republic": {-70, 19}, "Ecuador": {-78, -2},
	"Egypt": {30, 27}, "El Salvador": {-89, 14}, "Eritrea": {39, 16}, "Ethiopia": {40, 9},
	"France": {2, 46}, "Germany": {10, 51}, "Ghana": {-2, 8}, "Greece": {22, 39}, "Guatemala": {-90, 16},
	"Haiti": {-72, 19}, "Honduras": {-87, 15}, "India": {79, 21}, "Indonesia": {120, -5},
	"Iran": {53, 32}, "Iraq": {44, 33}, "Israel": {35, 31}, "Italy": {13, 42},
	"Japan": {138, 36}, "Jordan": {36, 31}, "Kazakhstan": {67, 48}, "Kenya": {38, 0},
	"Kuwait": {48, 29}, "Lebanon": {36, 34}, "Libya": {17, 27}, "Mali": {-4, 17},
	"Mauritania": {-10, 20}, "Mexico": {-102, 23}, "Morocco": {-6, 32}, "Mozambique": {35, -18},
	"Myanmar": {96, 22}, "Nepal": {84, 28}, "Nicaragua": {-85, 13}, "Niger": {8, 16},
	"Nigeria": {8, 10}, "Pakistan": {69, 30}, "Palestine": {35, 32}, "Peru": {-76, -10},
	"Philippines": {122, 13}, "Poland": {19, 52}, "Russia": {100, 60}, "Rwanda": {30, -2},
	"Saudi Arabia": {45, 24}, "Somalia": {46, 6}, "South Africa": {25, -30}, "South Korea": {128, 36}, "South Sudan": {32, 7},
	"Spain": {-4, 40}, "Sri Lanka": {81, 8}, "Sudan": {30, 16}, "Syria": {38, 35},
	"Taiwan": {121, 24}, "Thailand": {101, 15}, "Tunisia": {9, 34}, "Turkey": {35, 39},
	"UAE": {54, 24}, "Ukraine": {32, 49}, "United Arab Emirates": {54, 24},
	"United Kingdom": {-3, 55}, "United States": {-98, 39}, "Venezuela": {-67, 7}, "Yemen": {48, 16},
}

var codeToCountry = map[string]string{
	"TH": "Thailand", "AE": "United Arab Emirates", "DE": "Germany", "UA": "Ukraine", "MX": "Mexico",
	"IN": "India", "PK": "Pakistan", "CO": "Colombia", "PL": "Poland", "BD": "Bangladesh",
	"IT": "Italy", "DO": "Dominican Republic", "MM": "Myanmar", "IL": "Israel", "LB": "Lebanon",
	"IQ": "Iraq", "JO": "Jordan", "EG": "Egypt", "KE": "Kenya", "NG": "Nigeria", "ZA": "South Africa",
	"PH": "Philippines", "KR": "South Korea", "JP": "Japan", "FR": "France", "GB": "United Kingdom",
	"TR": "Turkey", "ES": "Spain", "GR": "Greece", "PE": "Peru", "EC": "Ecuador", "HT": "Haiti",
	"GT": "Guatemala", "SV": "El Salvador", "VE": "Venezuela",
}

func countryForCode(code string) (lat, lon float64, country string, ok bool) {
	name := codeToCountry[strings.ToUpper(strings.TrimSpace(code))]
	if name == "" {
		return 0, 0, "", false
	}
	c, ok := countryCentroids[name]
	if !ok {
		return 0, 0, "", false
	}
	return c[1], c[0], name, true
}

func findCountry(text string) (lat, lon float64, country string, ok bool) {
	t := strings.ToLower(text)
	for name, c := range countryCentroids {
		if strings.Contains(t, strings.ToLower(name)) {
			return c[1], c[0], name, true
		}
	}
	return 0, 0, "", false
}

func _compileGuard() {
	_ = fmt.Sprintf
}
