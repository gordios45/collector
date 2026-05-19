// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package vaac_global ingests the Bureau of Meteorology's public recent
// volcanic ash advisory mirror. The page aggregates no-key text VAA bulletins
// from several VAACs and fills coverage gaps outside the dedicated Tokyo and
// Washington collectors.
package vaac_global

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"html"
	"math"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
)

const (
	sourceID = "vaac_global"
	indexURL = "https://www.bom.gov.au/products/Volc_ash_recent.shtml"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := getBytes(ctx, indexURL)
	if err != nil {
		return nil, err
	}
	blocks := advisoryBlocks(string(buf))
	out := make([]events.Event, 0, len(blocks))
	for _, block := range blocks {
		if ev, ok := eventFromBlock(block); ok {
			out = append(out, ev)
		}
	}
	return dedupe(out), nil
}

func getBytes(ctx context.Context, rawURL string) ([]byte, error) {
	return exec.CommandContext(ctx, "curl", "-fsS", "-L", "--max-time", "30", "-A", "gordios/0.1", rawURL).Output()
}

var (
	brRe        = regexp.MustCompile(`(?i)<br\s*/?>`)
	tagRe       = regexp.MustCompile(`<[^>]+>`)
	fieldLineRe = regexp.MustCompile(`^([A-Z0-9 +/\[\]]+):\s*(.*)$`)
	psnDMRe     = regexp.MustCompile(`(?i)^([NS])(\d{2})(\d{2})\s+([EW])(\d{3})(\d{2})$`)
	psnDecRe    = regexp.MustCompile(`(?i)^([NS])?(\d+(?:\.\d+)?)\s*[, ]+\s*([EW])?(\d+(?:\.\d+)?)$`)
)

func advisoryBlocks(raw string) []string {
	text := brRe.ReplaceAllString(raw, "\n")
	text = tagRe.ReplaceAllString(text, "")
	text = html.UnescapeString(text)
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	out := []string{}
	current := []string{}
	flush := func() {
		if len(current) == 0 {
			return
		}
		block := strings.TrimSpace(strings.Join(current, "\n"))
		if strings.Contains(block, "VA ADVISORY") && strings.Contains(block, "PSN:") {
			out = append(out, block)
		}
		current = nil
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, " \t")
		if strings.HasPrefix(strings.TrimSpace(line), "Received ") {
			flush()
			current = append(current, strings.TrimSpace(line))
			continue
		}
		if len(current) > 0 {
			current = append(current, line)
		}
	}
	flush()
	return out
}

func eventFromBlock(block string) (events.Event, bool) {
	fields := parseFields(block)
	lat, lon, ok := parsePSN(fields["PSN"])
	if !ok {
		return events.Event{}, false
	}
	ts := parseDTG(fields["DTG"])
	if ts.IsZero() {
		ts = parseReceivedTime(block)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	volcanoName, volcanoID := splitVolcano(fields["VOLCANO"])
	vaac := firstNonEmpty(fields["VAAC"], vaacFromReceived(block), "UNKNOWN")
	advisoryNr := fields["ADVISORY NR"]
	extID := stableID(strings.Join([]string{vaac, volcanoName, volcanoID, advisoryNr, fields["DTG"]}, ":"))
	props := map[string]any{
		"source_provider":      "Bureau of Meteorology recent VAAC advisory mirror",
		"source_api_endpoint":  indexURL,
		"source_public_url":    indexURL,
		"source_provider_kind": "official_volcanic_ash_advisory_mirror",
		"volcano":              volcanoName,
		"volcano_id":           volcanoID,
		"vaac":                 vaac,
		"area":                 fields["AREA"],
		"dtg":                  fields["DTG"],
		"advisory_nr":          advisoryNr,
		"info_source":          fields["INFO SOURCE"],
		"source_elev":          fields["SOURCE ELEV"],
		"summit_elev":          fields["SUMMIT ELEV"],
		"aviation_colour_code": fields["AVIATION COLOUR CODE"],
		"eruption_details":     fields["ERUPTION DETAILS"],
		"obs_va_dtg":           fields["OBS VA DTG"],
		"obs_va_cld":           fields["OBS VA CLD"],
		"fcst_va_cld_6_hr":     fields["FCST VA CLD +6 HR"],
		"fcst_va_cld_12_hr":    fields["FCST VA CLD +12 HR"],
		"fcst_va_cld_18_hr":    fields["FCST VA CLD +18 HR"],
		"remarks":              fields["RMK"],
		"next_advisory":        strings.TrimSuffix(fields["NXT ADVISORY"], "="),
		"received":             firstLine(block),
		"raw_text":             block,
		"advisory_url":         indexURL,
		"source_payload_validity": map[string]any{
			"valid_start":    ts.Format(time.RFC3339),
			"valid_end":      ts.Add(18 * time.Hour).Format(time.RFC3339),
			"validity_basis": "vaac_text_advisory_forecast_window",
		},
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: "bom_recent:" + extID, Lat: lat, Lon: lon, Props: props}, true
}

func parseFields(block string) map[string]string {
	fields := map[string]string{}
	current := ""
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line == "VA ADVISORY" || strings.HasPrefix(line, "Received ") || strings.HasPrefix(line, "FV") {
			continue
		}
		if m := fieldLineRe.FindStringSubmatch(line); len(m) > 2 {
			current = strings.TrimSpace(m[1])
			fields[current] = strings.TrimSpace(m[2])
			continue
		}
		if current != "" {
			fields[current] = strings.TrimSpace(fields[current] + " " + line)
		}
	}
	return fields
}

func splitVolcano(s string) (name, id string) {
	parts := strings.Fields(strings.TrimSpace(s))
	if len(parts) == 0 {
		return "", ""
	}
	last := parts[len(parts)-1]
	if _, err := strconv.Atoi(last); err == nil && len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], " "), last
	}
	return strings.TrimSpace(s), ""
}

func parsePSN(s string) (float64, float64, bool) {
	s = strings.TrimSpace(strings.TrimSuffix(s, "="))
	if m := psnDMRe.FindStringSubmatch(s); len(m) == 7 {
		lat, ok1 := dmToDecimal(m[2], m[3], m[1])
		lon, ok2 := dmToDecimal(m[5], m[6], m[4])
		return lat, lon, ok1 && ok2 && validLatLon(lat, lon)
	}
	if m := psnDecRe.FindStringSubmatch(s); len(m) == 5 {
		lat, err1 := strconv.ParseFloat(m[2], 64)
		lon, err2 := strconv.ParseFloat(m[4], 64)
		if err1 != nil || err2 != nil {
			return 0, 0, false
		}
		if strings.EqualFold(m[1], "S") {
			lat = -lat
		}
		if strings.EqualFold(m[3], "W") {
			lon = -lon
		}
		return lat, lon, validLatLon(lat, lon)
	}
	return 0, 0, false
}

func dmToDecimal(deg, min, hemi string) (float64, bool) {
	d, err1 := strconv.ParseFloat(deg, 64)
	m, err2 := strconv.ParseFloat(min, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	v := d + m/60
	if strings.EqualFold(hemi, "S") || strings.EqualFold(hemi, "W") {
		v = -v
	}
	return v, true
}

func parseDTG(s string) time.Time {
	t, err := time.ParseInLocation("20060102/1504Z", strings.TrimSpace(s), time.UTC)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

var receivedRe = regexp.MustCompile(`Received\s+\S+\s+at\s+(\d{2}):(\d{2})\s+UTC,\s+(\d{2})/(\d{2})/(\d{2})`)

func parseReceivedTime(block string) time.Time {
	m := receivedRe.FindStringSubmatch(block)
	if len(m) != 6 {
		return time.Time{}
	}
	hour, _ := strconv.Atoi(m[1])
	minute, _ := strconv.Atoi(m[2])
	day, _ := strconv.Atoi(m[3])
	month, _ := strconv.Atoi(m[4])
	year, _ := strconv.Atoi(m[5])
	return time.Date(2000+year, time.Month(month), day, hour, minute, 0, 0, time.UTC)
}

func vaacFromReceived(block string) string {
	line := firstLine(block)
	if strings.Contains(line, "from ") {
		return strings.TrimSpace(line[strings.LastIndex(line, "from ")+5:])
	}
	return ""
}

func firstLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
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

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func validLatLon(lat, lon float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}
