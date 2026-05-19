// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Tokyo VAAC latest volcanic ash advisories.
package vaac_tokyo

import (
	"context"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	indexURL = "https://ds.data.jma.go.jp/svd/vaac/data/index.html"
	baseURL  = "https://ds.data.jma.go.jp/svd/vaac/data/"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "vaac_tokyo" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, indexURL, nil)
	if err != nil {
		return nil, err
	}
	links := latestLinks(string(buf))
	out := make([]events.Event, 0, len(links))
	for _, link := range links {
		ev, ok, err := fetchAdvisory(ctx, link)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

var linkRe = regexp.MustCompile(`href="(TextData/[0-9]{4}/[^"]+_Text\.html)"`)

func latestLinks(raw string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, m := range linkRe.FindAllStringSubmatch(raw, -1) {
		if _, ok := seen[m[1]]; ok {
			continue
		}
		seen[m[1]] = struct{}{}
		out = append(out, m[1])
		if len(out) >= 12 {
			break
		}
	}
	return out
}

func fetchAdvisory(ctx context.Context, rel string) (events.Event, bool, error) {
	fullURL := baseURL + rel
	buf, err := httpx.GetBytes(ctx, fullURL, nil)
	if err != nil {
		return events.Event{}, false, err
	}
	return eventFromTextPage(string(buf), fullURL)
}

var (
	brRe      = regexp.MustCompile(`(?i)<br\s*/?>`)
	tagRe     = regexp.MustCompile(`<[^>]+>`)
	vaaBlock  = regexp.MustCompile(`(?is)<!-- VAA Text Start -->(.*?)<!-- VAA Text End -->`)
	fieldLine = regexp.MustCompile(`^([A-Z0-9 +/]+):\s*(.*)$`)
)

func eventFromTextPage(raw, fullURL string) (events.Event, bool, error) {
	block := raw
	if m := vaaBlock.FindStringSubmatch(raw); len(m) > 1 {
		block = m[1]
	}
	text := brRe.ReplaceAllString(block, "\n")
	text = tagRe.ReplaceAllString(text, "")
	text = html.UnescapeString(text)
	fields := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line == "VA ADVISORY" || strings.HasPrefix(line, "FV") {
			continue
		}
		if m := fieldLine.FindStringSubmatch(line); len(m) > 2 {
			fields[strings.TrimSpace(m[1])] = strings.TrimSpace(m[2])
		}
	}
	lat, lon, ok := parsePSN(fields["PSN"])
	if !ok {
		return events.Event{}, false, nil
	}
	ts := parseDTG(fields["DTG"])
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	volcanoName, volcanoID := splitVolcano(fields["VOLCANO"])
	ext := fmt.Sprintf("%s:%s:%s", fields["ADVISORY NR"], volcanoName, fields["DTG"])
	props := map[string]any{
		"volcano":             volcanoName,
		"volcano_id":          volcanoID,
		"vaac":                firstNonEmpty(fields["VAAC"], "TOKYO"),
		"area":                fields["AREA"],
		"dtg":                 fields["DTG"],
		"advisory_nr":         fields["ADVISORY NR"],
		"eruption_details":    fields["ERUPTION DETAILS"],
		"obs_va_dtg":          fields["OBS VA DTG"],
		"obs_va_cld":          fields["OBS VA CLD"],
		"fcst_va_cld_6_hr":    fields["FCST VA CLD +6 HR"],
		"fcst_va_cld_12_hr":   fields["FCST VA CLD +12 HR"],
		"fcst_va_cld_18_hr":   fields["FCST VA CLD +18 HR"],
		"info_source":         fields["INFO SOURCE"],
		"source_elev":         fields["SOURCE ELEV"],
		"remarks":             fields["RMK"],
		"next_advisory":       strings.TrimSuffix(fields["NXT ADVISORY"], "="),
		"raw_text":            strings.TrimSpace(text),
		"advisory_url":        fullURL,
		"source_api_endpoint": indexURL,
	}
	return events.Event{
		Ts:     ts,
		Source: "vaac_tokyo",
		ExtID:  ext,
		Lat:    lat,
		Lon:    lon,
		Props:  props,
	}, true, nil
}

func splitVolcano(s string) (name, id string) {
	parts := strings.Fields(s)
	if len(parts) == 0 {
		return "", ""
	}
	last := parts[len(parts)-1]
	if _, err := strconv.Atoi(last); err == nil && len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], " "), last
	}
	return s, ""
}

var psnRe = regexp.MustCompile(`(?i)^([NS])(\d{2})(\d{2})\s+([EW])(\d{3})(\d{2})$`)

func parsePSN(s string) (float64, float64, bool) {
	m := psnRe.FindStringSubmatch(strings.TrimSpace(s))
	if len(m) == 0 {
		return 0, 0, false
	}
	lat, ok1 := dmToDecimal(m[2], m[3], m[1])
	lon, ok2 := dmToDecimal(m[5], m[6], m[4])
	return lat, lon, ok1 && ok2
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
	// Example: 20260428/2100Z
	t, err := time.ParseInLocation("20060102/1504Z", strings.TrimSpace(s), time.UTC)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
