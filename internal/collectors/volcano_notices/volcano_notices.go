// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// USGS HANS volcano notices with per-volcano alert/color sections.
package volcano_notices

import (
	"context"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const recentURL = "https://volcanoes.usgs.gov/hans-public/api/notice/getRecentNotices"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "volcano_notices" }
func (c *Collector) PollEvery() time.Duration { return 60 * time.Minute }

type noticeSummary struct {
	SentUTC          string `json:"sent_utc"`
	SentUnixTime     int64  `json:"sent_unixtime"`
	NoticeIdentifier string `json:"notice_identifier"`
	Volcanoes        string `json:"volcanoes"`
	NoticeTypeTitle  string `json:"notice_type_title"`
	NoticeTypeCD     string `json:"notice_type_cd"`
	NoticeCategory   string `json:"notice_category"`
	ObsFullName      string `json:"obs_fullname"`
	ObsAbbr          string `json:"obs_abbr"`
	NoticeURL        string `json:"notice_url"`
	NoticeData       string `json:"notice_data"`
}

type noticeDetail struct {
	NoticeIdentifier string          `json:"notice_identifier"`
	NoticeTitle      string          `json:"notice_title"`
	NoticeType       string          `json:"notice_type"`
	NoticeTypeTitle  string          `json:"notice_type_title"`
	NoticeTypeCD     string          `json:"notice_type_cd"`
	SentUTC          string          `json:"sent_utc"`
	SentUnixTime     int64           `json:"sent_unixtime"`
	ObsAbbr          string          `json:"obs_abbr"`
	ObsFullName      string          `json:"obs_fullname"`
	NoticeURL        string          `json:"notice_url"`
	NoticeData       string          `json:"notice_data"`
	HighestAlert     string          `json:"highest_alert_level"`
	HighestColor     string          `json:"highest_color_code"`
	Sections         []noticeSection `json:"notice_sections"`
}

type noticeSection struct {
	SectionHTML string `json:"sectionHtml"`
	Summary     string `json:"summary"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var notices []noticeSummary
	if err := httpx.GetJSON(ctx, recentURL, nil, &notices); err != nil {
		return nil, err
	}
	out := []events.Event{}
	now := time.Now().UTC()
	for i, n := range notices {
		if i >= 20 {
			break
		}
		ts := unixTime(n.SentUnixTime)
		if !ts.IsZero() && ts.Before(now.Add(-14*24*time.Hour)) {
			continue
		}
		if n.NoticeData == "" {
			continue
		}
		evs, err := fetchNotice(ctx, n)
		if err != nil {
			return nil, err
		}
		out = append(out, evs...)
	}
	return out, nil
}

func fetchNotice(ctx context.Context, summary noticeSummary) ([]events.Event, error) {
	var detail noticeDetail
	if err := httpx.GetJSON(ctx, summary.NoticeData, nil, &detail); err != nil {
		return nil, err
	}
	if detail.NoticeIdentifier == "" {
		detail.NoticeIdentifier = summary.NoticeIdentifier
	}
	if detail.NoticeURL == "" {
		detail.NoticeURL = summary.NoticeURL
	}
	if detail.NoticeData == "" {
		detail.NoticeData = summary.NoticeData
	}
	if detail.ObsFullName == "" {
		detail.ObsFullName = summary.ObsFullName
	}
	if detail.NoticeTypeTitle == "" {
		detail.NoticeTypeTitle = summary.NoticeTypeTitle
	}
	return eventsFromDetail(detail), nil
}

func eventsFromDetail(detail noticeDetail) []events.Event {
	ts := unixTime(detail.SentUnixTime)
	if ts.IsZero() {
		ts = parseNoticeDate(detail.SentUTC)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	out := []events.Event{}
	for _, section := range detail.Sections {
		v, ok := parseSection(section.SectionHTML)
		if !ok {
			continue
		}
		summary := stripHTML(firstNonEmpty(section.Summary, v.Summary))
		props := map[string]any{
			"notice_identifier": detail.NoticeIdentifier,
			"notice_title":      detail.NoticeTitle,
			"notice_type":       firstNonEmpty(detail.NoticeType, detail.NoticeTypeTitle),
			"notice_type_cd":    detail.NoticeTypeCD,
			"notice_url":        detail.NoticeURL,
			"notice_data":       detail.NoticeData,
			"sent_utc":          detail.SentUTC,
			"observatory":       detail.ObsFullName,
			"observatory_abbr":  detail.ObsAbbr,
			"highest_alert":     detail.HighestAlert,
			"highest_color":     detail.HighestColor,
			"volcano_name":      v.Name,
			"vnum":              v.VNum,
			"alert_level":       v.AlertLevel,
			"color_code":        v.ColorCode,
			"summary":           summary,
		}
		collectorutil.AddVolcanoNoticeScores(props)
		ext := detail.NoticeIdentifier + ":" + firstNonEmpty(v.VNum, v.Name)
		out = append(out, events.Event{
			Ts:     ts,
			Source: "volcano_notices",
			ExtID:  ext,
			Lat:    v.Lat,
			Lon:    v.Lon,
			Props:  props,
		})
	}
	return out
}

type volcanoSection struct {
	Name       string
	VNum       string
	Lat        float64
	Lon        float64
	AlertLevel string
	ColorCode  string
	Summary    string
}

var (
	volcanoHeaderRe = regexp.MustCompile(`(?is)<b>\s*([A-Z][A-Z0-9 .'\-]+?)\s*</b>\s*\(VNUM\s*#([0-9]+)\)\s*<br\s*/?>\s*([^<]+)`)
	alertRe         = regexp.MustCompile(`(?is)Current Volcano Alert Level:\s*([^<]+)`)
	colorRe         = regexp.MustCompile(`(?is)Current Aviation Color Code:\s*([^<]+)`)
	dmsRe           = regexp.MustCompile(`(?i)(\d{1,3})[°º]\s*(\d{1,2})'\s*(\d{1,2}(?:\.\d+)?)"?\s*([NS])\s+(\d{1,3})[°º]\s*(\d{1,2})'\s*(\d{1,2}(?:\.\d+)?)"?\s*([EW])`)
	htmlTagRe       = regexp.MustCompile(`<[^>]+>`)
	wsRe            = regexp.MustCompile(`\s+`)
)

func parseSection(raw string) (volcanoSection, bool) {
	raw = html.UnescapeString(raw)
	m := volcanoHeaderRe.FindStringSubmatch(raw)
	if len(m) == 0 {
		return volcanoSection{}, false
	}
	lat, lon, ok := parseDMSPair(m[3])
	if !ok {
		return volcanoSection{}, false
	}
	v := volcanoSection{
		Name: strings.Title(strings.ToLower(strings.TrimSpace(m[1]))),
		VNum: strings.TrimSpace(m[2]),
		Lat:  lat,
		Lon:  lon,
	}
	if a := alertRe.FindStringSubmatch(raw); len(a) > 1 {
		v.AlertLevel = strings.TrimSpace(htmlTagRe.ReplaceAllString(a[1], ""))
	}
	if c := colorRe.FindStringSubmatch(raw); len(c) > 1 {
		v.ColorCode = strings.TrimSpace(htmlTagRe.ReplaceAllString(c[1], ""))
	}
	return v, true
}

func parseDMSPair(s string) (float64, float64, bool) {
	m := dmsRe.FindStringSubmatch(s)
	if len(m) == 0 {
		return 0, 0, false
	}
	lat, ok1 := dmsToDecimal(m[1], m[2], m[3], m[4])
	lon, ok2 := dmsToDecimal(m[5], m[6], m[7], m[8])
	return lat, lon, ok1 && ok2
}

func dmsToDecimal(deg, min, sec, hemi string) (float64, bool) {
	d, err1 := strconv.ParseFloat(deg, 64)
	m, err2 := strconv.ParseFloat(min, 64)
	s, err3 := strconv.ParseFloat(sec, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, false
	}
	v := d + m/60 + s/3600
	if strings.EqualFold(hemi, "S") || strings.EqualFold(hemi, "W") {
		v = -v
	}
	return v, true
}

func stripHTML(s string) string {
	s = htmlTagRe.ReplaceAllString(html.UnescapeString(s), " ")
	s = wsRe.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func unixTime(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

func parseNoticeDate(s string) time.Time {
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, strings.TrimSpace(s), time.UTC); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
