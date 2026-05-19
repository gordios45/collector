// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NOAA OSPO current volcanic ash advisories.
package vaac_washington

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const (
	indexURL = "https://www.ospo.noaa.gov/products/atmosphere/vaac/messages.html"
	baseURL  = "https://www.ospo.noaa.gov"
)

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "vaac_washington" }
func (c *Collector) PollEvery() time.Duration { return 15 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	buf, err := httpx.GetBytes(ctx, indexURL, nil)
	if err != nil {
		return nil, err
	}
	links := latestXMLLinks(string(buf), 40)
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

var xmlLinkRe = regexp.MustCompile(`href="([^"]+/xml_files/[^"]+\.xml)"`)

func latestXMLLinks(raw string, limit int) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, m := range xmlLinkRe.FindAllStringSubmatch(raw, -1) {
		link := normalizeURL(html.UnescapeString(m[1]))
		if _, ok := seen[link]; ok {
			continue
		}
		seen[link] = struct{}{}
		out = append(out, link)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func fetchAdvisory(ctx context.Context, fullURL string) (events.Event, bool, error) {
	buf, err := httpx.GetBytes(ctx, fullURL, nil)
	if err != nil {
		return events.Event{}, false, err
	}
	return eventFromXML(buf, fullURL)
}

type bulletin struct {
	Information []struct {
		Advisory vaa `xml:"VolcanicAshAdvisory"`
	} `xml:"meteorologicalInformation"`
	BulletinIdentifier string `xml:"bulletinIdentifier"`
}

type vaa struct {
	IssueTime struct {
		TimeInstant struct {
			TimePosition string `xml:"timePosition"`
		} `xml:"TimeInstant"`
	} `xml:"issueTime"`
	IssuingCentre struct {
		Unit struct {
			TimeSlice struct {
				UnitTimeSlice struct {
					Name string `xml:"name"`
				} `xml:"UnitTimeSlice"`
			} `xml:"timeSlice"`
		} `xml:"Unit"`
	} `xml:"issuingVolcanicAshAdvisoryCentre"`
	Volcano struct {
		EruptingVolcano struct {
			Name     string `xml:"name"`
			Position struct {
				Point struct {
					Pos string `xml:"pos"`
				} `xml:"Point"`
			} `xml:"position"`
			EruptionDate string `xml:"eruptionDate"`
		} `xml:"EruptingVolcano"`
	} `xml:"volcano"`
	StateOrRegion     string `xml:"stateOrRegion"`
	SummitElevation   string `xml:"summitElevation"`
	AdvisoryNumber    string `xml:"advisoryNumber"`
	InformationSource string `xml:"informationSource"`
	EruptionDetails   string `xml:"eruptionDetails"`
	Observation       struct {
		Conditions []struct {
			Status         string `xml:"status,attr"`
			IsEstimated    string `xml:"isEstimated,attr"`
			PhenomenonTime struct {
				TimeInstant struct {
					TimePosition string `xml:"timePosition"`
				} `xml:"TimeInstant"`
			} `xml:"phenomenonTime"`
			AshCloud struct {
				Observed struct {
					AshCloudExtent ashCloudExtent `xml:"ashCloudExtent"`
					Direction      string         `xml:"directionOfMotion"`
					Speed          string         `xml:"speedOfMotion"`
				} `xml:"VolcanicAshCloudObservedOrEstimated"`
			} `xml:"ashCloud"`
		} `xml:"VolcanicAshObservedOrEstimatedConditions"`
	} `xml:"observation"`
	Remarks          string `xml:"remarks"`
	NextAdvisoryTime struct {
		TimeInstant struct {
			TimePosition string `xml:"timePosition"`
		} `xml:"TimeInstant"`
	} `xml:"nextAdvisoryTime"`
}

type ashCloudExtent struct {
	AirspaceVolume struct {
		UpperLimit string `xml:"upperLimit"`
		LowerLimit string `xml:"lowerLimit"`
		Surface    struct {
			Patches struct {
				PolygonPatch struct {
					Exterior struct {
						LinearRing struct {
							PosList string `xml:"posList"`
						} `xml:"LinearRing"`
					} `xml:"exterior"`
				} `xml:"PolygonPatch"`
			} `xml:"patches"`
		} `xml:"horizontalProjection>Surface"`
	} `xml:"AirspaceVolume"`
}

func eventFromXML(buf []byte, fullURL string) (events.Event, bool, error) {
	var b bulletin
	if err := xml.Unmarshal(buf, &b); err != nil {
		return events.Event{}, false, err
	}
	if len(b.Information) == 0 {
		return events.Event{}, false, nil
	}
	a := b.Information[0].Advisory
	lat, lon, ok := parsePos(a.Volcano.EruptingVolcano.Position.Point.Pos)
	if !ok {
		return events.Event{}, false, nil
	}
	ts := parseTime(a.IssueTime.TimeInstant.TimePosition)
	if ts.IsZero() {
		ts = parseTime(a.Volcano.EruptingVolcano.EruptionDate)
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	volcano, volcanoID := splitVolcano(a.Volcano.EruptingVolcano.Name)
	ob := firstObservation(a)
	geom := polygonWKT(ob.PosList)
	props := map[string]any{
		"volcano":             volcano,
		"volcano_id":          volcanoID,
		"vaac":                firstNonEmpty(a.IssuingCentre.Unit.TimeSlice.UnitTimeSlice.Name, "WASHINGTON"),
		"state_or_region":     strings.TrimSpace(a.StateOrRegion),
		"issue_time":          a.IssueTime.TimeInstant.TimePosition,
		"advisory_nr":         strings.TrimSpace(a.AdvisoryNumber),
		"eruption_date":       a.Volcano.EruptingVolcano.EruptionDate,
		"eruption_details":    strings.TrimSpace(a.EruptionDetails),
		"information_source":  strings.TrimSpace(a.InformationSource),
		"summit_elevation_ft": strings.TrimSpace(a.SummitElevation),
		"obs_status":          ob.Status,
		"obs_is_estimated":    ob.IsEstimated,
		"obs_time":            ob.Time,
		"upper_limit_fl":      ob.UpperLimit,
		"lower_limit":         ob.LowerLimit,
		"direction_deg":       ob.Direction,
		"speed_kt":            ob.Speed,
		"ash_polygon_wkt":     geom,
		"remarks":             strings.TrimSpace(a.Remarks),
		"next_advisory":       a.NextAdvisoryTime.TimeInstant.TimePosition,
		"bulletin_identifier": b.BulletinIdentifier,
		"advisory_url":        fullURL,
		"source_api_endpoint": indexURL,
	}
	collectorutil.AddVAACScores(props)
	return events.Event{
		Ts:     ts,
		Source: "vaac_washington",
		ExtID:  firstNonEmpty(b.BulletinIdentifier, fmt.Sprintf("%s:%s:%s", a.AdvisoryNumber, volcano, a.IssueTime.TimeInstant.TimePosition)),
		Lat:    lat,
		Lon:    lon,
		Geom:   geom,
		Props:  props,
	}, true, nil
}

type observation struct {
	Status      string
	IsEstimated string
	Time        string
	UpperLimit  string
	LowerLimit  string
	Direction   string
	Speed       string
	PosList     string
}

func firstObservation(a vaa) observation {
	if len(a.Observation.Conditions) == 0 {
		return observation{}
	}
	c := a.Observation.Conditions[0]
	extent := c.AshCloud.Observed.AshCloudExtent.AirspaceVolume
	return observation{
		Status:      c.Status,
		IsEstimated: c.IsEstimated,
		Time:        c.PhenomenonTime.TimeInstant.TimePosition,
		UpperLimit:  extent.UpperLimit,
		LowerLimit:  extent.LowerLimit,
		Direction:   c.AshCloud.Observed.Direction,
		Speed:       c.AshCloud.Observed.Speed,
		PosList:     extent.Surface.Patches.PolygonPatch.Exterior.LinearRing.PosList,
	}
}

func parsePos(raw string) (float64, float64, bool) {
	parts := strings.Fields(raw)
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(parts[0], 64)
	lon, err2 := strconv.ParseFloat(parts[1], 64)
	return lat, lon, err1 == nil && err2 == nil
}

func polygonWKT(posList string) string {
	parts := strings.Fields(posList)
	if len(parts) < 6 || len(parts)%2 != 0 {
		return ""
	}
	coords := make([]string, 0, len(parts)/2)
	for i := 0; i+1 < len(parts); i += 2 {
		lat, err1 := strconv.ParseFloat(parts[i], 64)
		lon, err2 := strconv.ParseFloat(parts[i+1], 64)
		if err1 != nil || err2 != nil {
			return ""
		}
		coords = append(coords, fmt.Sprintf("%.6f %.6f", lon, lat))
	}
	if len(coords) < 4 {
		return ""
	}
	if coords[0] != coords[len(coords)-1] {
		coords = append(coords, coords[0])
	}
	return "POLYGON((" + strings.Join(coords, ",") + "))"
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

func parseTime(raw string) time.Time {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
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

func normalizeURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return baseURL + raw
	}
	return baseURL + "/products/atmosphere/vaac/" + raw
}
