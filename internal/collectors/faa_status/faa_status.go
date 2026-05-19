// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// FAA NAS airport status.
// https://nasstatus.faa.gov/api/airport-status-information
// Endpoint returns XML (not JSON); we parse the closures+delays sections
// and flatten each airport into an event using its IATA code as ext_id.
package faa_status

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://nasstatus.faa.gov/api/airport-status-information"

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "faa_status" }
func (c *Collector) PollEvery() time.Duration { return 5 * time.Minute }

// airportLoc is a tiny hard-coded lat/lon lookup for the majors FAA reports
// on. Enough to render the few dozen airports that typically have status.
var airportLoc = map[string][2]float64{
	"ATL": {33.6407, -84.4277}, "LAX": {33.9425, -118.4081}, "ORD": {41.9742, -87.9073},
	"DFW": {32.8998, -97.0403}, "DEN": {39.8561, -104.6737}, "JFK": {40.6413, -73.7781},
	"SFO": {37.6213, -122.3790}, "SEA": {47.4502, -122.3088}, "LAS": {36.0801, -115.1522},
	"MCO": {28.4312, -81.3081}, "CLT": {35.2140, -80.9431}, "EWR": {40.6895, -74.1745},
	"LGA": {40.7769, -73.8740}, "MIA": {25.7959, -80.2870}, "PHX": {33.4342, -112.0116},
	"IAH": {29.9902, -95.3368}, "BOS": {42.3656, -71.0096}, "MSP": {44.8820, -93.2218},
	"DTW": {42.2124, -83.3534}, "FLL": {26.0742, -80.1506}, "PHL": {39.8744, -75.2424},
	"BWI": {39.1774, -76.6684}, "SLC": {40.7899, -111.9791}, "IAD": {38.9445, -77.4558},
	"DCA": {38.8512, -77.0402}, "MDW": {41.7868, -87.7522}, "HNL": {21.3187, -157.9225},
	"SAN": {32.7338, -117.1933}, "TPA": {27.9755, -82.5332}, "STL": {38.7487, -90.3700},
	"PDX": {45.5898, -122.5951}, "BNA": {36.1245, -86.6782}, "AUS": {30.1975, -97.6664},
	"OAK": {37.7213, -122.2208}, "MSY": {29.9911, -90.2581}, "SMF": {38.6953, -121.5908},
	"RDU": {35.8801, -78.7880}, "SJC": {37.3639, -121.9289},
	"ASE": {39.2232, -106.8688},
}

type airportStatus struct {
	ARPT    string `xml:"ARPT"`
	Reason  string `xml:"Reason"`
	Type    string `xml:"Type"`
	Start   string `xml:"Start"`
	End     string `xml:"End"`
	EndTime string `xml:"End_Time"`
	Reopen  string `xml:"Reopen"`
	Min     string `xml:"Min"`
	Max     string `xml:"Max"`
	Avg     string `xml:"Avg"`
	Trend   string `xml:"Trend"`
}

type flowPoint struct {
	Lat float64 `xml:"Lat,attr"`
	Lon float64 `xml:"Long,attr"`
}

type airspaceFlow struct {
	CTLElement       string      `xml:"CTL_Element"`
	Reason           string      `xml:"Reason"`
	AFPStartTime     string      `xml:"AFP_StartTime"`
	AFPEndTime       string      `xml:"AFP_EndTime"`
	FCAStartDateTime string      `xml:"FCA_StartDateTime"`
	FCAEndDateTime   string      `xml:"FCA_EndDateTime"`
	Avg              string      `xml:"Avg"`
	Floor            string      `xml:"Floor"`
	Ceiling          string      `xml:"Ceiling"`
	Line             []flowPoint `xml:"Line>Point"`
}

type faaXML struct {
	UpdateTime string `xml:"Update_Time"`
	DelayTypes []struct {
		Name               string          `xml:"Name"`
		AirportClosureList []airportStatus `xml:"Airport_Closure_List>Airport"`
		GroundDelayList    []airportStatus `xml:"Ground_Delay_List>Ground_Delay"`
		ArriveDepartPair   []airportStatus `xml:"Arrive_Depart_Delay_List>Airport"`
		GroundStopList     []airportStatus `xml:"Ground_Stop_List>Program"`
		AirspaceFlowList   []airspaceFlow  `xml:"Airspace_Flow_List>Airspace_Flow"`
	} `xml:"Delay_type"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	hdrs := map[string]string{
		"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
		"Accept":     "text/xml,application/xml,*/*",
	}
	buf, err := httpx.GetBytes(ctx, url, hdrs)
	if err != nil {
		// Keep the browser-like TLS path as fallback; historically this feed
		// was behind Akamai rules that rejected Go's default ClientHello.
		buf, err = httpx.GetBytesBrowser(ctx, url, hdrs)
		if err != nil {
			return nil, err
		}
	}
	var doc faaXML
	if err := xml.Unmarshal(buf, &doc); err != nil {
		return nil, fmt.Errorf("parse xml: %w", err)
	}

	now := time.Now().UTC()
	out := []events.Event{}
	push := func(category string, list []airportStatus) {
		for _, a := range list {
			code := strings.ToUpper(strings.TrimSpace(a.ARPT))
			if code == "" {
				continue
			}
			loc, ok := airportLoc[code]
			if !ok {
				continue
			}
			props := map[string]any{
				"iata":             code,
				"category":         category,
				"reason":           a.Reason,
				"type":             a.Type,
				"start":            a.Start,
				"end":              firstNonEmpty(a.End, a.EndTime, a.Reopen),
				"min":              a.Min,
				"max":              a.Max,
				"avg":              a.Avg,
				"trend":            a.Trend,
				"feed_update_time": doc.UpdateTime,
			}
			collectorutil.AddFAAStatusScores(props)
			out = append(out, events.Event{
				Ts: now, Source: "faa_status", ExtID: code + "_" + category,
				Lat: loc[0], Lon: loc[1], Props: props,
			})
		}
	}
	pushFlow := func(list []airspaceFlow) {
		for _, f := range list {
			id := strings.TrimSpace(f.CTLElement)
			if id == "" || len(f.Line) == 0 {
				continue
			}
			var lat, lon float64
			for _, p := range f.Line {
				lat += p.Lat
				lon += p.Lon
			}
			lat /= float64(len(f.Line))
			lon /= float64(len(f.Line))
			props := map[string]any{
				"category":            "airspace_flow",
				"ctl_element":         id,
				"reason":              f.Reason,
				"avg":                 f.Avg,
				"floor":               f.Floor,
				"ceiling":             f.Ceiling,
				"afp_start_time":      f.AFPStartTime,
				"afp_end_time":        f.AFPEndTime,
				"fca_start_date_time": f.FCAStartDateTime,
				"fca_end_date_time":   f.FCAEndDateTime,
				"feed_update_time":    doc.UpdateTime,
			}
			collectorutil.AddFAAStatusScores(props)
			out = append(out, events.Event{
				Ts:     now,
				Source: "faa_status",
				ExtID:  id + "_airspace_flow",
				Lat:    lat,
				Lon:    lon,
				Props:  props,
			})
		}
	}
	for _, t := range doc.DelayTypes {
		push("closure", t.AirportClosureList)
		push("ground_delay", t.GroundDelayList)
		push("arrive_depart", t.ArriveDepartPair)
		push("ground_stop", t.GroundStopList)
		pushFlow(t.AirspaceFlowList)
	}
	return out, nil
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return x
		}
	}
	return ""
}
