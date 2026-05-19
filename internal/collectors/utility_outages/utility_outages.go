// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package utility_outages ingests no-key utility outage-map feeds.
//
// These feeds complement grid-operator telemetry (EIA/ENTSO-E) with local
// customer outage reports: affected customer counts, causes, status, ETRs,
// and outage polygons/centroids where the public map exposes them.
package utility_outages

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"
)

const sourceID = "utility_outages"

type Collector struct{}

var postClient = &http.Client{Timeout: 20 * time.Second}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var out []events.Event
	out = append(out, c.fetchDTE(ctx)...)
	out = append(out, c.fetchSeattleCityLight(ctx)...)
	out = append(out, c.fetchNueces(ctx)...)
	out = append(out, c.fetchLubbock(ctx)...)
	out = append(out, c.fetchSCE(ctx)...)
	out = append(out, c.fetchCenterPoint(ctx)...)
	out = append(out, c.fetchEPB(ctx)...)
	out = append(out, c.fetchHECO(ctx)...)
	out = append(out, c.fetchEntergy(ctx)...)
	out = append(out, c.fetchNOVEC(ctx)...)
	out = append(out, c.fetchMidAmerican(ctx)...)
	out = append(out, c.fetchPGE(ctx)...)
	out = append(out, c.fetchHuntsville(ctx)...)
	out = append(out, c.fetchRockyMountain(ctx)...)
	return dedupe(out), nil
}

func (c *Collector) fetchDTE(ctx context.Context) []events.Event {
	const endpoint = "https://outagemap.serv.dteenergy.com/GISRest/services/OMP/OutageLocations/MapServer/2/query?f=pjson&where=1%3D1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometryType=esriGeometryEnvelope&inSR=102100&outFields=*&outSR=4326"
	var raw arcResponse
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil || raw.Error.Message != "" {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, wkt, ok := centroidAndWKT(f.Geometry)
		if !ok {
			continue
		}
		a := f.Attributes
		id := firstNonEmpty(textAny(a["JOB_ID"]), stableID(fmt.Sprint(a)))
		ts := parseTimeAny(a["OFF_DTTM"], a["CREATIONDATE"], a["CREATED_DATE"])
		affected, _ := floatAny(a["TOAL_CUST_AFFECTED"])
		props := baseProps("DTE Energy", endpoint, "https://outage.dteenergy.com/map", id, affected)
		copyProps(props, a)
		props["cause"] = textAny(a["CAUSE"])
		props["estimated_restoration_time"] = timeString(parseTimeAny(a["EST_REP_DTTM"]))
		props["outage_score"] = outageScore(affected, props["cause"], "")
		out = append(out, event(ts, "dte:"+id, lat, lon, wkt, props))
	}
	return out
}

func (c *Collector) fetchCenterPoint(ctx context.Context) []events.Event {
	const endpoint = "http://gis.centerpointenergy.com/arcgis/rest/services/Outage/OUTAGE_TRACKER_OEP_ALL/MapServer/0/query?f=json&where=1%3D1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=4326"
	var raw arcResponse
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil || raw.Error.Message != "" {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, wkt, ok := centroidAndWKT(f.Geometry)
		if !ok {
			continue
		}
		a := f.Attributes
		id := firstNonEmpty(textAny(a["OBJECTID"]), textAny(a["objectid"]), stableID(fmt.Sprint(a)))
		affected, _ := maxFloat(a, "CUSTOMERS", "CustAffected", "cust_a", "COUNT_")
		ts := parseTimeAny(a["START_TIME"], a["StartTime"], a["CREATED_DATE"], a["LASTUPDATE"])
		props := baseProps("CenterPoint Energy", endpoint, "http://gis.centerpointenergy.com/outagetracker/", id, affected)
		copyProps(props, a)
		props["outage_score"] = outageScore(affected, props["cause"], props["status"])
		out = append(out, event(ts, "centerpoint:"+id, lat, lon, wkt, props))
	}
	return out
}

func (c *Collector) fetchSeattleCityLight(ctx context.Context) []events.Event {
	const endpoint = "https://utilisocial.io/datacapable/v1/map/events/SCL?inProgress=true"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		var wrapped struct {
			Events []map[string]any `json:"events"`
			Data   []map[string]any `json:"data"`
		}
		if err2 := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &wrapped); err2 != nil {
			return nil
		}
		rows = append(wrapped.Events, wrapped.Data...)
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["latitude"])
		lon, lonOK := floatAny(row["longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(textAny(row["identifier"]), textAny(row["id"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["createdDate"], row["startTime"])
		affected, _ := floatAny(row["numPeople"])
		props := baseProps("Seattle City Light", endpoint, "https://www.seattle.gov/city-light/outages", id, affected)
		copyProps(props, row)
		props["city"] = textAny(row["city"])
		props["state"] = textAny(row["state"])
		props["estimated_restoration_time"] = textAny(row["etrDate"])
		props["outage_score"] = outageScore(affected, nil, "")
		out = append(out, event(ts, "seattle_city_light:"+id, lat, lon, polygonWKTFromUtilisocial(row), props))
	}
	return out
}

func (c *Collector) fetchNueces(ctx context.Context) []events.Event {
	const endpoint = "https://outage.nueceselectric.org/data/outages.json"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		pt, _ := row["outagePoint"].(map[string]any)
		lat, latOK := floatAny(pt["lat"])
		lon, lonOK := floatAny(pt["lng"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(textAny(row["outageRecID"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["outageStartTime"])
		affected, _ := floatAny(row["customersOutInitially"])
		current, _ := floatAny(row["customersOutNow"])
		props := baseProps("Nueces Electric Cooperative", endpoint, "https://outage.nueceselectric.org/", id, affected)
		copyProps(props, row)
		props["customers_out_now"] = current
		props["estimated_restoration_time"] = firstNonEmpty(textAny(row["outageEndTime"]), textAny(row["estimatedTimeOfRestoral"]))
		props["outage_score"] = outageScore(math.Max(affected, current), nil, "")
		out = append(out, event(ts, "nueces:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchLubbock(ctx context.Context) []events.Event {
	const endpoint = "https://electricoutage.ci.lubbock.tx.us/GridVuServer/Public/getAllOutages"
	var raw map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := []events.Event{}
	for _, row := range mapsAt(raw, "outageLst") {
		lat, latOK := floatAny(row["lat"])
		lon, lonOK := floatAny(row["lon"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(textAny(row["id"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["date"])
		affected, _ := floatAny(row["custAffected"])
		props := baseProps("Lubbock Power & Light", endpoint, "https://electricoutage.ci.lubbock.tx.us/gridvu/", id, affected)
		copyProps(props, row)
		props["cause"] = textAny(row["reason"])
		props["status"] = textAny(row["status"])
		props["outage_score"] = outageScore(affected, row["reason"], row["status"])
		out = append(out, event(ts, "lubbock:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchSCE(ctx context.Context) []events.Event {
	const endpoint = "https://prodms.dms.sce.com/outage/v1/currentOutages"
	var raw map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	rows := mapsPath(raw, "outageMapDataResponse", "AOCIncidents", "incident")
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["centroidY"])
		lon, lonOK := floatAny(row["centroidX"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		id := firstNonEmpty(textAny(row["incidentId"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["outageStartDateTime"])
		affected, _ := floatAny(row["numberOfCustomersAffected"])
		props := baseProps("Southern California Edison", endpoint, "https://www.sce.com/outage-center/check-outage-status", id, affected)
		copyProps(props, row)
		props["cause"] = textAny(row["memoCauseCodeDescription"])
		props["status"] = textAny(row["crewStatusCodeDescription"])
		props["county"] = strings.TrimSpace(textAny(row["countyName"]))
		props["city"] = textAny(row["cityName"])
		props["estimated_restoration_time"] = textAny(row["estimateCLUDateTime"])
		props["outage_score"] = outageScore(affected, row["memoCauseCodeDescription"], row["crewStatusCodeDescription"])
		out = append(out, event(ts, "sce:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchEPB(ctx context.Context) []events.Event {
	const endpoint = "https://api.epb.com/web/api/v1/outages/power/incidents"
	var raw struct {
		OutagePoints []map[string]any `json:"outage_points"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.OutagePoints))
	for _, row := range raw.OutagePoints {
		lat, latOK := floatAny(row["latitude"])
		lon, lonOK := floatAny(row["longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) || math.Abs(lat) < 1 || math.Abs(lon) < 1 {
			continue
		}
		affected, _ := floatAny(row["customer_qty"])
		status := strings.ReplaceAll(textAny(row["incident_status"]), "_", " ")
		id := stableID(fmt.Sprintf("%0.5f:%0.5f:%0.f:%s", lat, lon, affected, status))
		props := baseProps("EPB", endpoint, "https://epb.com/outage-map", id, affected)
		copyProps(props, row)
		props["state"] = "TN"
		props["county"] = "Hamilton County"
		props["status"] = status
		props["outage_score"] = outageScore(affected, nil, status)
		out = append(out, event(time.Now().UTC(), "epb:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchHECO(ctx context.Context) []events.Event {
	const endpoint = "https://outageapi-heco.azurewebsites.net/api/outages/text/heco"
	var raw struct {
		Records []map[string]any `json:"CustomerOutageRecord"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Records))
	for _, row := range raw.Records {
		lat, lon, ok := hecoPoint(row)
		if !ok {
			continue
		}
		id := firstNonEmpty(textAny(row["EventId"]), stableID(fmt.Sprint(row)))
		ts := parseTimeAny(row["OutageStartTime"], row["UploadTime"])
		affected, _ := floatAny(row["CustomersOut"])
		props := baseProps("Hawaiian Electric", endpoint, "https://www.hawaiianelectric.com/safety-and-outages/power-outages/oahu-outage-map", id, affected)
		copyProps(props, row)
		props["state"] = "HI"
		props["city"] = textAny(row["Area"])
		props["cause"] = textAny(row["Cause"])
		props["status"] = textAny(row["Status"])
		props["estimated_restoration_time"] = textAny(row["EstimatedRestoreTime"])
		props["customers_out_now"] = affected
		props["outage_score"] = outageScore(affected, row["Cause"], row["Status"])
		out = append(out, event(ts, "heco:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchEntergy(ctx context.Context) []events.Event {
	const endpoint = "https://arcgis.entergy.datacapable.com/arcgis/rest/services/Public/MapServer/2/query?f=json&outFields=*&where=1%3D1%20AND%20((zoomLevel%20%3D%2013)%20AND%20((zoomLevel%20%3D%2013%20)%20OR%20(zoomLevel%20%3D10%20AND%20id%20IS%20NULL)))"
	var raw arcResponse
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil || raw.Error.Message != "" {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Features))
	for _, f := range raw.Features {
		lat, lon, wkt, ok := centroidAndWKT(f.Geometry)
		if !ok {
			continue
		}
		a := f.Attributes
		id := firstNonEmpty(textAny(a["id"]), textAny(a["identifier"]), textAny(a["objectid"]), stableID(fmt.Sprint(a)))
		affected, _ := floatAny(a["numpeople"])
		props := baseProps("Entergy", endpoint, "https://www.etrviewoutage.com/map?state=la", id, affected)
		copyProps(props, a)
		props["outage_score"] = outageScore(affected, nil, textAny(a["status"]))
		out = append(out, event(time.Now().UTC(), "entergy:"+id, lat, lon, wkt, props))
	}
	return out
}

func (c *Collector) fetchNOVEC(ctx context.Context) []events.Event {
	const endpoint = "https://www.novec.com/stormcenter/data/outagedtl.xml?1"
	buf, err := httpx.GetBytes(ctx, endpoint, map[string]string{"Accept": "application/xml,text/xml,*/*"})
	if err != nil {
		return nil
	}
	var raw struct {
		Outages []struct {
			ID     string `xml:"id,attr"`
			Cause  string `xml:"cause,attr"`
			County string `xml:"county,attr"`
			ETime  string `xml:"eTime,attr"`
			STime  string `xml:"sTime,attr"`
			NumOut string `xml:"numOut,attr"`
			Type   string `xml:"type,attr"`
			Lat    string `xml:"lat,attr"`
			Lng    string `xml:"lng,attr"`
		} `xml:"outage"`
	}
	if err := xml.Unmarshal(buf, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Outages))
	for _, row := range raw.Outages {
		lat, latOK := floatAny(row.Lat)
		lon, lonOK := floatAny(row.Lng)
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		affected, _ := floatAny(row.NumOut)
		id := firstNonEmpty(row.ID, stableID(fmt.Sprint(row)))
		ts := parseTimeString(row.STime)
		props := baseProps("NOVEC", endpoint, "https://www.novec.com/stormcenter/index.cfm", id, affected)
		props["cause"] = row.Cause
		props["county"] = row.County
		props["state"] = "VA"
		props["type"] = row.Type
		props["estimated_restoration_time"] = row.ETime
		props["outage_score"] = outageScore(affected, row.Cause, row.Type)
		out = append(out, event(ts, "novec:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchMidAmerican(ctx context.Context) []events.Event {
	const endpoint = "https://www.midamericanenergy.com/outagewatch/api/Incident/GetIncidentOutageData/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, http.NoBody)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "gordios/0.1 (+https://github.com/gordios)")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Referer", "https://www.midamericanenergy.com/OutageWatch/dsk.html")
	r, err := postClient.Do(req)
	if err != nil {
		return nil
	}
	defer r.Body.Close()
	if r.StatusCode >= 400 {
		return nil
	}
	var rows []map[string]any
	if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		lat, latOK := floatAny(row["Latitude"])
		lon, lonOK := floatAny(row["Longitude"])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		affected, _ := floatAny(row["Downstream"])
		id := firstNonEmpty(textAny(row["IncidentID"]), stableID(fmt.Sprint(row)))
		ts := parseMonthDayTime(textAny(row["CreateDatetime"]))
		props := baseProps("MidAmerican Energy", endpoint, "https://www.midamericanenergy.com/OutageWatch/dsk.html", id, affected)
		copyProps(props, row)
		props["cause"] = textAny(row["LocationCause"])
		props["status"] = firstNonEmpty(textAny(row["StatusCd"]), textAny(row["FacJobStatusCd"]))
		props["city"] = textAny(row["METRO_AREA"])
		props["estimated_restoration_time"] = textAny(row["ETR"])
		props["outage_score"] = outageScore(affected, row["LocationCause"], row["StatusCd"])
		out = append(out, event(ts, "midamerican:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchPGE(ctx context.Context) []events.Event {
	const endpoint = "https://files.sfchronicle.com/project-feeds/outage-data-live.json"
	var raw struct {
		Regions []map[string]any `json:"outagesRegions"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := []events.Event{}
	for _, region := range raw.Regions {
		for _, row := range mapsAt(region, "outages") {
			lat, latOK := floatAny(firstNonEmpty(textAny(row["latitude"]), textAny(region["latitude"])))
			lon, lonOK := floatAny(firstNonEmpty(textAny(row["longitude"]), textAny(region["longitude"])))
			if !latOK || !lonOK || !validLatLon(lat, lon) {
				continue
			}
			id := firstNonEmpty(textAny(row["outageNumber"]), stableID(fmt.Sprint(row)))
			ts := parseTimeAny(row["outageStartTime"], row["lastUpdateTime"])
			affected, _ := floatAny(row["estCustAffected"])
			props := baseProps("PG&E / San Francisco Chronicle", endpoint, "https://www.sfchronicle.com/projects/pge-shutoff-power-outages-map/", id, affected)
			copyProps(props, row)
			props["city"] = textAny(region["regionName"])
			props["state"] = "CA"
			props["cause"] = textAny(row["cause"])
			props["status"] = textAny(row["crewCurrentStatus"])
			props["estimated_restoration_time"] = timeString(parseTimeAny(row["currentEtor"], row["autoEtor"]))
			props["outage_score"] = outageScore(affected, row["cause"], row["crewCurrentStatus"])
			out = append(out, event(ts, "pge:"+id, lat, lon, "", props))
		}
	}
	return out
}

func (c *Collector) fetchHuntsville(ctx context.Context) []events.Event {
	const endpoint = "https://www.hsvutil.org/maps/the_file.js?_=1615230798276"
	var rows []map[string]any
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &rows); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(rows))
	for _, row := range rows {
		parts := strings.Split(textAny(row["val"]), "_")
		if len(parts) < 2 {
			continue
		}
		lat, latOK := floatAny(parts[0])
		lon, lonOK := floatAny(parts[1])
		if !latOK || !lonOK || !validLatLon(lat, lon) {
			continue
		}
		affected := 1.0
		if len(parts) >= 3 {
			if v, ok := floatAny(parts[2]); ok {
				affected = v
			}
		}
		id := stableID(fmt.Sprintf("%0.4f:%0.4f:%0.f", lat, lon, affected))
		props := baseProps("Huntsville Utilities", endpoint, "https://www.hsvutil.org/outagemap/", id, affected)
		copyProps(props, row)
		props["city"] = "Huntsville"
		props["state"] = "AL"
		props["outage_score"] = outageScore(affected, nil, "")
		out = append(out, event(time.Now().UTC(), "huntsville:"+id, lat, lon, "", props))
	}
	return out
}

func (c *Collector) fetchRockyMountain(ctx context.Context) []events.Event {
	const endpoint = "https://www.rockymountainpower.net/etc/pcorp/datafiles/outagemap/listUT.json"
	var raw struct {
		LastUpdated string           `json:"last_upd"`
		Counties    []map[string]any `json:"counties"`
	}
	if err := httpx.GetJSON(ctx, endpoint, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return nil
	}
	out := make([]events.Event, 0, len(raw.Counties))
	for _, row := range raw.Counties {
		county := textAny(row["countyName"])
		latLon, ok := utahCountyCentroids[strings.ToLower(county)]
		if !ok {
			continue
		}
		plan, _ := floatAny(row["custOutPlan"])
		unplan, _ := floatAny(row["custOutUnplan"])
		affected := plan + unplan
		if affected <= 0 {
			continue
		}
		id := stableID(county + ":" + raw.LastUpdated)
		ts := parseTimeString(raw.LastUpdated)
		props := baseProps("Rocky Mountain Power", endpoint, "https://www.rockymountainpower.net/outages-safety.html", id, affected)
		copyProps(props, row)
		props["county"] = county
		props["state"] = "UT"
		props["customers_out_now"] = affected
		props["planned_customers_out"] = plan
		props["unplanned_customers_out"] = unplan
		props["outage_score"] = outageScore(affected, nil, "")
		out = append(out, event(ts, "rocky_mountain:"+id, latLon[0], latLon[1], "", props))
	}
	return out
}

type arcResponse struct {
	Features []struct {
		Attributes map[string]any `json:"attributes"`
		Geometry   map[string]any `json:"geometry"`
	} `json:"features"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func baseProps(provider, endpoint, publicURL, id string, affected float64) map[string]any {
	return map[string]any{
		"source_provider":      provider,
		"source_api_endpoint":  endpoint,
		"source_public_url":    publicURL,
		"outage_id":            id,
		"affected_customers":   affected,
		"source_provider_kind": "public_utility_outage_map",
		"labels":               []string{"power_outage"},
	}
}

func event(ts time.Time, id string, lat, lon float64, wkt string, props map[string]any) events.Event {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	if wkt == "" {
		wkt = fmt.Sprintf("POINT(%f %f)", lon, lat)
	}
	addUtilityOutageSubtypeScores(props)
	props["source_payload_validity"] = map[string]any{
		"valid_start":    ts.Format(time.RFC3339),
		"valid_end":      time.Now().UTC().Add(4 * time.Hour).Format(time.RFC3339),
		"validity_basis": "public_outage_map_current_snapshot",
	}
	return events.Event{Ts: ts, Source: sourceID, ExtID: stableID(id), Lat: lat, Lon: lon, Geom: wkt, Props: props}
}

func outageScore(affected float64, cause, status any) float64 {
	score := propx.ClampFloat(math.Log1p(math.Max(0, affected))/3.0, 0.3, 3.2)
	text := strings.ToLower(textAny(cause) + " " + textAny(status))
	if strings.Contains(text, "transmission") || strings.Contains(text, "substation") {
		score += 0.5
	}
	if strings.Contains(text, "crew") || strings.Contains(text, "onsite") || strings.Contains(text, "on site") {
		score += 0.2
	}
	if affected >= 5000 {
		score += 0.6
	}
	return propx.ClampFloat(score, 0, 4)
}

func addUtilityOutageSubtypeScores(props map[string]any) {
	score, _ := floatAny(props["outage_score"])
	customers, _ := floatAny(props["affected_customers"])
	if current, ok := floatAny(props["customers_out_now"]); ok {
		customers = math.Max(customers, current)
	}
	if customers > 0 {
		props["affected_customers_score"] = propx.ClampFloat(math.Log1p(math.Max(0, customers))/4.0, 0, 3)
	}
	if customers >= 5000 || score >= 2.5 {
		props["major_outage_score"] = propx.ClampFloat(math.Max(score, 1.5), 0, 4)
	}
}

func centroidAndWKT(g map[string]any) (float64, float64, string, bool) {
	if x, xOK := floatAny(g["x"]); xOK {
		if y, yOK := floatAny(g["y"]); yOK {
			lon, lat := normalizeXY(x, y)
			if validLatLon(lat, lon) {
				return lat, lon, fmt.Sprintf("POINT(%f %f)", lon, lat), true
			}
		}
	}
	ringsRaw, ok := g["rings"].([]any)
	if !ok || len(ringsRaw) == 0 {
		return 0, 0, "", false
	}
	var all [][2]float64
	var ringWKTs []string
	for _, ringAny := range ringsRaw {
		ring, ok := ringAny.([]any)
		if !ok {
			continue
		}
		var pts []string
		for _, ptAny := range ring {
			pt, ok := ptAny.([]any)
			if !ok || len(pt) < 2 {
				continue
			}
			x, xOK := floatAny(pt[0])
			y, yOK := floatAny(pt[1])
			if !xOK || !yOK {
				continue
			}
			lon, lat := normalizeXY(x, y)
			if !validLatLon(lat, lon) {
				continue
			}
			all = append(all, [2]float64{lat, lon})
			pts = append(pts, fmt.Sprintf("%f %f", lon, lat))
		}
		if len(pts) >= 3 {
			if pts[0] != pts[len(pts)-1] {
				pts = append(pts, pts[0])
			}
			ringWKTs = append(ringWKTs, "("+strings.Join(pts, ",")+")")
		}
	}
	if len(all) == 0 {
		return 0, 0, "", false
	}
	var lat, lon float64
	for _, p := range all {
		lat += p[0]
		lon += p[1]
	}
	lat /= float64(len(all))
	lon /= float64(len(all))
	wkt := ""
	if len(ringWKTs) > 0 {
		wkt = "POLYGON(" + strings.Join(ringWKTs, ",") + ")"
	}
	return lat, lon, wkt, validLatLon(lat, lon)
}

func polygonWKTFromUtilisocial(row map[string]any) string {
	polys, ok := row["polygons"].([]any)
	if !ok || len(polys) == 0 {
		return ""
	}
	var rings []string
	for _, p := range polys {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		points, ok := pm["points"].([]any)
		if !ok {
			continue
		}
		pts := []string{}
		for _, item := range points {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			lat, latOK := floatAny(m["latitude"])
			lon, lonOK := floatAny(m["longitude"])
			if latOK && lonOK && validLatLon(lat, lon) {
				pts = append(pts, fmt.Sprintf("%f %f", lon, lat))
			}
		}
		if len(pts) >= 3 {
			if pts[0] != pts[len(pts)-1] {
				pts = append(pts, pts[0])
			}
			rings = append(rings, "("+strings.Join(pts, ",")+")")
		}
	}
	if len(rings) == 0 {
		return ""
	}
	return "POLYGON(" + strings.Join(rings, ",") + ")"
}

func hecoPoint(row map[string]any) (float64, float64, bool) {
	for _, key := range []string{"CircleCoordinates", "PolygonCoordinates"} {
		wrapper, _ := row[key].(map[string]any)
		if wrapper == nil {
			continue
		}
		coords, _ := wrapper["Coordinate"].([]any)
		for _, item := range coords {
			pt, _ := item.(map[string]any)
			lat, latOK := floatAny(pt["Y"])
			lon, lonOK := floatAny(pt["X"])
			if latOK && lonOK && validLatLon(lat, lon) {
				return lat, lon, true
			}
		}
	}
	return 0, 0, false
}

func normalizeXY(x, y float64) (float64, float64) {
	if math.Abs(x) > 180 || math.Abs(y) > 90 {
		return webMercatorToLonLat(x, y)
	}
	return x, y
}

func webMercatorToLonLat(x, y float64) (float64, float64) {
	lon := x * 180.0 / 20037508.34
	lat := math.Atan(math.Exp(y*math.Pi/20037508.34))*360.0/math.Pi - 90.0
	return lon, lat
}

func mapsAt(raw map[string]any, key string) []map[string]any {
	arr, _ := raw[key].([]any)
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func mapsPath(raw map[string]any, path ...string) []map[string]any {
	var cur any = raw
	for _, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[key]
	}
	if arr, ok := cur.([]any); ok {
		out := make([]map[string]any, 0, len(arr))
		for _, item := range arr {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	if m, ok := cur.(map[string]any); ok {
		return []map[string]any{m}
	}
	return nil
}

func maxFloat(m map[string]any, keys ...string) (float64, bool) {
	var best float64
	var ok bool
	for _, k := range keys {
		if v, vOK := floatAny(m[k]); vOK {
			best = math.Max(best, v)
			ok = true
		}
	}
	return best, ok
}

func copyProps(dst map[string]any, src map[string]any) {
	for k, v := range src {
		if _, exists := dst[k]; exists {
			dst["raw_"+k] = v
		} else {
			dst[k] = v
		}
	}
}

func parseTimeAny(vals ...any) time.Time {
	for _, v := range vals {
		if t := parseTimeString(textAny(v)); !t.IsZero() {
			return t
		}
	}
	return time.Now().UTC()
}

func parseTimeString(s string) time.Time {
	s = strings.TrimSpace(strings.TrimSuffix(s, " CST"))
	s = strings.TrimSuffix(s, " CDT")
	if s == "" || strings.EqualFold(s, "null") || strings.EqualFold(s, "none") {
		return time.Time{}
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
		time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000-0700", "2006-01-02T15:04:05.000+0000",
		"2006-01-02T15:04:05", "2006-01-02 15:04:05", "1/2/2006 3:04:05 PM", "January 2, 2006 03:04 PM", "Jan 2, 2006 03:04 PM",
		"01/02/2006 15:04:05", "01/02/2006 03:04:05 PM", "03:04 PM 01/02/2006", "15:04 PM 01/02/2006",
		"Monday, January 02 03:04 PM, 2006",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func parseMonthDayTime(s string) time.Time {
	s = strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
	if s == "" {
		return time.Now().UTC()
	}
	withYear := fmt.Sprintf("%d-%s", time.Now().UTC().Year(), s)
	for _, layout := range []string{"2006-01-02 03:04 PM", "2006-1-2 03:04 PM"} {
		if t, err := time.Parse(layout, withYear); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func floatAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func textAny(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(v))
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
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}

func dedupe(in []events.Event) []events.Event {
	seen := map[string]bool{}
	out := make([]events.Event, 0, len(in))
	for _, e := range in {
		if e.Source == "" || e.ExtID == "" || (e.Geom == "" && !e.HasPoint()) {
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

var utahCountyCentroids = map[string][2]float64{
	"beaver":     {38.3566, -113.2351},
	"box elder":  {41.5211, -113.0833},
	"cache":      {41.7233, -111.7447},
	"carbon":     {39.6482, -110.5885},
	"daggett":    {40.8870, -109.5047},
	"davis":      {40.9900, -112.1115},
	"duchesne":   {40.2970, -110.4252},
	"emery":      {39.0086, -110.7211},
	"garfield":   {37.8545, -111.4419},
	"grand":      {38.9585, -109.5893},
	"iron":       {37.8590, -113.2899},
	"juab":       {39.7026, -112.7833},
	"kane":       {37.2760, -111.8159},
	"millard":    {39.0738, -113.1011},
	"morgan":     {41.0885, -111.5732},
	"piute":      {38.3350, -112.1294},
	"rich":       {41.6320, -111.2447},
	"salt lake":  {40.6679, -111.9241},
	"san juan":   {37.6266, -109.8047},
	"sanpete":    {39.3723, -111.5752},
	"sevier":     {38.7470, -111.8047},
	"summit":     {40.8660, -110.9557},
	"tooele":     {40.4486, -113.1316},
	"uintah":     {40.1258, -109.5174},
	"utah":       {40.1199, -111.6701},
	"wasatch":    {40.3307, -111.1687},
	"washington": {37.2803, -113.5047},
	"wayne":      {38.3369, -110.9020},
	"weber":      {41.2712, -111.9147},
}
