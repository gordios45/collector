// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package airline_safety

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

func TestIATAISSARegistryTableParsing(t *testing.T) {
	html := []byte(`<table class="datatable">
<thead><tr><td>Airline name</td><td>Region</td><td>Country / Territory</td><td>Registration Expiry</td><td>Registration Comments</td><td>ICAO Code</td><td>IATA Code</td></tr></thead>
<tbody><tr><td><a href="/airline">Azul Conecta</a></td><td>The Americas</td><td>Brazil</td><td>28 May 2027</td><td></td><td>OWT</td><td>2F</td></tr></tbody>
</table>`)
	rows := tableRowsByHeader(html, "airline_name", "registration_expiry")
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	if rows[0]["airline_name"] != "Azul Conecta" || rows[0]["icao_code"] != "OWT" {
		t.Fatalf("bad row: %#v", rows[0])
	}
}

func TestEUAirSafetyListXLSXParsing(t *testing.T) {
	f := excelize.NewFile()
	f.SetSheetName("Sheet1", "Annex A")
	f.SetSheetRow("Annex A", "A1", &[]any{"Name of the legal entity of the air carrier as indicated on its AOC (and its trading name, if different)", "Air Operator Certificate ('AOC') Number or Operating Licence Number", "State of the Operator", "ICAO three letter designator"})
	f.SetSheetRow("Annex A", "A2", &[]any{"Example Air", "AOC 001", "Exampleland", "EXA"})
	f.NewSheet("Annex B")
	f.SetSheetRow("Annex B", "A1", &[]any{"Name of the legal entity of the air carrier as indicated on its AOC (and its trading name, if different)", "Air Operator Certificate ('AOC') Number", "State of the Operator", "Aircraft type restricted", "ICAO three letter designator"})
	f.SetSheetRow("Annex B", "A2", &[]any{"Restricted Air", "AOC 002", "Restrictia", "B737", "RST"})
	var buf bytes.Buffer
	if _, err := f.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	evs, err := parseEUAirSafetyListXLSX(buf.Bytes(), ts, "https://example.test/asl.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("events = %d", len(evs))
	}
	if evs[0].Source != euAirSafetyListSourceID || evs[0].Props["restriction_type"] != "operating_ban" {
		t.Fatalf("bad Annex A event: %#v", evs[0])
	}
	if evs[1].Props["restriction_type"] != "operational_restriction" {
		t.Fatalf("bad Annex B event: %#v", evs[1])
	}
}

func TestFAAIASAResultDocumentParsing(t *testing.T) {
	page := []byte(`<html><body><a href="/sites/faa.gov/files/IASAWSR119r.pdf">IASAWSR119r.pdf</a><div>Last updated: Wednesday, April 23, 2025</div></body></html>`)
	if got := firstLinkMatching(page, faaIASAResultsURL, "IASAWS"); !strings.HasSuffix(got, "/IASAWSR119r.pdf") {
		t.Fatalf("pdf link = %q", got)
	}
	if got := sourceDateOrModified(page, time.Time{}); got.Format("2006-01-02") != "2025-04-23" {
		t.Fatalf("date = %s", got)
	}
}

func TestICAOUSOAPViewerMetadataParsing(t *testing.T) {
	page := `<iframe src="https://istars.icao.int/Sites/String"></iframe><a href="/safety/iStars/Pages/API-Data-Service.aspx">iSTARS API Data Service</a>`
	if got := firstMatch(page, `(?is)<iframe[^>]+src="([^"]+)"`); got != "https://istars.icao.int/Sites/String" {
		t.Fatalf("iframe = %q", got)
	}
	if got := firstLinkMatching([]byte(page), icaoUSOAPViewerURL, "API-Data-Service"); !strings.Contains(got, "/API-Data-Service.aspx") {
		t.Fatalf("api link = %q", got)
	}
}

func TestFAASDRCSVParsing(t *testing.T) {
	csv := `OperatorControlNumber,DifficultyDate,SubmissionDate,OperatorDesignator,RegistryNNumber,AircraftMake,AircraftModel,PartName,PartCondition,StageOfOperationCode,Discrepancy
OLD,01/01/2026,2026-01-02T14:09:41.140-05:00,AALA,123AA,BOEING,737,FLOORBEAM,CRACKED,IN,old
NEW,05/01/2026,2026-05-02T14:09:41.140-05:00,CALA,456CC,AIRBUS,A320,CHILLER,ODOR,CR,new
`
	evs, err := parseFAASDRCSV(strings.NewReader(csv), "https://example.test/SDR-2026.csv", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].ExtID != "sdr:NEW" {
		t.Fatalf("events: %#v", evs)
	}
	if evs[0].Props["aircraft_model"] != "A320" {
		t.Fatalf("props: %#v", evs[0].Props)
	}
}

func TestNTSBDirectoryParsing(t *testing.T) {
	page := []byte(`<table><tr><th>File name</th><th>Date created</th><th>File size</th><th>File link</th></tr>
<tr><td id="fileName">avall.zip</td><td id="fileDate">5/1/2026 6:52:03 AM</td><td id="fileSize">94435592</td><td><a href="/avdata/FileDirectory/DownloadFile?fileID=avall">avall.zip</a></td></tr>
<tr><td id="fileName">up15MAY.zip</td><td id="fileDate">5/15/2026 3:00:40 AM</td><td id="fileSize">653472</td><td><a href="/avdata/FileDirectory/DownloadFile?fileID=up15MAY">up15MAY.zip</a></td></tr></table>`)
	evs := parseNTSBDirectory(page, time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC), ntsbAvDataURL)
	if len(evs) != 2 {
		t.Fatalf("events = %d", len(evs))
	}
	if evs[0].Source != ntsbAccidentsSourceID || evs[0].Props["file_url"] == "" {
		t.Fatalf("bad event: %#v", evs[0])
	}
}

func TestEASACZIBJSONParsing(t *testing.T) {
	raw := []byte(`{"conflict_zones":[{"Nid":"20599","issued_date":"2017-03-31T00:00:00+0300","valid_until_date":"31/10/2026","field_easa_valid_until_descr":"<p>31/10/2026, unless reviewed earlier.</p>","name":"Airspace of Syria","status":"Active","country":"Syria","coordinates":"33.5130695, 36.3095814","updated":"<time datetime=\"2026-05-12T16:42:06+03:00\">2026-05-12T16:42:06+0300</time>"}]}`)
	evs, err := parseEASACZIBJSON(raw, time.Time{}, easaCZIBJSONURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].ExtID != "czib:20599" || !evs[0].HasPoint() {
		t.Fatalf("events: %#v", evs)
	}
}

func TestFAAFlightRestrictionsParsing(t *testing.T) {
	page := []byte(`<!--PAGEWATCH--><h2>Haiti</h2><ul><li><a href="/air_traffic/publications/us_restrictions/KICZ_NOTAM_A0024-26_Haiti_Prohibition.pdf">KICZ NOTAM A0024/26 - Security - United States of America Prohibition Against Certain Flights in the Territory and Airspace of Haiti</a></li></ul><!--/PAGEWATCH--><div>Last updated: Thursday, March 19, 2026</div>`)
	evs := parseFAAFlightRestrictions(page, time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC), faaFlightRestrictionsURL)
	if len(evs) != 1 {
		t.Fatalf("events = %d", len(evs))
	}
	if evs[0].Props["restriction_area"] != "Haiti" || !strings.Contains(evs[0].Props["document_url"].(string), "Haiti_Prohibition") {
		t.Fatalf("bad event: %#v", evs[0])
	}
}

func TestDOTCertificatedCarrierPageParsing(t *testing.T) {
	page := []byte(`<a href="/sites/dot.gov/files/2026-04/Cert%20Carrier%20List_April%202026_0.pdf">Cert Carrier List_April 2026_0.pdf</a><div>Last updated: Tuesday, April 21, 2026</div>`)
	if got := firstLinkMatching(page, dotCertificatedCarrierURL, "Cert Carrier"); !strings.Contains(got, "Cert%20Carrier%20List") {
		t.Fatalf("document link = %q", got)
	}
	if got := sourceDateOrModified(page, time.Time{}); got.Format("2006-01-02") != "2026-04-21" {
		t.Fatalf("date = %s", got)
	}
}

func TestAirlineReportsMonitor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Mon, 18 May 2026 12:00:00 GMT")
		w.Header().Set("ETag", `"v1"`)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("<title>Annual Report</title>"))
		}
	}))
	defer srv.Close()
	ev, ok := monitorURL(t.Context(), srv.Client(), srv.URL)
	if !ok {
		t.Fatal("monitorURL returned no event")
	}
	if ev.Source != airlineReportsSourceID || ev.Props["status_code"] != http.StatusOK {
		t.Fatalf("bad event: %#v", ev)
	}
}

func TestIATAIOSAMonitorMetadata(t *testing.T) {
	ts := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	props := baseProps("IATA Operational Safety Audit Registry", iataIOSAPageURL, ts, 14*24*time.Hour, "iata_iosa_registry_page")
	props["registry_url"] = iataIOSARegistryURL
	if props["registry_url"] != iataIOSARegistryURL || props["source_api_endpoint"] != iataIOSAPageURL {
		t.Fatalf("props: %#v", props)
	}
}
