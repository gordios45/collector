// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package vaac_washington

import (
	"strings"
	"testing"
)

func TestLatestXMLLinks(t *testing.T) {
	got := latestXMLLinks(`<a href="/products/atmosphere/vaac/volcanoes/xml_files/FVXX20_20260428_2053.xml">(XML)</a><a href="/products/atmosphere/vaac/volcanoes/xml_files/FVXX20_20260428_2053.xml">(XML)</a>`, 5)
	if len(got) != 1 {
		t.Fatalf("links=%v", got)
	}
	if got[0] != "https://www.ospo.noaa.gov/products/atmosphere/vaac/volcanoes/xml_files/FVXX20_20260428_2053.xml" {
		t.Fatalf("link=%q", got[0])
	}
}

func TestEventFromXML(t *testing.T) {
	raw := []byte(`<?xml version='1.0' encoding='UTF-8'?>
<MeteorologicalBulletin xmlns="http://def.wmo.int/collect/2014" xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:aixm="http://www.aixm.aero/schema/5.1.1">
  <meteorologicalInformation>
    <VolcanicAshAdvisory xmlns="http://icao.int/iwxxm/3.0">
      <issueTime><gml:TimeInstant><gml:timePosition>2026-04-28T20:53:00Z</gml:timePosition></gml:TimeInstant></issueTime>
      <issuingVolcanicAshAdvisoryCentre><Unit xmlns="http://www.aixm.aero/schema/5.1.1"><timeSlice><UnitTimeSlice><name>WASHINGTON</name></UnitTimeSlice></timeSlice></Unit></issuingVolcanicAshAdvisoryCentre>
      <volcano><EruptingVolcano xmlns="http://def.wmo.int/metce/2013"><name>FUEGO 342090</name><position><gml:Point><gml:pos>14.467 -90.867</gml:pos></gml:Point></position><eruptionDate>2026-04-28T20:53:00Z</eruptionDate></EruptingVolcano></volcano>
      <stateOrRegion>GUATEMALA</stateOrRegion>
      <summitElevation uom="[ft_i]">12346</summitElevation>
      <advisoryNumber>2026/497</advisoryNumber>
      <informationSource>GOES-19. NWP MODELS.</informationSource>
      <eruptionDetails>INTMT VA EMS</eruptionDetails>
      <observation><VolcanicAshObservedOrEstimatedConditions isEstimated="true" status="IDENTIFIABLE"><phenomenonTime><gml:TimeInstant><gml:timePosition>2026-04-28T20:20:00Z</gml:timePosition></gml:TimeInstant></phenomenonTime><ashCloud><VolcanicAshCloudObservedOrEstimated><ashCloudExtent><aixm:AirspaceVolume><aixm:upperLimit uom="FL">140</aixm:upperLimit><aixm:lowerLimit>GND</aixm:lowerLimit><aixm:horizontalProjection><aixm:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>14.467 -90.884 14.350 -91.250 14.267 -91.217 14.467 -90.867 14.467 -90.884</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></aixm:Surface></aixm:horizontalProjection></aixm:AirspaceVolume></ashCloudExtent><directionOfMotion uom="deg">225</directionOfMotion><speedOfMotion uom="[kn_i]">15</speedOfMotion></VolcanicAshCloudObservedOrEstimated></ashCloud></VolcanicAshObservedOrEstimatedConditions></observation>
      <remarks>LGT VA EMS</remarks>
      <nextAdvisoryTime><gml:TimeInstant><gml:timePosition>2026-04-29T03:00:00Z</gml:timePosition></gml:TimeInstant></nextAdvisoryTime>
    </VolcanicAshAdvisory>
  </meteorologicalInformation>
  <bulletinIdentifier>A_LUXX20KNES282053_C_KNES_20260428205414.xml</bulletinIdentifier>
</MeteorologicalBulletin>`)
	ev, ok, err := eventFromXML(raw, "https://example.test/vaa.xml")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("event skipped")
	}
	if ev.Source != "vaac_washington" || ev.Props["volcano"] != "FUEGO" || ev.Props["vaac"] != "WASHINGTON" {
		t.Fatalf("event wrong: %+v", ev)
	}
	if ev.Lat < 14.46 || ev.Lat > 14.47 || ev.Lon > -90.86 || ev.Lon < -90.88 {
		t.Fatalf("lat/lon wrong: %.6f %.6f", ev.Lat, ev.Lon)
	}
	if !strings.HasPrefix(ev.Geom, "POLYGON((") {
		t.Fatalf("geom=%q", ev.Geom)
	}
}
