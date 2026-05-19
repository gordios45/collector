// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package faa_status

import (
	"encoding/xml"
	"testing"
)

func TestFAAStatusXMLShape(t *testing.T) {
	const sample = `<AIRPORT_STATUS_INFORMATION>
  <Update_Time>Mon Apr 27 12:54:18 2026 GMT</Update_Time>
  <Delay_type>
    <Name>Ground Stop Programs</Name>
    <Ground_Stop_List>
      <Program><ARPT>DEN</ARPT><Reason>low visibility</Reason><End_Time>7:45 am MDT</End_Time></Program>
    </Ground_Stop_List>
  </Delay_type>
  <Delay_type>
    <Name>Ground Delay Programs</Name>
    <Ground_Delay_List>
      <Ground_Delay><ARPT>SFO</ARPT><Reason>other</Reason><Avg>52 minutes</Avg><Max>2 hours and 8 minutes</Max></Ground_Delay>
    </Ground_Delay_List>
  </Delay_type>
  <Delay_type>
    <Name>Airspace Flow Programs</Name>
    <Airspace_Flow_List>
      <Airspace_Flow>
        <CTL_Element>FCAJX1</CTL_Element><Reason>other</Reason><Avg>24 minutes</Avg>
        <Line><Point Lat="30.55" Long="-79.68"/><Point Lat="29.97" Long="-82.02"/></Line>
      </Airspace_Flow>
    </Airspace_Flow_List>
  </Delay_type>
  <Delay_type>
    <Name>Airport Closures</Name>
    <Airport_Closure_List>
      <Airport><ARPT>ASE</ARPT><Reason>airport closed</Reason><Start>Apr 23 at 15:00 UTC.</Start><Reopen>May 22 at 01:00 UTC.</Reopen></Airport>
    </Airport_Closure_List>
  </Delay_type>
</AIRPORT_STATUS_INFORMATION>`

	var doc faaXML
	if err := xml.Unmarshal([]byte(sample), &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.DelayTypes[0].GroundStopList[0].ARPT; got != "DEN" {
		t.Fatalf("ground stop ARPT = %q, want DEN", got)
	}
	if got := doc.DelayTypes[1].GroundDelayList[0].ARPT; got != "SFO" {
		t.Fatalf("ground delay ARPT = %q, want SFO", got)
	}
	if got := doc.DelayTypes[2].AirspaceFlowList[0].CTLElement; got != "FCAJX1" {
		t.Fatalf("airspace flow CTL_Element = %q, want FCAJX1", got)
	}
	if got := len(doc.DelayTypes[2].AirspaceFlowList[0].Line); got != 2 {
		t.Fatalf("airspace flow points = %d, want 2", got)
	}
	if got := doc.DelayTypes[3].AirportClosureList[0].ARPT; got != "ASE" {
		t.Fatalf("closure ARPT = %q, want ASE", got)
	}
}
