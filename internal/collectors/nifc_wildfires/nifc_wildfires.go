// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NIFC WFIGS — active US wildfire incidents (named, with acreage / containment).
//
// Source: National Interagency Fire Center, WFIGS Incident Locations Current
// dataset (ArcGIS REST). Each feature is a wildfire incident with point
// geometry plus the canonical management attributes (name, acres, %
// containment, cause, state). Filtered to active wildfires
// (IncidentTypeCategory='WF' AND FireOutDateTime IS NULL).
//
// Complements firms (satellite hot-spot detection): firms shows where
// pixels are burning, this layer attaches a name/acreage/containment to
// the incident managing that fire.
package nifc_wildfires

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const baseURL = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Incident_Locations_Current/FeatureServer/0/query"

func queryURL() string {
	v := url.Values{}
	v.Set("where", "IncidentTypeCategory='WF' AND FireOutDateTime IS NULL")
	v.Set("outFields", "IrwinID,UniqueFireIdentifier,IncidentName,IncidentTypeCategory,IncidentTypeKind,IncidentShortDescription,IncidentSize,DiscoveryAcres,PercentContained,FireDiscoveryDateTime,FireOutDateTime,FireCause,FireCauseGeneral,POOState,POOCounty,POOJurisdictionalAgency,IncidentComplexityLevel,FireBehaviorGeneral,TotalIncidentPersonnel,IncidentManagementOrganization,ModifiedOnDateTime_dt")
	v.Set("outSR", "4326")
	v.Set("f", "json")
	v.Set("resultRecordCount", "2000")
	return baseURL + "?" + v.Encode()
}

type geometry struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type feature struct {
	Attributes map[string]any `json:"attributes"`
	Geometry   *geometry      `json:"geometry"`
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nifc_wildfires" }
func (c *Collector) PollEvery() time.Duration { return 30 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var resp struct {
		Features []feature `json:"features"`
		Error    *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := httpx.GetJSON(ctx, queryURL(), map[string]string{"Accept": "application/json"}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		// ArcGIS sometimes returns 200 with an error body; treat as failure.
		return nil, &arcgisErr{code: resp.Error.Code, msg: resp.Error.Message}
	}
	out := make([]events.Event, 0, len(resp.Features))
	for _, f := range resp.Features {
		if f.Geometry == nil {
			continue
		}
		// IRWIN ID is the canonical cross-system fire identifier; fall back
		// to UniqueFireIdentifier if missing.
		ext := strOf(f.Attributes["IrwinID"])
		if ext == "" {
			ext = strOf(f.Attributes["UniqueFireIdentifier"])
		}
		if ext == "" {
			continue
		}
		ts := time.Now().UTC()
		if v, ok := f.Attributes["ModifiedOnDateTime_dt"].(float64); ok && v > 0 {
			// ArcGIS returns epoch milliseconds.
			ts = time.UnixMilli(int64(v)).UTC()
		} else if v, ok := f.Attributes["FireDiscoveryDateTime"].(float64); ok && v > 0 {
			ts = time.UnixMilli(int64(v)).UTC()
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "nifc_wildfires",
			ExtID:  ext,
			Lat:    f.Geometry.Y,
			Lon:    f.Geometry.X,
			Props:  f.Attributes,
		})
	}
	return out, nil
}

type arcgisErr struct {
	code int
	msg  string
}

func (e *arcgisErr) Error() string {
	return fmt.Sprintf("arcgis error %d: %s", e.code, e.msg)
}

func strOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
