// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// NHC — National Hurricane Center active tropical cyclones.
//
// Endpoint: https://www.nhc.noaa.gov/CurrentStorms.json
// Public, no auth, no rate limit. Returns one record per active storm
// across the Atlantic and Eastern/Central Pacific basins. Each record
// carries the cyclone's current center, intensity, pressure, movement,
// plus URLs to the forecast track / cone KMZ files (we ship the URLs
// in props so the layer can fetch the cone polygon on demand later).
//
// Off-season the activeStorms array is empty; that's a normal "0 events"
// tick, not an error.
package nhc

import (
	"context"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const url = "https://www.nhc.noaa.gov/CurrentStorms.json"

type advisory struct {
	URL string `json:"url"`
}
type kmz struct {
	ZoomURL string `json:"zoomURL"`
	KMZFile string `json:"kmzFile"`
}

type storm struct {
	ID               string   `json:"id"`
	BinNumber        string   `json:"binNumber"`
	Name             string   `json:"name"`
	Classification   string   `json:"classification"`
	Intensity        string   `json:"intensity"`
	Pressure         string   `json:"pressure"`
	Latitude         string   `json:"latitude"`
	LatitudeNumeric  float64  `json:"latitudeNumeric"`
	Longitude        string   `json:"longitude"`
	LongitudeNumeric float64  `json:"longitudeNumeric"`
	MovementDir      any      `json:"movementDir"`   // sometimes int, sometimes string
	MovementSpeed    any      `json:"movementSpeed"` // sometimes int, sometimes string
	LastUpdate       string   `json:"lastUpdate"`
	PublicAdvisory   advisory `json:"publicAdvisory"`
	ForecastTrack    kmz      `json:"forecastTrack"`
	ForecastCone     kmz      `json:"forecastCone"`
	BestTrack        kmz      `json:"bestTrack"`
}

type Collector struct{}

func New() (*Collector, error)                { return &Collector{}, nil }
func (c *Collector) ID() string               { return "nhc" }
func (c *Collector) PollEvery() time.Duration { return 10 * time.Minute }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var resp struct {
		ActiveStorms []storm `json:"activeStorms"`
	}
	if err := httpx.GetJSON(ctx, url, nil, &resp); err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(resp.ActiveStorms))
	for _, s := range resp.ActiveStorms {
		if s.ID == "" {
			continue
		}
		// Skip if no usable position (shouldn't happen for an active storm).
		if s.LatitudeNumeric == 0 && s.LongitudeNumeric == 0 {
			continue
		}
		ts := time.Now().UTC()
		if s.LastUpdate != "" {
			if t, err := time.Parse(time.RFC3339, s.LastUpdate); err == nil {
				ts = t.UTC()
			}
		}
		props := map[string]any{
			"id":             s.ID,
			"name":           s.Name,
			"classification": s.Classification,
			"intensity":      s.Intensity,
			"pressure":       s.Pressure,
			"binNumber":      s.BinNumber,
			"latitude":       s.Latitude,
			"longitude":      s.Longitude,
			"movementDir":    asString(s.MovementDir),
			"movementSpeed":  asString(s.MovementSpeed),
			"lastUpdate":     s.LastUpdate,
			"advisoryUrl":    s.PublicAdvisory.URL,
			"forecastCone":   s.ForecastCone.KMZFile,
			"forecastTrack":  s.ForecastTrack.KMZFile,
			"bestTrack":      s.BestTrack.KMZFile,
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "nhc",
			ExtID:  s.ID + ":" + s.BinNumber, // BinNumber bumps each advisory cycle
			Lat:    s.LatitudeNumeric,
			Lon:    s.LongitudeNumeric,
			Props:  props,
		})
	}
	return out, nil
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	}
	return ""
}
