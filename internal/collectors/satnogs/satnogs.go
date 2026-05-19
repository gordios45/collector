// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// SatNOGS collector — public satellite observation schedule / RF receptions.
//
// This is not a direct "attack happened" signal. It is supporting evidence for
// collection posture: which satellites were actually observed by public ground
// stations, at what frequency and pass geometry. The classifier gives it low
// weight and uses it mainly to enrich ISR / space-domain hypotheses.
package satnogs

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/httpx"
)

const endpoint = "https://network.satnogs.org/api/observations/"

type Collector struct{}

func New() (*Collector, error) { return &Collector{}, nil }

func (c *Collector) ID() string               { return "satnogs" }
func (c *Collector) PollEvery() time.Duration { return 1 * time.Hour }

type observation struct {
	ID                     int64   `json:"id"`
	Start                  string  `json:"start"`
	End                    string  `json:"end"`
	GroundStation          int64   `json:"ground_station"`
	StationName            string  `json:"station_name"`
	StationLat             float64 `json:"station_lat"`
	StationLng             float64 `json:"station_lng"`
	Status                 string  `json:"status"`
	WaterfallStatus        string  `json:"waterfall_status"`
	VettedStatus           string  `json:"vetted_status"`
	NoradCatID             int64   `json:"norad_cat_id"`
	TLE0                   string  `json:"tle0"`
	TLE1                   string  `json:"tle1"`
	TLE2                   string  `json:"tle2"`
	TLESource              string  `json:"tle_source"`
	MaxAltitude            float64 `json:"max_altitude"`
	RiseAzimuth            float64 `json:"rise_azimuth"`
	SetAzimuth             float64 `json:"set_azimuth"`
	TransmitterDescription string  `json:"transmitter_description"`
	TransmitterMode        string  `json:"transmitter_mode"`
	ObservationFrequency   *int64  `json:"observation_frequency"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	now := time.Now().UTC()
	v := url.Values{}
	v.Set("format", "json")
	v.Set("start", now.Add(-12*time.Hour).Format(time.RFC3339))
	v.Set("end", now.Format(time.RFC3339))
	v.Set("limit", "200")

	var rows []observation
	if err := httpx.GetJSON(ctx, endpoint+"?"+v.Encode(), nil, &rows); err != nil {
		return nil, err
	}

	out := make([]events.Event, 0, len(rows))
	for _, o := range rows {
		if o.ID == 0 || o.StationLat == 0 && o.StationLng == 0 {
			continue
		}
		// Low-elevation scheduled passes are too weak/noisy to be useful as
		// posture evidence.
		if o.MaxAltitude > 0 && o.MaxAltitude < 20 {
			continue
		}
		ts, err := time.Parse(time.RFC3339, o.Start)
		if err != nil {
			ts = now
		}
		props := map[string]any{
			"observation_id":   o.ID,
			"ground_station":   o.GroundStation,
			"station_name":     o.StationName,
			"status":           o.Status,
			"waterfall_status": o.WaterfallStatus,
			"vetted_status":    o.VettedStatus,
			"norad_cat_id":     o.NoradCatID,
			"satellite_name":   o.TLE0,
			"tle_source":       o.TLESource,
			"max_altitude":     o.MaxAltitude,
			"rise_azimuth":     o.RiseAzimuth,
			"set_azimuth":      o.SetAzimuth,
			"transmitter":      o.TransmitterDescription,
			"mode":             o.TransmitterMode,
			"start":            o.Start,
			"end":              o.End,
		}
		if o.ObservationFrequency != nil {
			props["observation_frequency"] = *o.ObservationFrequency
		}
		out = append(out, events.Event{
			Ts:     ts,
			Source: "satnogs",
			ExtID:  strconv.FormatInt(o.ID, 10),
			Lat:    o.StationLat,
			Lon:    o.StationLng,
			Props:  props,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("satnogs returned no usable observations")
	}
	return out, nil
}
