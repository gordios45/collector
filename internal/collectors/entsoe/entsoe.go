// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// ENTSO-E Transparency collector — European power-grid unavailability.
// Gated on ENTSOE_TOKEN (free registration). Polls the "Unavailability
// of Production Units" document type (A80) for the configured bidding
// zone(s); default = the 11 largest EU markets. Each outage → one event
// with a country-centroid position + MW + reason + period.
//
// Document reference:
//
//	https://transparency.entsoe.eu/content/static_content/Static%20content/knowledge%20base/SFTP-Transparency_Docs.html
package entsoe

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/events"
	"github.com/gordios45/collector/internal/geo"
)

// ENTSO-E bidding-zone EIC codes -> ISO-2 country for geo.Centroids.
// Expanded as we hit new zones; the ~largest markets cover most events.
var zones = map[string]string{
	"10YCZ-CEPS-----N": "CZ",
	"10YDE-VE-------2": "DE",
	"10YES-REE------0": "ES",
	"10YFR-RTE------C": "FR",
	"10YGB----------A": "GB",
	"10YIT-GRTN-----B": "IT",
	"10YNL----------L": "NL",
	"10YPL-AREA-----S": "PL",
	"10YRO-TEL------P": "RO",
	"10YSE-1--------K": "SE",
	"10YUA-WEPS-----0": "UA",
}

type Collector struct {
	token  string
	client *http.Client
}

func New() (*Collector, error) {
	tok := strings.TrimSpace(os.Getenv("ENTSOE_TOKEN"))
	if tok == "" {
		return nil, fmt.Errorf("ENTSOE_TOKEN not set")
	}
	return &Collector{token: tok, client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (c *Collector) ID() string               { return "entsoe_outage" }
func (c *Collector) PollEvery() time.Duration { return 1 * time.Hour }

// A80 document schema — only the fields we need.
type a80 struct {
	XMLName    xml.Name `xml:"Unavailability_MarketDocument"`
	TimeSeries []struct {
		MRID                 string `xml:"mRID"`
		BusinessType         string `xml:"businessType"`
		ProductionRegistered struct {
			Name     string `xml:"name"`
			Capacity string `xml:"nominalIP_powerSystemResources.nominalP"`
		} `xml:"production_RegisteredResource"`
		PowerSystemResource struct {
			Name string `xml:"name"`
		} `xml:"powerSystemResource"`
		Period struct {
			TimeInterval struct {
				Start string `xml:"start"`
				End   string `xml:"end"`
			} `xml:"timeInterval"`
			Point []struct {
				Quantity string `xml:"quantity"`
			} `xml:"Point"`
		} `xml:"Available_Period"`
		Reason struct {
			Code string `xml:"code"`
			Text string `xml:"text"`
		} `xml:"Reason"`
	} `xml:"TimeSeries"`
}

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	// Query window: "now" → +7d, but ENTSO-E wants YYYYMMDDHHMM format.
	now := time.Now().UTC()
	start := now.Format("200601021504")
	end := now.Add(7 * 24 * time.Hour).Format("200601021504")

	var all []events.Event
	for zone, cc := range zones {
		u := fmt.Sprintf(
			"https://web-api.tp.entsoe.eu/api?securityToken=%s&documentType=A80&biddingZone_Domain=%s&periodStart=%s&periodEnd=%s",
			c.token, zone, start, end,
		)
		body, err := c.download(ctx, u)
		if err != nil {
			continue // zone-level failure is non-fatal
		}
		var doc a80
		if err := xml.Unmarshal(body, &doc); err != nil {
			continue
		}
		ll, ok := geo.Centroids[cc]
		if !ok {
			continue
		}
		for _, ts := range doc.TimeSeries {
			if ts.MRID == "" {
				continue
			}
			start, _ := time.Parse("2006-01-02T15:04Z", ts.Period.TimeInterval.Start)
			stop, _ := time.Parse("2006-01-02T15:04Z", ts.Period.TimeInterval.End)
			q := "0"
			if len(ts.Period.Point) > 0 {
				q = ts.Period.Point[0].Quantity
			}
			props := map[string]any{
				"country":     cc,
				"zone":        zone,
				"asset":       firstNonEmpty(ts.ProductionRegistered.Name, ts.PowerSystemResource.Name),
				"capacity_mw": ts.ProductionRegistered.Capacity,
				"quantity_mw": q,
				"business":    ts.BusinessType,
				"reason_code": ts.Reason.Code,
				"reason":      ts.Reason.Text,
				"start":       ts.Period.TimeInterval.Start,
				"end":         ts.Period.TimeInterval.End,
			}
			evTs := start
			if evTs.IsZero() {
				evTs = now
			}
			_ = stop // we keep both in props; ev.Ts is "when it starts"
			all = append(all, events.Event{
				Ts:     evTs,
				Source: "entsoe_outage",
				ExtID:  ts.MRID,
				Lat:    ll.Lat, Lon: ll.Lon,
				Props: props,
			})
		}
	}
	return all, nil
}

func (c *Collector) download(ctx context.Context, url string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "gordios/0.1")
	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("entsoe %d", r.StatusCode)
	}
	return io.ReadAll(io.LimitReader(r.Body, 16<<20))
}

func firstNonEmpty(args ...string) string {
	for _, s := range args {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return "—"
}
