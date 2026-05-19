// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Maritime collector — AISStream.io real-time AIS WebSocket.
//
// AISStream publishes vessel position reports globally. We subscribe to a
// worldwide bounding box and write every PositionReport into the
// `features` table keyed by (source='maritime', ext_id=MMSI).
// That gives the gateway a trivial "latest vessel state" query — one row
// per vessel, hot-upserted as reports stream in.
//
// Gated by AISSTREAM_KEY. Flushes every 2 s to keep row churn under control
// without starving real-time UI (clients re-fetch on refresh).
package maritime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/sources"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

const wsURL = "wss://stream.aisstream.io/v0/stream"

type Collector struct {
	pool   *pgxpool.Pool
	sink   *sources.FeatureSink
	apiKey string
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	key := strings.TrimSpace(os.Getenv("AISSTREAM_KEY"))
	if key == "" {
		return nil, fmt.Errorf("AISSTREAM_KEY not set")
	}
	return &Collector{
		pool:   pool,
		sink:   sources.NewFeatureSink(pool, "maritime", 1000, 2*time.Second),
		apiKey: key,
	}, nil
}

func (c *Collector) ID() string { return "maritime" }

// AIS reports don't carry a "vessel went offline" message — a vessel that
// stops transmitting would linger in `features` forever. Sweep rows with
// no update in 6 h every 10 min. 6 h balances: short-haul vessels at anchor
// may not transmit for hours, but anything older is likely stale.
func (c *Collector) sweepStale(ctx context.Context) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		tag, err := c.pool.Exec(ctx,
			`DELETE FROM features WHERE source='maritime' AND updated_at < now() - interval '6 hours'`)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[maritime] sweep: %v", err)
			continue
		}
		if n := tag.RowsAffected(); n > 0 {
			log.Printf("[maritime] swept %d stale vessels", n)
		}
	}
}

func (c *Collector) Run(ctx context.Context) error {
	go c.sink.Run(ctx)
	go c.sweepStale(ctx)

	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.once(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("[maritime] ws: %v; reconnect in %s", err, backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
	return nil
}

func (c *Collector) once(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(256 * 1024)

	// Worldwide subscription. AISStream's bounding-box format is a list of
	// 2-point boxes where each point is [lat, lon].
	// PositionReport = lat/lon/speed/course plus AISStream metadata for the
	// fields we use in the UI/scoring path. Static reports require direct
	// JSONB patch updates against the hot features table; keep them out of the
	// live subscription and let repeated position reports refresh metadata.
	sub, _ := json.Marshal(map[string]any{
		"APIKey":             c.apiKey,
		"BoundingBoxes":      [][][]float64{{{-85, -180}, {85, 180}}},
		"FilterMessageTypes": []string{"PositionReport"},
	})
	if err := ws.Write(ctx, websocket.MessageText, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Println("[maritime] connected — worldwide AIS subscription")

	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		var frame aisFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			continue
		}
		switch frame.MessageType {
		case "PositionReport":
			c.handlePosition(ctx, &frame)
		case "ShipStaticData":
			c.handleStatic(ctx, &frame)
		}
	}
}

func (c *Collector) handlePosition(ctx context.Context, f *aisFrame) {
	pr := f.Message.PositionReport
	if pr.Latitude == nil || pr.Longitude == nil {
		return
	}
	mmsi := mmsiFrom(f.MetaData.MMSI, pr.UserID)
	if mmsi == 0 {
		return
	}
	props := map[string]any{
		"mmsi": mmsi,
		"name": strings.TrimSpace(f.MetaData.ShipName),
		"dest": strings.TrimSpace(f.MetaData.Destination),
	}
	if f.MetaData.VesselType != nil {
		props["type"] = *f.MetaData.VesselType
	}
	if pr.Sog != nil {
		props["sog"] = *pr.Sog
	}
	if pr.Cog != nil {
		props["cog"] = *pr.Cog
	}
	if pr.TrueHeading != nil {
		props["heading"] = *pr.TrueHeading
	}
	if pr.NavigationalStatus != nil {
		props["nav_status"] = *pr.NavigationalStatus
	}
	if f.MetaData.TimeUTC != "" {
		props["ts"] = f.MetaData.TimeUTC
	}
	c.sink.Push(ctx, features.Feature{
		ExtID:   fmt.Sprintf("%d", mmsi),
		GeomWKT: fmt.Sprintf("POINT(%f %f)", *pr.Longitude, *pr.Latitude),
		Props:   props,
	})
}

// handleStatic patches the existing vessel row with name/type/destination
// without moving it. Done via a direct SQL JSON-merge so we don't have to
// know the current position — if the row doesn't exist yet, we skip
// (a PositionReport will arrive eventually and create it).
func (c *Collector) handleStatic(ctx context.Context, f *aisFrame) {
	// Static reports are intentionally ignored in the hot path. They used to
	// patch the latest feature row directly, but global AIS volume can make
	// those JSONB updates contend with signal generation and collector flushes.
	// Position reports already carry enough metadata for current UI/scoring.
}

type aisFrame struct {
	MessageType string `json:"MessageType"`
	Message     struct {
		PositionReport struct {
			UserID             *int64   `json:"UserID"`
			Latitude           *float64 `json:"Latitude"`
			Longitude          *float64 `json:"Longitude"`
			Sog                *float64 `json:"Sog"`
			Cog                *float64 `json:"Cog"`
			TrueHeading        *float64 `json:"TrueHeading"`
			NavigationalStatus *int     `json:"NavigationalStatus"`
		} `json:"PositionReport"`
		ShipStaticData struct {
			UserID               *int64   `json:"UserID"`
			Name                 string   `json:"Name"`
			CallSign             string   `json:"CallSign"`
			Destination          string   `json:"Destination"`
			Type                 *int     `json:"Type"`
			MaximumStaticDraught *float64 `json:"MaximumStaticDraught"`
			Dimension            struct {
				A *int `json:"A"`
				B *int `json:"B"`
				C *int `json:"C"`
				D *int `json:"D"`
			} `json:"Dimension"`
		} `json:"ShipStaticData"`
	} `json:"Message"`
	MetaData struct {
		MMSI        *int64  `json:"MMSI"`
		ShipName    string  `json:"ShipName"`
		VesselType  *int    `json:"VesselType"`
		Destination string  `json:"Destination"`
		TimeUTC     string  `json:"time_utc"`
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
	} `json:"MetaData"`
}

func mmsiFrom(meta, msg *int64) int64 {
	if meta != nil {
		return *meta
	}
	if msg != nil {
		return *msg
	}
	return 0
}
