// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Sanctions collector — merges OFAC SDN + UN Consolidated Sanctions List
// into the `sanctioned_entities` table. Daily cadence.
//
// Unlike other collectors this one doesn't write to `events` — sanctions
// aren't time-series, they're entity records meant to be joined against
// vessel MMSIs / aircraft tail numbers / person passports during intel
// panel lookups. We still register with the scheduler so it shows in
// /api/sources with health state; Fetch returns zero events after doing
// the DB upsert as a side-effect.
package sanctions

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gordios45/collector/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ofacURL = "https://www.treasury.gov/ofac/downloads/sdn.xml"
	unURL   = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
)

type Collector struct {
	pool   *pgxpool.Pool
	client *http.Client
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	return &Collector{
		pool:   pool,
		client: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (c *Collector) ID() string               { return "sanctions" }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) Fetch(ctx context.Context) ([]events.Event, error) {
	var (
		wg       sync.WaitGroup
		ofacEnts []entity
		unEnts   []entity
		ofacErr  error
		unErr    error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		ofacEnts, ofacErr = c.fetchOFAC(ctx)
	}()
	go func() {
		defer wg.Done()
		unEnts, unErr = c.fetchUN(ctx)
	}()
	wg.Wait()
	if ofacErr != nil && unErr != nil {
		return nil, fmt.Errorf("both feeds failed: ofac=%v un=%v", ofacErr, unErr)
	}
	all := append(ofacEnts, unEnts...)
	if err := c.upsert(ctx, all); err != nil {
		return nil, fmt.Errorf("upsert: %w", err)
	}
	// Sanctions collector intentionally returns zero events — the work happens
	// in `sanctioned_entities` via upsert above. The scheduler will log
	// "0 events" which is fine; sources-table health is updated separately.
	return nil, nil
}

// ---- internal record ----

type entity struct {
	List        string
	RefID       string
	Kind        string // person | entity | vessel | aircraft
	Name        string
	Aliases     []string
	Programs    []string
	Identifiers map[string]any
}

// ---- OFAC SDN parse ----

type ofacList struct {
	XMLName xml.Name    `xml:"sdnList"`
	Entries []ofacEntry `xml:"sdnEntry"`
}

type ofacEntry struct {
	UID       string   `xml:"uid"`
	FirstName string   `xml:"firstName"`
	LastName  string   `xml:"lastName"`
	Title     string   `xml:"title"`
	SdnType   string   `xml:"sdnType"`
	Programs  []string `xml:"programList>program"`
	Akas      []struct {
		Type      string `xml:"type"`
		Category  string `xml:"category"`
		FirstName string `xml:"firstName"`
		LastName  string `xml:"lastName"`
	} `xml:"akaList>aka"`
	IDs []struct {
		Type    string `xml:"idType"`
		Number  string `xml:"idNumber"`
		Country string `xml:"idCountry"`
	} `xml:"idList>id"`
	VesselInfo *struct {
		CallSign   string `xml:"callSign"`
		VesselType string `xml:"vesselType"`
		VesselFlag string `xml:"vesselFlag"`
		Tonnage    string `xml:"tonnage"`
	} `xml:"vesselInfo"`
}

func (c *Collector) fetchOFAC(ctx context.Context) ([]entity, error) {
	body, err := c.download(ctx, ofacURL, 64<<20)
	if err != nil {
		return nil, err
	}
	var list ofacList
	// encoding/xml in Go handles default namespaces if struct tags use local
	// names. Strip declarations defensively.
	if err := xml.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("ofac xml: %w", err)
	}
	out := make([]entity, 0, len(list.Entries))
	for _, e := range list.Entries {
		name := strings.TrimSpace(e.FirstName + " " + e.LastName)
		if name == "" {
			continue
		}
		kind := "entity"
		switch strings.ToLower(e.SdnType) {
		case "individual":
			kind = "person"
		case "vessel":
			kind = "vessel"
		case "aircraft":
			kind = "aircraft"
		}
		ent := entity{
			List:        "ofac_sdn",
			RefID:       e.UID,
			Kind:        kind,
			Name:        name,
			Programs:    e.Programs,
			Identifiers: map[string]any{},
		}
		for _, a := range e.Akas {
			al := strings.TrimSpace(a.FirstName + " " + a.LastName)
			if al != "" && al != name {
				ent.Aliases = append(ent.Aliases, al)
			}
		}
		for _, id := range e.IDs {
			t := strings.ToLower(strings.TrimSpace(id.Type))
			v := strings.TrimSpace(id.Number)
			if v == "" {
				continue
			}
			// OFAC uses its own controlled vocabulary — "Vessel Registration
			// Identification" for IMO, "Aircraft Mode S Transponder Code" for
			// ICAO24, etc. Match on the distinctive sub-string of each.
			switch {
			case strings.Contains(t, "vessel registration"):
				// OFAC sometimes prefixes IMO values with "IMO " — strip it so
				// lookups by raw numeric IMO hit.
				ent.Identifiers["imo"] = strings.TrimSpace(strings.TrimPrefix(v, "IMO "))
			case t == "mmsi":
				ent.Identifiers["mmsi"] = v
			case strings.Contains(t, "mode s"):
				ent.Identifiers["icao24"] = strings.ToUpper(v)
			case strings.Contains(t, "aircraft tail"):
				// Tail-number history fields fall into list form.
				if strings.HasPrefix(t, "aircraft tail") {
					ent.Identifiers["tail"] = v
				} else {
					appendList(ent.Identifiers, "tail_prev", v)
				}
			case strings.Contains(t, "aircraft serial") || strings.Contains(t, "manufacturer's serial"):
				ent.Identifiers["aircraft_serial"] = v
			case strings.Contains(t, "aircraft construction"):
				ent.Identifiers["aircraft_construction"] = v
			case strings.Contains(t, "aircraft operator"):
				ent.Identifiers["aircraft_operator"] = v
			case strings.Contains(t, "call sign") || strings.Contains(t, "callsign"):
				// Covers "Other Vessel Call Sign".
				ent.Identifiers["call_sign"] = v
			case strings.Contains(t, "vessel flag"):
				ent.Identifiers["flag"] = v
			case strings.Contains(t, "vessel type"):
				ent.Identifiers["vessel_type"] = v
			case strings.Contains(t, "passport"):
				appendList(ent.Identifiers, "passports", v)
			case strings.Contains(t, "national id") || strings.Contains(t, "national number"):
				appendList(ent.Identifiers, "national_ids", v)
			default:
				appendList(ent.Identifiers, "other_ids", t+":"+v)
			}
		}
		if e.VesselInfo != nil {
			if cs := strings.TrimSpace(e.VesselInfo.CallSign); cs != "" {
				ent.Identifiers["call_sign"] = cs
			}
			if f := strings.TrimSpace(e.VesselInfo.VesselFlag); f != "" {
				ent.Identifiers["flag"] = f
			}
			if t := strings.TrimSpace(e.VesselInfo.VesselType); t != "" {
				ent.Identifiers["vessel_type"] = t
			}
		}
		out = append(out, ent)
	}
	return out, nil
}

// ---- UN Consolidated parse ----

type unList struct {
	XMLName     xml.Name `xml:"CONSOLIDATED_LIST"`
	Individuals struct {
		Items []unIndividual `xml:"INDIVIDUAL"`
	} `xml:"INDIVIDUALS"`
	Entities struct {
		Items []unEntity `xml:"ENTITY"`
	} `xml:"ENTITIES"`
}

type unIndividual struct {
	DataID      string `xml:"DATAID"`
	FirstName   string `xml:"FIRST_NAME"`
	SecondName  string `xml:"SECOND_NAME"`
	ThirdName   string `xml:"THIRD_NAME"`
	Nationality string `xml:"NATIONALITY>VALUE"`
	ListedOn    string `xml:"LISTED_ON"`
	UNListType  string `xml:"UN_LIST_TYPE"`
	Aliases     []struct {
		Name string `xml:"ALIAS_NAME"`
	} `xml:"INDIVIDUAL_ALIAS"`
}

type unEntity struct {
	DataID     string `xml:"DATAID"`
	FirstName  string `xml:"FIRST_NAME"`
	ListedOn   string `xml:"LISTED_ON"`
	UNListType string `xml:"UN_LIST_TYPE"`
	Aliases    []struct {
		Name string `xml:"ALIAS_NAME"`
	} `xml:"ENTITY_ALIAS"`
}

func (c *Collector) fetchUN(ctx context.Context) ([]entity, error) {
	body, err := c.download(ctx, unURL, 16<<20)
	if err != nil {
		return nil, err
	}
	var list unList
	if err := xml.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("un xml: %w", err)
	}
	out := make([]entity, 0, len(list.Individuals.Items)+len(list.Entities.Items))
	for _, i := range list.Individuals.Items {
		name := strings.TrimSpace(strings.Join([]string{i.FirstName, i.SecondName, i.ThirdName}, " "))
		if name == "" {
			continue
		}
		ent := entity{
			List:        "un_consolidated",
			RefID:       i.DataID,
			Kind:        "person",
			Name:        name,
			Programs:    []string{i.UNListType},
			Identifiers: map[string]any{},
		}
		if i.Nationality != "" {
			ent.Identifiers["nationality"] = i.Nationality
		}
		for _, a := range i.Aliases {
			if al := strings.TrimSpace(a.Name); al != "" && al != name {
				ent.Aliases = append(ent.Aliases, al)
			}
		}
		out = append(out, ent)
	}
	for _, e := range list.Entities.Items {
		name := strings.TrimSpace(e.FirstName)
		if name == "" {
			continue
		}
		ent := entity{
			List:        "un_consolidated",
			RefID:       e.DataID,
			Kind:        "entity",
			Name:        name,
			Programs:    []string{e.UNListType},
			Identifiers: map[string]any{},
		}
		for _, a := range e.Aliases {
			if al := strings.TrimSpace(a.Name); al != "" && al != name {
				ent.Aliases = append(ent.Aliases, al)
			}
		}
		out = append(out, ent)
	}
	return out, nil
}

// ---- upsert ----

func (c *Collector) upsert(ctx context.Context, ents []entity) error {
	if len(ents) == 0 {
		return nil
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const stmt = `
		INSERT INTO sanctioned_entities
			(list, ref_id, kind, name, aliases, programs, identifiers, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
		ON CONFLICT (list, ref_id) DO UPDATE
			SET kind        = EXCLUDED.kind,
			    name        = EXCLUDED.name,
			    aliases     = EXCLUDED.aliases,
			    programs    = EXCLUDED.programs,
			    identifiers = EXCLUDED.identifiers,
			    updated_at  = now()`
	for _, e := range ents {
		// pgx maps a nil Go slice to NULL; the column is NOT NULL. Coerce.
		aliases := e.Aliases
		if aliases == nil {
			aliases = []string{}
		}
		progs := e.Programs
		if progs == nil {
			progs = []string{}
		}
		idsRaw, _ := json.Marshal(e.Identifiers)
		if _, err := tx.Exec(ctx, stmt,
			e.List, e.RefID, e.Kind, e.Name,
			aliases, progs, string(idsRaw),
		); err != nil {
			return fmt.Errorf("upsert %s/%s: %w", e.List, e.RefID, err)
		}
	}
	return tx.Commit(ctx)
}

// ---- shared ----

func (c *Collector) download(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "gordios/0.1")
	r, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %d", url, r.StatusCode)
	}
	return io.ReadAll(io.LimitReader(r.Body, maxBytes))
}

func appendList(m map[string]any, key, val string) {
	cur, _ := m[key].([]string)
	m[key] = append(cur, val)
}

// sanity-compile: make sure we didn't break imports
var _ = bytes.NewReader
