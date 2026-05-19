// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Gateway REST surface. All responses are JSON.
//
//	GET /api/latest?source=X&limit=N   → one row per ext_id for source,
//	                                     newest first, with ts+lat+lon+props.
//	                                     Defaults to geospatial rows only;
//	                                     list-only panels can pass list_only=1
//	                                     to skip spatial centroid work.
//	GET /api/sources                   → list of registered sources with health.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RestHandler struct{ Pool *pgxpool.Pool }

const (
	latestDefaultLimit     = 10000
	latestMaxLimit         = 20000
	latestSampleMin        = 1000
	latestSampleMultiplier = 3
	latestSampleCap        = 20000
)

func boundedLatestSampleLimit(limit int) int {
	if limit >= latestDefaultLimit {
		return limit
	}
	n := limit * latestSampleMultiplier
	if n < latestSampleMin {
		n = latestSampleMin
	}
	if n > latestSampleCap {
		n = latestSampleCap
	}
	return n
}

func eventRef(source, extID string, ts time.Time) string {
	if extID != "" {
		return source + ":" + extID
	}
	return source + ":" + ts.UTC().Format(time.RFC3339Nano)
}

func h3R4Param(r *http.Request) string {
	h3 := strings.TrimSpace(r.URL.Query().Get("h3_r4"))
	if h3 == "" {
		h3 = strings.TrimSpace(r.URL.Query().Get("h3"))
	}
	return h3
}

func latestListOnlySource(source string) bool {
	switch source {
	case "bluesky", "cisa_kev", "ghsl_population", "ghsl_smod", "hrsl_population":
		return true
	default:
		return false
	}
}

func (h *RestHandler) Register(mux *http.ServeMux) {
	h.RegisterRaw(mux)
}

func (h *RestHandler) RegisterRaw(mux *http.ServeMux) {
	mux.HandleFunc("/api/sources", h.sources)
	mux.HandleFunc("/api/sources/status", h.sourceStatus)
	mux.HandleFunc("/api/source-status", h.sourceStatus)
	mux.HandleFunc("/api/latest", h.latest)
	mux.HandleFunc("/api/features", h.featuresEndpoint)
	mux.HandleFunc("/api/events/geojson", h.eventsGeoJSON)
	mux.HandleFunc("/api/bgp/snapshot", h.bgpSnapshot)
	mux.HandleFunc("/api/cdse/auth/status", h.cdseAuthStatus)
	mux.HandleFunc("/api/sanctions/lookup", h.sanctionsLookup)
	mux.HandleFunc("/api/carriers", h.carriersList)
	mux.HandleFunc("/api/carriers/lookup", h.carrierLookup)
	mux.HandleFunc("/api/military/lookup", h.militaryLookup)
	mux.HandleFunc("/api/aircraft/lookup", h.aircraftLookup)
	h.RegisterIngestionAOIs(mux)
}

// /api/aircraft/lookup?icao24=<hex>  OR  ?reg=<tail>
//
// Single-row fetch from the OpenSky-sourced aircraft_registry. Gives the
// intel panel type/model/operator/owner/country-of-registration without
// the panel needing to combine multiple API calls.
func (h *RestHandler) aircraftLookup(w http.ResponseWriter, r *http.Request) {
	hex := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("icao24"))), "0x")
	reg := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("reg")))
	if hex == "" && reg == "" {
		writeErr(w, http.StatusBadRequest, "icao24 or reg required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var (
		row pgx.Row
	)
	if hex != "" {
		row = h.Pool.QueryRow(ctx, `
			SELECT icao24, registration, manufacturer_name, model, typecode,
			       serial_number, icao_aircraft_type, operator, operator_callsign,
			       operator_icao, operator_iata, owner, built, status, notes,
			       imported_at
			  FROM aircraft_registry
			 WHERE icao24 = $1`, hex)
	} else {
		row = h.Pool.QueryRow(ctx, `
			SELECT icao24, registration, manufacturer_name, model, typecode,
			       serial_number, icao_aircraft_type, operator, operator_callsign,
			       operator_icao, operator_iata, owner, built, status, notes,
			       imported_at
			  FROM aircraft_registry
			 WHERE registration = $1`, reg)
	}

	type aircraft struct {
		ICAO24           string    `json:"icao24"`
		Registration     *string   `json:"registration,omitempty"`
		ManufacturerName *string   `json:"manufacturer,omitempty"`
		Model            *string   `json:"model,omitempty"`
		TypeCode         *string   `json:"typecode,omitempty"`
		SerialNumber     *string   `json:"serial,omitempty"`
		ICAOAircraftType *string   `json:"icao_type,omitempty"`
		Operator         *string   `json:"operator,omitempty"`
		OperatorCallsign *string   `json:"operator_callsign,omitempty"`
		OperatorICAO     *string   `json:"operator_icao,omitempty"`
		OperatorIATA     *string   `json:"operator_iata,omitempty"`
		Owner            *string   `json:"owner,omitempty"`
		Built            *string   `json:"built,omitempty"`
		Status           *string   `json:"status,omitempty"`
		Notes            *string   `json:"notes,omitempty"`
		ImportedAt       time.Time `json:"imported_at"`
	}
	var a aircraft
	err := row.Scan(&a.ICAO24, &a.Registration, &a.ManufacturerName, &a.Model, &a.TypeCode,
		&a.SerialNumber, &a.ICAOAircraftType, &a.Operator, &a.OperatorCallsign,
		&a.OperatorICAO, &a.OperatorIATA, &a.Owner, &a.Built, &a.Status, &a.Notes,
		&a.ImportedAt)
	if err != nil {
		// Not found → return a shape the client can recognise without erroring.
		writeJSON(w, map[string]any{"found": false})
		return
	}
	writeJSON(w, map[string]any{"found": true, "aircraft": a})
}

// /api/military/lookup?callsign=<cs>&icao24=<hex>
//
// Hand-side: either field is optional — the handler returns whichever
// dimensions it can resolve. Callsign gives operator/role; ICAO24 hex
// gives country of registration. The intel panel calls this once per
// military click and renders both on the same banner.
func (h *RestHandler) militaryLookup(w http.ResponseWriter, r *http.Request) {
	cs := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("callsign")))
	hex := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("icao24"))), "0x")
	if cs == "" && hex == "" {
		writeErr(w, http.StatusBadRequest, "callsign or icao24 required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	out := map[string]any{"callsign": cs, "icao24": hex}

	// Callsign prefix: match up to the first digit. "RCH157" → "RCH".
	if cs != "" {
		prefix := cs
		for i, c := range cs {
			if c >= '0' && c <= '9' {
				prefix = cs[:i]
				break
			}
		}
		if prefix != "" {
			var (
				operator, country, role, notes *string
				aircraft                       []string
			)
			err := h.Pool.QueryRow(ctx, `
				SELECT operator, country_code, role, aircraft_types, notes
				  FROM military_callsigns WHERE prefix = $1`, prefix,
			).Scan(&operator, &country, &role, &aircraft, &notes)
			if err == nil {
				out["callsign_prefix"] = prefix
				out["operator"] = operator
				out["operator_country"] = country
				out["role"] = role
				out["aircraft_types"] = aircraft
				out["notes"] = notes
			} else {
				out["callsign_prefix"] = prefix
			}
		}
	}

	// ICAO24 hex → country of registration.
	if hex != "" {
		var v int64
		if _, err := fmt.Sscanf(hex, "%x", &v); err == nil && v > 0 {
			var code, name *string
			err := h.Pool.QueryRow(ctx, `
				SELECT country_code, country_name
				  FROM icao24_country_ranges
				 WHERE $1 BETWEEN range_start AND range_end
				 LIMIT 1`, v,
			).Scan(&code, &name)
			if err == nil {
				out["registration_country_code"] = code
				out["registration_country"] = name
			}
		}
	}
	writeJSON(w, out)
}

// /api/carriers?country=X&rating=X&region=X&q=search&limit=N
// Lists carrier advisories with filters. Powers the CARRIER ADVISORIES panel.
func (h *RestHandler) carriersList(w http.ResponseWriter, r *http.Request) {
	country := strings.TrimSpace(r.URL.Query().Get("country"))
	rating := strings.TrimSpace(r.URL.Query().Get("rating"))
	region := strings.TrimSpace(strings.ToUpper(r.URL.Query().Get("region")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	limit := 1000
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Build SQL + args defensively — every filter parameter is bound.
	args := []any{}
	conds := []string{"TRUE"}
	if country != "" {
		args = append(args, country)
		conds = append(conds, fmt.Sprintf("country ILIKE $%d", len(args)))
	}
	if rating != "" {
		args = append(args, rating)
		conds = append(conds, fmt.Sprintf("rating = $%d", len(args)))
	}
	if region != "" {
		args = append(args, region)
		conds = append(conds, fmt.Sprintf("region = $%d", len(args)))
	}
	if q != "" {
		args = append(args, q+"%", q)
		conds = append(conds, fmt.Sprintf("(name ILIKE $%d OR to_tsvector('simple', coalesce(name,'') || ' ' || coalesce(summary,'')) @@ plainto_tsquery('simple', $%d))", len(args)-1, len(args)))
	}
	args = append(args, limit)
	sql := fmt.Sprintf(`
		SELECT id, name, iata, icao, country, rating, operational_status,
		       summary, region, last_updated_at, content
		  FROM carrier_advisories
		 WHERE %s
		 ORDER BY
		     CASE rating
		         WHEN 'AVOID'                THEN 0
		         WHEN 'NOT_PREFERRED'        THEN 1
		         WHEN 'MODERATELY_PREFERRED' THEN 2
		         WHEN 'HIGHLY_PREFERRED'     THEN 3
		         ELSE 4
		     END,
		     country, name
		 LIMIT $%d`, strings.Join(conds, " AND "), len(args))
	rows, err := h.Pool.Query(ctx, sql, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type carrier struct {
		ID                string         `json:"id"`
		Name              string         `json:"name"`
		IATA              *string        `json:"iata,omitempty"`
		ICAO              *string        `json:"icao,omitempty"`
		Country           *string        `json:"country,omitempty"`
		Rating            *string        `json:"rating,omitempty"`
		OperationalStatus *string        `json:"operational_status,omitempty"`
		Summary           *string        `json:"summary,omitempty"`
		Region            *string        `json:"region,omitempty"`
		LastUpdatedAt     *time.Time     `json:"last_updated_at,omitempty"`
		Content           map[string]any `json:"content,omitempty"`
	}
	out := make([]carrier, 0, 128)
	for rows.Next() {
		var c carrier
		var raw []byte
		if err := rows.Scan(&c.ID, &c.Name, &c.IATA, &c.ICAO, &c.Country, &c.Rating,
			&c.OperationalStatus, &c.Summary, &c.Region, &c.LastUpdatedAt, &raw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &c.Content)
		}
		out = append(out, c)
	}
	writeJSON(w, map[string]any{"count": len(out), "carriers": out})
}

// /api/carriers/lookup?iata=XX OR ?icao=YYY
// Returns all matching advisories (there can be >1 — same IATA reassigned).
func (h *RestHandler) carrierLookup(w http.ResponseWriter, r *http.Request) {
	iata := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("iata")))
	icao := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("icao")))
	if iata == "" && icao == "" {
		writeErr(w, http.StatusBadRequest, "iata or icao required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var rows pgx.Rows
	var err error
	switch {
	case icao != "":
		rows, err = h.Pool.Query(ctx, `
			SELECT id, name, iata, icao, country, rating, operational_status,
			       summary, description_en, region, last_updated_at, content
			  FROM carrier_advisories WHERE icao = $1`, icao)
	default:
		rows, err = h.Pool.Query(ctx, `
			SELECT id, name, iata, icao, country, rating, operational_status,
			       summary, description_en, region, last_updated_at, content
			  FROM carrier_advisories WHERE iata = $1`, iata)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type match struct {
		ID            string         `json:"id"`
		Name          string         `json:"name"`
		IATA          *string        `json:"iata,omitempty"`
		ICAO          *string        `json:"icao,omitempty"`
		Country       *string        `json:"country,omitempty"`
		Rating        *string        `json:"rating,omitempty"`
		OpStatus      *string        `json:"operational_status,omitempty"`
		Summary       *string        `json:"summary,omitempty"`
		DescriptionEN *string        `json:"description_en,omitempty"`
		Region        *string        `json:"region,omitempty"`
		LastUpdatedAt *time.Time     `json:"last_updated_at,omitempty"`
		Content       map[string]any `json:"content,omitempty"`
	}
	out := make([]match, 0, 2)
	for rows.Next() {
		var m match
		var raw []byte
		if err := rows.Scan(&m.ID, &m.Name, &m.IATA, &m.ICAO, &m.Country, &m.Rating,
			&m.OpStatus, &m.Summary, &m.DescriptionEN, &m.Region, &m.LastUpdatedAt, &raw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &m.Content)
		}
		out = append(out, m)
	}
	writeJSON(w, map[string]any{"count": len(out), "matches": out})
}

// /api/events/geojson?source=<id>&max_age_min=<N>&limit=<L>
//
// Returns the newest-per-ext_id events for a source as a GeoJSON
// FeatureCollection with the full geom preserved (polygons stay polygons).
// Powers layers that need real shapes — SIGMETs, NGA warnings, etc. —
// without forcing them to hit the raw-SQL path.
func (h *RestHandler) eventsGeoJSON(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		writeErr(w, http.StatusBadRequest, "source required")
		return
	}
	maxAgeMin := 60
	if s := r.URL.Query().Get("max_age_min"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxAgeMin = n
		}
	}
	limit := 500
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < 10000 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	h3R4 := h3R4Param(r)
	h3Clause := ""
	args := []any{source, maxAgeMin, boundedLatestSampleLimit(limit)}
	if h3R4 != "" {
		h3Clause = "AND h3_r4 = $4"
		args = append(args, h3R4)
	}
	rows, err := h.Pool.Query(ctx, `
		WITH recent AS MATERIALIZED (
		  SELECT ext_id, ts, geom, h3_r4, props
		    FROM events
		   WHERE source = $1
		     AND geom IS NOT NULL
		     `+h3Clause+`
		     AND ts BETWEEN now() - make_interval(mins => $2)
		                    AND now() + make_interval(mins => $2)
		   ORDER BY ts DESC
		   LIMIT $3
		)
		SELECT ext_id, ts, h3_r4,
		       ST_AsGeoJSON(geom::geometry)::jsonb AS geometry,
		       props
		  FROM recent`, args...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type feature struct {
		Type       string          `json:"type"`
		ID         string          `json:"id"`
		Geometry   json.RawMessage `json:"geometry"`
		Properties map[string]any  `json:"properties"`
	}
	out := struct {
		Type     string    `json:"type"`
		Source   string    `json:"source"`
		Features []feature `json:"features"`
	}{Type: "FeatureCollection", Source: source}

	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		var f feature
		var ts time.Time
		var h3Cell *string
		var geomRaw []byte
		var propsRaw []byte
		if err := rows.Scan(&f.ID, &ts, &h3Cell, &geomRaw, &propsRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		key := f.ID
		if key == "" {
			key = ts.UTC().Format(time.RFC3339Nano)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		f.Type = "Feature"
		f.Geometry = json.RawMessage(geomRaw)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &f.Properties)
		}
		if f.Properties == nil {
			f.Properties = map[string]any{}
		}
		f.Properties["_event_ref"] = eventRef(source, f.ID, ts)
		f.Properties["_source"] = source
		f.Properties["_ext_id"] = f.ID
		f.Properties["_ts"] = ts.UTC().Format(time.RFC3339)
		if h3Cell != nil {
			f.Properties["_h3_r4"] = *h3Cell
		}
		out.Features = append(out.Features, f)
		if len(out.Features) >= limit {
			rows.Close()
			break
		}
	}
	writeJSON(w, out)
}

// /api/bgp/snapshot  →  latest routed/registered + 24 h baseline per country.
//
// Replaces the client-side baseline computation. Single DB round-trip does:
//
//	latest     — DISTINCT ON (ext_id) the newest event per country
//	baselines  — AVG(routed_ratio) over the past 24 h per country
//
// LEFT JOIN so a country with <24 h of history still surfaces.
func (h *RestHandler) bgpSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := h.Pool.Query(ctx, `
		WITH recent AS MATERIALIZED (
			SELECT ext_id, ts, props, geom
			  FROM events
			 WHERE source = 'bgp_visibility'
			   AND ts > now() - interval '7 days'
			 ORDER BY ts DESC
			 LIMIT 5000
		), latest AS (
			SELECT DISTINCT ON (ext_id)
			       ext_id, ts, props,
			       ST_Y(ST_Centroid(geom::geometry)) AS lat,
			       ST_X(ST_Centroid(geom::geometry)) AS lon
			  FROM recent
			 ORDER BY ext_id, ts DESC
		), baselines AS (
			SELECT ext_id,
			       AVG((props->>'routed_ratio')::float) AS baseline,
			       COUNT(*)                             AS samples
			  FROM events
			 WHERE source = 'bgp_visibility'
			   AND ts > now() - interval '24 hours'
			 GROUP BY ext_id
		)
		SELECT l.ext_id, l.ts, l.lat, l.lon, l.props,
		       b.baseline, b.samples
		  FROM latest l LEFT JOIN baselines b USING (ext_id)
		 ORDER BY l.ext_id
	`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type row struct {
		Country  string         `json:"country"`
		Ts       time.Time      `json:"ts"`
		Lat      float64        `json:"lat"`
		Lon      float64        `json:"lon"`
		Ratio    float64        `json:"ratio"`
		Baseline *float64       `json:"baseline_24h,omitempty"`
		Samples  int            `json:"samples_24h"`
		DeltaPP  *float64       `json:"delta_pp,omitempty"`
		Props    map[string]any `json:"props"`
	}
	out := make([]row, 0, 16)
	for rows.Next() {
		var r row
		var lat, lon *float64
		var propsRaw []byte
		if err := rows.Scan(&r.Country, &r.Ts, &lat, &lon, &propsRaw, &r.Baseline, &r.Samples); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if lat != nil {
			r.Lat = *lat
		}
		if lon != nil {
			r.Lon = *lon
		}
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &r.Props)
		}
		if r.Props != nil {
			if v, ok := r.Props["routed_ratio"].(float64); ok {
				r.Ratio = v
			}
		}
		if r.Baseline != nil {
			d := (r.Ratio - *r.Baseline) * 100.0
			r.DeltaPP = &d
		}
		out = append(out, r)
	}
	writeJSON(w, map[string]any{"count": len(out), "countries": out})
}

// /api/sanctions/lookup?kind=<mmsi|imo|icao24|tail|call_sign>&value=<id>
//
// Looks up a sanctioned entity by structural identifier. Powered by the
// GIN index on sanctioned_entities.identifiers; O(log n) even across the
// combined OFAC+UN corpus (~20 k rows).
func (h *RestHandler) sanctionsLookup(w http.ResponseWriter, r *http.Request) {
	kind := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("kind")))
	value := strings.TrimSpace(r.URL.Query().Get("value"))
	if kind == "" || value == "" {
		writeErr(w, http.StatusBadRequest, "kind and value required")
		return
	}
	// Callers tend to pass case inconsistently for hex ident codes.
	if kind == "icao24" || kind == "tail" {
		value = strings.ToUpper(value)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// jsonb @> constrains to rows whose identifiers object contains the
	// given key/value pair — matches both scalar ("imo":"9423578") and
	// array-valued ("passports":["X12345"]) cases via jsonb containment.
	rows, err := h.Pool.Query(ctx, `
		SELECT list, ref_id, kind, name, aliases, programs, identifiers, updated_at
		  FROM sanctioned_entities
		 WHERE identifiers @> jsonb_build_object($1::text, $2::text)::jsonb
		    OR identifiers @> jsonb_build_object($1::text, jsonb_build_array($2::text))::jsonb
		 LIMIT 20`, kind, value)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type match struct {
		List        string         `json:"list"`
		RefID       string         `json:"ref_id"`
		Kind        string         `json:"kind"`
		Name        string         `json:"name"`
		Aliases     []string       `json:"aliases"`
		Programs    []string       `json:"programs"`
		Identifiers map[string]any `json:"identifiers"`
		UpdatedAt   time.Time      `json:"updated_at"`
	}
	out := make([]match, 0, 4)
	for rows.Next() {
		var m match
		var ids []byte
		if err := rows.Scan(&m.List, &m.RefID, &m.Kind, &m.Name,
			&m.Aliases, &m.Programs, &ids, &m.UpdatedAt); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(ids) > 0 {
			_ = json.Unmarshal(ids, &m.Identifiers)
		}
		out = append(out, m)
	}
	writeJSON(w, map[string]any{
		"kind":    kind,
		"value":   value,
		"matches": out,
		"count":   len(out),
	})
}

// /api/features?source=chokepoints → GeoJSON FeatureCollection built from
// the `features` table. The response is shaped as GeoJSON so frontend layers
// can render feature rows through the same path.
func (h *RestHandler) featuresEndpoint(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		writeErr(w, http.StatusBadRequest, "source required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	// Optional bbox filter: ?bbox=west,south,east,north. Cuts a 7 MB
	// global AIS payload down to a few hundred KB per visible frame.
	// Silently ignored if not parseable — static-feature queries (e.g.
	// chokepoints) rarely need bbox and shouldn't fail on a bad one.
	west, south, east, north, hasBBox := parseBBox(r.URL.Query().Get("bbox"))
	h3R4 := h3R4Param(r)

	var (
		rows pgx.Rows
		err  error
	)
	switch {
	case hasBBox && h3R4 != "":
		rows, err = h.Pool.Query(ctx, `
			SELECT ext_id,
			       h3_r4,
			       ST_AsGeoJSON(geom::geometry)::jsonb AS geometry,
			       props
			  FROM features
			 WHERE source = $1
			   AND h3_r4 = $2
			   AND ST_Intersects(geom, ST_MakeEnvelope($3, $4, $5, $6, 4326)::geography)`,
			source, h3R4, west, south, east, north)
	case hasBBox:
		rows, err = h.Pool.Query(ctx, `
			SELECT ext_id,
			       h3_r4,
			       ST_AsGeoJSON(geom::geometry)::jsonb AS geometry,
			       props
			  FROM features
			 WHERE source = $1
			   AND ST_Intersects(geom, ST_MakeEnvelope($2, $3, $4, $5, 4326)::geography)`,
			source, west, south, east, north)
	case h3R4 != "":
		rows, err = h.Pool.Query(ctx, `
			SELECT ext_id,
			       h3_r4,
			       ST_AsGeoJSON(geom::geometry)::jsonb AS geometry,
			       props
			  FROM features
			 WHERE source = $1
			   AND h3_r4 = $2`, source, h3R4)
	default:
		rows, err = h.Pool.Query(ctx, `
			SELECT ext_id,
			       h3_r4,
			       ST_AsGeoJSON(geom::geometry)::jsonb AS geometry,
			       props
			  FROM features
			 WHERE source = $1`, source)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	type feature struct {
		Type       string          `json:"type"`
		ID         string          `json:"id,omitempty"`
		Geometry   json.RawMessage `json:"geometry"`
		Properties map[string]any  `json:"properties"`
	}
	out := struct {
		Type     string    `json:"type"`
		Source   string    `json:"source"`
		Features []feature `json:"features"`
	}{Type: "FeatureCollection", Source: source}

	for rows.Next() {
		var f feature
		var h3Cell *string
		var geomRaw []byte
		var propsRaw []byte
		if err := rows.Scan(&f.ID, &h3Cell, &geomRaw, &propsRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		f.Type = "Feature"
		f.Geometry = json.RawMessage(geomRaw)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &f.Properties)
		}
		if f.Properties == nil {
			f.Properties = map[string]any{}
		}
		if h3Cell != nil {
			f.Properties["_h3_r4"] = *h3Cell
		}
		f.Properties["_source"] = source
		f.Properties["_ext_id"] = f.ID
		out.Features = append(out.Features, f)
	}
	writeJSON(w, out)
}

type sourceRow struct {
	ID           string         `json:"id"`
	Kind         string         `json:"kind"`
	PollEverySec *int           `json:"poll_every_s,omitempty"`
	Enabled      bool           `json:"enabled"`
	LastOKAt     *time.Time     `json:"last_ok_at,omitempty"`
	LastErr      *string        `json:"last_err,omitempty"`
	Config       map[string]any `json:"config,omitempty"`
}

func (h *RestHandler) sources(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	rows, err := h.Pool.Query(ctx, `
		SELECT id, kind, poll_every_s, enabled, last_ok_at, last_err, config
		  FROM sources ORDER BY id`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	out := make([]sourceRow, 0, 16)
	for rows.Next() {
		var s sourceRow
		var cfgRaw []byte
		if err := rows.Scan(&s.ID, &s.Kind, &s.PollEverySec, &s.Enabled, &s.LastOKAt, &s.LastErr, &cfgRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(cfgRaw) > 0 {
			_ = json.Unmarshal(cfgRaw, &s.Config)
		}
		out = append(out, s)
	}
	writeJSON(w, out)
}

type latestRow struct {
	Ts     time.Time      `json:"ts"`
	Source string         `json:"source"`
	ExtID  string         `json:"ext_id"`
	Ref    string         `json:"event_ref"`
	H3R4   *string        `json:"h3_r4,omitempty"`
	Lat    *float64       `json:"lat,omitempty"`
	Lon    *float64       `json:"lon,omitempty"`
	Props  map[string]any `json:"props"`
}

func (h *RestHandler) latest(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		writeErr(w, http.StatusBadRequest, "source required")
		return
	}
	limit := latestDefaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > latestMaxLimit {
		limit = latestMaxLimit
	}
	maxAgeMin := 60 // default: newest snapshot per ext_id in last hour
	if s := r.URL.Query().Get("max_age_min"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxAgeMin = n
		}
	}
	boolQuery := func(name string) bool {
		s := r.URL.Query().Get(name)
		return s == "1" || strings.EqualFold(s, "true") || strings.EqualFold(s, "yes")
	}
	listOnly := boolQuery("list_only") || latestListOnlySource(source)
	includeNullGeom := listOnly || boolQuery("include_null_geom")
	h3R4 := h3R4Param(r)
	extID := strings.TrimSpace(r.URL.Query().Get("ext_id"))
	// The no-centroid path is intentionally restricted to small list panels.
	// Exposing it for arbitrary sources lets a browser request very wide raw
	// hypertable scans without geometry work, which can pressure Postgres hard
	// enough to kill a backend under the local Docker memory cap.
	omitCentroid := listOnly
	if listOnly && limit > 1000 {
		limit = 1000
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	now := time.Now().UTC()
	latestFrom := now.Add(-time.Duration(maxAgeMin) * time.Minute)
	latestTo := now.Add(time.Duration(maxAgeMin) * time.Minute)

	// Read newest rows through the source/time index and dedupe in Go. Letting
	// PostgreSQL sort a large source window by ext_id is too expensive for
	// high-volume layers such as FIRMS, lightning, and TLE.
	query := `
		WITH recent AS MATERIALIZED (
		  SELECT ts, source, ext_id, h3_r4, geom, props
		    FROM events
		   WHERE source = $1
		     %s
		     %s
		     %s
		     AND ts BETWEEN $2::timestamptz AND $3::timestamptz
		   ORDER BY ts DESC
		   LIMIT $4
		)
		SELECT ts, source, ext_id, h3_r4,
		       %s AS lat,
		       %s AS lon,
		       props
		  FROM recent`
	geomClause := "AND geom IS NOT NULL"
	if includeNullGeom {
		geomClause = ""
	}
	regularArgs := []any{source, latestFrom, latestTo, boundedLatestSampleLimit(limit)}
	nextArg := 5
	h3Clause := ""
	if h3R4 != "" {
		h3Clause = fmt.Sprintf("AND h3_r4 = $%d", nextArg)
		regularArgs = append(regularArgs, h3R4)
		nextArg++
	}
	extClause := ""
	if extID != "" {
		extClause = fmt.Sprintf("AND ext_id = $%d", nextArg)
		regularArgs = append(regularArgs, extID)
	}
	if omitCentroid {
		omitArgs := []any{source, latestFrom, boundedLatestSampleLimit(limit)}
		omitNextArg := 4
		omitH3Clause := ""
		if h3R4 != "" {
			omitH3Clause = fmt.Sprintf("AND h3_r4 = $%d", omitNextArg)
			omitArgs = append(omitArgs, h3R4)
			omitNextArg++
		}
		omitExtClause := ""
		if extID != "" {
			omitExtClause = fmt.Sprintf("AND ext_id = $%d", omitNextArg)
			omitArgs = append(omitArgs, extID)
		}
		rows, err := h.Pool.Query(ctx, fmt.Sprintf(`
			WITH recent AS MATERIALIZED (
			  SELECT ts, source, ext_id, h3_r4, props, ingested_at
			    FROM events
			   WHERE source = $1
			     %s
			     %s
			     %s
			     AND ingested_at >= $2::timestamptz
			   ORDER BY ingested_at DESC
			   LIMIT $3
			)
			SELECT ts, source, ext_id, h3_r4,
			       NULL::double precision AS lat,
			       NULL::double precision AS lon,
			       props
			  FROM recent`, geomClause, omitH3Clause, omitExtClause), omitArgs...)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeLatestRows(w, rows, source, limit)
		return
	}
	latExpr := "CASE WHEN geom IS NULL THEN NULL ELSE ST_Y(ST_Centroid(geom::geometry)) END"
	lonExpr := "CASE WHEN geom IS NULL THEN NULL ELSE ST_X(ST_Centroid(geom::geometry)) END"
	rows, err := h.Pool.Query(ctx, fmt.Sprintf(query, geomClause, h3Clause, extClause, latExpr, lonExpr), regularArgs...)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeLatestRows(w, rows, source, limit)
}

func writeLatestRows(w http.ResponseWriter, rows pgx.Rows, source string, limit int) {
	defer rows.Close()
	out := make([]latestRow, 0, 128)
	seen := make(map[string]struct{}, limit)
	for rows.Next() {
		var row latestRow
		var propsRaw []byte
		var h3Cell *string
		var lat, lon *float64
		if err := rows.Scan(&row.Ts, &row.Source, &row.ExtID, &h3Cell, &lat, &lon, &propsRaw); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		key := row.ExtID
		if key == "" {
			key = row.Ts.UTC().Format(time.RFC3339Nano)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		row.H3R4 = h3Cell
		row.Lat = lat
		row.Lon = lon
		row.Ref = eventRef(row.Source, row.ExtID, row.Ts)
		if len(propsRaw) > 0 {
			_ = json.Unmarshal(propsRaw, &row.Props)
		}
		if row.Props == nil {
			row.Props = map[string]any{}
		}
		row.Props["_event_ref"] = row.Ref
		row.Props["_source"] = row.Source
		row.Props["_ext_id"] = row.ExtID
		row.Props["_ts"] = row.Ts.UTC().Format(time.RFC3339)
		if h3Cell != nil {
			row.Props["_h3_r4"] = *h3Cell
		}
		out = append(out, row)
		if len(out) >= limit {
			rows.Close()
			break
		}
	}
	writeJSON(w, map[string]any{
		"source": source,
		"count":  len(out),
		"assets": out,
	})
}

// parseBBox parses "west,south,east,north" (degrees, WGS84).
// Returns ok=false if missing or malformed.
func parseBBox(s string) (w, s2, e, n float64, ok bool) {
	if s == "" {
		return
	}
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return
	}
	vals := make([]float64, 4)
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return
		}
		vals[i] = f
	}
	// Basic sanity: lat in [-90, 90], lon in [-180, 180], west<east, south<north.
	if vals[1] < -90 || vals[3] > 90 || vals[0] < -180 || vals[2] > 180 {
		return
	}
	if vals[0] >= vals[2] || vals[1] >= vals[3] {
		return
	}
	return vals[0], vals[1], vals[2], vals[3], true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("access-control-allow-origin", "*")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("access-control-allow-origin", "*")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
