// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ingestionAOI struct {
	ID         string         `json:"id"`
	Label      string         `json:"label"`
	Kind       string         `json:"kind"`
	Lat        float64        `json:"lat"`
	Lon        float64        `json:"lon"`
	Priority   float64        `json:"priority"`
	RadiusM    *float64       `json:"radius_m,omitempty"`
	Collectors []string       `json:"collectors"`
	Metadata   map[string]any `json:"metadata"`
	Enabled    bool           `json:"enabled"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type ingestionAOIRequest struct {
	ID         string         `json:"id"`
	Label      string         `json:"label"`
	Kind       string         `json:"kind"`
	Lat        float64        `json:"lat"`
	Lon        float64        `json:"lon"`
	Priority   *float64       `json:"priority"`
	RadiusM    *float64       `json:"radius_m"`
	Collectors []string       `json:"collectors"`
	Metadata   map[string]any `json:"metadata"`
	Props      map[string]any `json:"props"`
	Enabled    *bool          `json:"enabled"`
}

func (h *RestHandler) RegisterIngestionAOIs(mux *http.ServeMux) {
	mux.HandleFunc("/api/ingestion/aois", h.ingestionAOIs)
	mux.HandleFunc("/api/ingestion/aois/", h.ingestionAOIByID)
}

func (h *RestHandler) ingestionAOIs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listIngestionAOIs(w, r)
	case http.MethodPost:
		h.upsertIngestionAOI(w, r, "")
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *RestHandler) ingestionAOIByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/api/ingestion/aois/"))
	if id == "" || strings.Contains(id, "/") {
		writeErr(w, http.StatusNotFound, "aoi not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.getIngestionAOI(w, r, id)
	case http.MethodPut, http.MethodPost:
		h.upsertIngestionAOI(w, r, id)
	case http.MethodDelete:
		h.deleteIngestionAOI(w, r, id)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *RestHandler) listIngestionAOIs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	collector := strings.TrimSpace(r.URL.Query().Get("collector"))
	rows, err := h.Pool.Query(ctx, `
		SELECT id, label, kind, lat, lon, priority, radius_m, collectors, metadata, enabled, created_at, updated_at
		  FROM ingestion_aois
		 WHERE ($1 = '' OR cardinality(collectors) = 0 OR $1 = ANY(collectors))
		 ORDER BY enabled DESC, priority DESC, updated_at DESC`, collector)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	out := []ingestionAOI{}
	for rows.Next() {
		a, err := scanIngestionAOI(rows)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]any{"aois": out})
}

func (h *RestHandler) getIngestionAOI(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	row := h.Pool.QueryRow(ctx, `
		SELECT id, label, kind, lat, lon, priority, radius_m, collectors, metadata, enabled, created_at, updated_at
		  FROM ingestion_aois
		 WHERE id = $1`, id)
	a, err := scanIngestionAOIRow(row)
	if err != nil {
		writeErr(w, http.StatusNotFound, "aoi not found")
		return
	}
	writeJSON(w, a)
}

func (h *RestHandler) upsertIngestionAOI(w http.ResponseWriter, r *http.Request, pathID string) {
	var req ingestionAOIRequest
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.Metadata == nil && req.Props != nil {
		req.Metadata = req.Props
	}
	if pathID != "" {
		req.ID = pathID
	}
	req.ID = normalizeAOIID(req.ID)
	if req.Label = strings.TrimSpace(req.Label); req.Label == "" {
		req.Label = req.ID
	}
	if req.Kind = strings.TrimSpace(req.Kind); req.Kind == "" {
		req.Kind = "manual"
	}
	if req.ID == "" {
		req.ID = generatedAOIID(req.Kind, req.Label, req.Lat, req.Lon)
	}
	if !validAOILatLon(req.Lat, req.Lon) {
		writeErr(w, http.StatusBadRequest, "valid lat/lon required")
		return
	}
	priority := 1.0
	if req.Priority != nil {
		priority = *req.Priority
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if req.RadiusM != nil && *req.RadiusM <= 0 {
		writeErr(w, http.StatusBadRequest, "radius_m must be positive")
		return
	}
	metadata := req.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "metadata must be json")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	row := h.Pool.QueryRow(ctx, `
		INSERT INTO ingestion_aois
		  (id, label, kind, lat, lon, priority, radius_m, collectors, metadata, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
		ON CONFLICT (id) DO UPDATE
		   SET label = EXCLUDED.label,
		       kind = EXCLUDED.kind,
		       lat = EXCLUDED.lat,
		       lon = EXCLUDED.lon,
		       priority = EXCLUDED.priority,
		       radius_m = EXCLUDED.radius_m,
		       collectors = EXCLUDED.collectors,
		       metadata = EXCLUDED.metadata,
		       enabled = EXCLUDED.enabled,
		       updated_at = now()
		RETURNING id, label, kind, lat, lon, priority, radius_m, collectors, metadata, enabled, created_at, updated_at`,
		req.ID, req.Label, req.Kind, req.Lat, req.Lon, priority, req.RadiusM, normalizeCollectors(req.Collectors), string(rawMetadata), enabled)
	a, err := scanIngestionAOIRow(row)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, a)
}

func (h *RestHandler) deleteIngestionAOI(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tag, err := h.Pool.Exec(ctx, `DELETE FROM ingestion_aois WHERE id = $1`, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		writeErr(w, http.StatusNotFound, "aoi not found")
		return
	}
	writeJSON(w, map[string]any{"deleted": true, "id": id})
}

type ingestionAOIScanner interface {
	Scan(dest ...any) error
}

func scanIngestionAOIRow(row ingestionAOIScanner) (ingestionAOI, error) {
	var a ingestionAOI
	if err := row.Scan(
		&a.ID, &a.Label, &a.Kind, &a.Lat, &a.Lon, &a.Priority,
		&a.RadiusM, &a.Collectors, &a.Metadata, &a.Enabled, &a.CreatedAt, &a.UpdatedAt,
	); err != nil {
		return ingestionAOI{}, err
	}
	if a.Metadata == nil {
		a.Metadata = map[string]any{}
	}
	return a, nil
}

func scanIngestionAOI(row ingestionAOIScanner) (ingestionAOI, error) {
	return scanIngestionAOIRow(row)
}

func normalizeCollectors(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, s := range in {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

var aoiIDRe = regexp.MustCompile(`[^a-z0-9:_-]+`)

func normalizeAOIID(id string) string {
	id = strings.ToLower(strings.TrimSpace(id))
	id = aoiIDRe.ReplaceAllString(id, "-")
	id = strings.Trim(id, "-")
	return id
}

func generatedAOIID(kind, label string, lat, lon float64) string {
	base := normalizeAOIID(kind + ":" + label)
	h := fnv.New32a()
	_, _ = h.Write([]byte(fmt.Sprintf("%s|%.6f|%.6f", base, lat, lon)))
	if base == "" {
		base = "aoi"
	}
	return base + ":" + strconv.FormatUint(uint64(h.Sum32()), 16)
}

func validAOILatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && (lat != 0 || lon != 0)
}
