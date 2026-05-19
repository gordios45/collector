// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type sourceStatusResponse struct {
	GeneratedAt time.Time             `json:"generated_at"`
	Count       int                   `json:"count"`
	Sources     []sourceStatusPayload `json:"sources"`
}

type sourceStatusPayload struct {
	ID                  string                          `json:"id"`
	Name                string                          `json:"name"`
	Kind                string                          `json:"kind"`
	Enabled             bool                            `json:"enabled"`
	Status              string                          `json:"status"`
	Stale               bool                            `json:"stale"`
	PollEverySec        *int                            `json:"poll_every_s,omitempty"`
	RefreshRate         string                          `json:"refresh_rate,omitempty"`
	StaleAfterSec       *int                            `json:"stale_after_s,omitempty"`
	LastFetchAt         *time.Time                      `json:"last_fetch_at,omitempty"`
	LastOKAt            *time.Time                      `json:"last_ok_at,omitempty"`
	LastErr             *string                         `json:"last_err,omitempty"`
	LastEventTS         *time.Time                      `json:"last_event_ts,omitempty"`
	LastEventIngestedAt *time.Time                      `json:"last_event_ingested_at,omitempty"`
	FreshnessContract   *sourceFreshnessContractPayload `json:"freshness_contract,omitempty"`
	RecentRuns          []sourceIngestRun               `json:"recent_runs"`
}

type sourceFreshnessContractPayload struct {
	Enabled                  bool    `json:"enabled"`
	ExpectedMinRowsPerWindow int     `json:"expected_min_rows_per_window"`
	ExpectedWindowSec        *int    `json:"expected_window_s,omitempty"`
	ExpectedMaxLagSec        *int    `json:"expected_max_lag_s,omitempty"`
	Severity                 string  `json:"severity,omitempty"`
	Note                     *string `json:"note,omitempty"`
}

type sourceIngestRun struct {
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	OK           bool      `json:"ok"`
	RowsFetched  int       `json:"rows_fetched"`
	RowsInserted int       `json:"rows_inserted"`
	PayloadBytes int64     `json:"payload_bytes"`
	DurationMS   int       `json:"duration_ms"`
	Error        *string   `json:"error,omitempty"`
}

func (h *RestHandler) sourceStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := h.Pool.Query(ctx, `
		SELECT s.id, s.kind, s.poll_every_s, s.enabled, s.last_fetch_at, s.last_ok_at, s.last_err, s.config,
		       COALESCE(s.freshness_contract_enabled, false),
		       COALESCE(s.expected_min_rows_per_window, 0),
		       EXTRACT(EPOCH FROM s.expected_window)::int,
		       EXTRACT(EPOCH FROM s.expected_max_lag)::int,
		       COALESCE(s.freshness_contract_severity, ''),
		       s.freshness_contract_note,
		       COALESCE(runs.recent_runs, '[]'::jsonb)
		  FROM sources s
		  LEFT JOIN LATERAL (
		    SELECT jsonb_agg(jsonb_build_object(
		             'started_at', started_at,
		             'finished_at', finished_at,
		             'ok', ok,
		             'rows_fetched', rows_fetched,
		             'rows_inserted', rows_inserted,
		             'payload_bytes', payload_bytes,
		             'duration_ms', duration_ms,
		             'error', error
		           ) ORDER BY started_at DESC) AS recent_runs
		      FROM (
		        SELECT started_at, finished_at, ok, rows_fetched, rows_inserted, payload_bytes, duration_ms, error
		          FROM source_ingest_runs
		         WHERE source_id = s.id
		         ORDER BY started_at DESC
		         LIMIT 3
		      ) rr
		  ) runs ON TRUE
		 ORDER BY s.id`)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	now := time.Now().UTC()
	out := sourceStatusResponse{GeneratedAt: now, Sources: []sourceStatusPayload{}}
	for rows.Next() {
		var row sourceStatusPayload
		var cfgRaw []byte
		var runsRaw []byte
		var freshnessEnabled bool
		var expectedMinRows int
		var expectedWindowSec *int
		var expectedMaxLagSec *int
		var freshnessSeverity string
		var freshnessNote *string
		if err := rows.Scan(
			&row.ID, &row.Kind, &row.PollEverySec, &row.Enabled,
			&row.LastFetchAt, &row.LastOKAt, &row.LastErr, &cfgRaw,
			&freshnessEnabled, &expectedMinRows, &expectedWindowSec, &expectedMaxLagSec,
			&freshnessSeverity, &freshnessNote,
			&runsRaw,
		); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		cfg := map[string]any{}
		if len(cfgRaw) > 0 {
			_ = json.Unmarshal(cfgRaw, &cfg)
		}
		if len(runsRaw) > 0 {
			_ = json.Unmarshal(runsRaw, &row.RecentRuns)
		}
		if row.RecentRuns == nil {
			row.RecentRuns = []sourceIngestRun{}
		}
		row.Name = sourceDisplayName(row.ID, cfg)
		row.RefreshRate = refreshRateLabel(row.PollEverySec)
		if freshnessEnabled || expectedMinRows > 0 || expectedWindowSec != nil || expectedMaxLagSec != nil {
			row.FreshnessContract = &sourceFreshnessContractPayload{
				Enabled:                  freshnessEnabled,
				ExpectedMinRowsPerWindow: expectedMinRows,
				ExpectedWindowSec:        expectedWindowSec,
				ExpectedMaxLagSec:        expectedMaxLagSec,
				Severity:                 freshnessSeverity,
				Note:                     freshnessNote,
			}
		}
		row.Status, row.Stale, row.StaleAfterSec = sourceStatusValue(row.ID, row.Enabled, row.PollEverySec, row.LastOKAt, row.LastErr, expectedMinRows, row.RecentRuns, now)
		out.Sources = append(out.Sources, row)
	}
	out.Count = len(out.Sources)
	writeJSON(w, out)
}

func sourceDisplayName(id string, cfg map[string]any) string {
	for _, key := range []string{"name", "label", "title"} {
		if v, ok := cfg[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	parts := strings.Fields(strings.ReplaceAll(id, "_", " "))
	for i, part := range parts {
		if len(part) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	if len(parts) == 0 {
		return id
	}
	return strings.Join(parts, " ")
}

func refreshRateLabel(pollEverySec *int) string {
	if pollEverySec == nil || *pollEverySec <= 0 {
		return "static/manual"
	}
	return (time.Duration(*pollEverySec) * time.Second).String()
}

func sourceStatusValue(id string, enabled bool, pollEverySec *int, lastOKAt *time.Time, lastErr *string, expectedMinRows int, recentRuns []sourceIngestRun, now time.Time) (string, bool, *int) {
	if !enabled {
		return "disabled", false, nil
	}
	if lastErr != nil && strings.TrimSpace(*lastErr) != "" {
		if strings.HasPrefix(strings.TrimSpace(*lastErr), "freshness_contract_violated") {
			return "freshness_contract_violated", true, staleAfterSeconds(pollEverySec)
		}
		return "error", false, staleAfterSeconds(pollEverySec)
	}
	if pollEverySec == nil || *pollEverySec <= 0 {
		if lastOKAt == nil {
			return "static", false, nil
		}
		return "ok", false, nil
	}
	if lastOKAt == nil {
		return "never_ok", false, staleAfterSeconds(pollEverySec)
	}
	staleAfter := staleAfterSeconds(pollEverySec)
	if staleAfter != nil && now.Sub(lastOKAt.UTC()) > time.Duration(*staleAfter)*time.Second {
		return "stale", true, staleAfter
	}
	if expectedMinRows > 0 && sourceStatusHasStaleSuccess(id, recentRuns) {
		return "stale_success", true, staleAfter
	}
	return "ok", false, staleAfter
}

func sourceStatusHasStaleSuccess(id string, recentRuns []sourceIngestRun) bool {
	switch strings.TrimSpace(id) {
	case "space_weather", "sentinel_sar_change", "usgs_shakemap", "swdi_radar_signatures",
		"nga_warnings", "nhc_gis_cones", "official_advisories", "planned_protests",
		"netblocks_rss", "fews_net_food_security", "ifrc_go", "volcanoes",
		"copernicus_gdo_drought", "inform_risk_severity",
		"gps_jamming", "tor_metrics", "safecast", "open_meteo_anomalies", "black_marble_nightlights":
	default:
		return false
	}
	if len(recentRuns) < 3 {
		return false
	}
	for i := 0; i < 3; i++ {
		if !recentRuns[i].OK || recentRuns[i].RowsInserted > 0 {
			return false
		}
	}
	return true
}

func staleAfterSeconds(pollEverySec *int) *int {
	if pollEverySec == nil || *pollEverySec <= 0 {
		return nil
	}
	n := *pollEverySec * 3
	if n < 3600 {
		n = 3600
	}
	return &n
}
