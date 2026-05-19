// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package aircraftregistry imports the OpenSky Network aircraftDatabase.csv
// (~520k records), published openly at s3.opensky-network.org for research use.
//
// Idempotent UPSERT on icao24; re-running with a newer CSV refreshes.
package aircraftregistry

import (
	"bytes"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/fetchcache"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const openSkyURL = "https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv"

func Main(args []string) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	fs := flag.NewFlagSet("aircraft-registry", flag.ExitOnError)
	csvPath := fs.String("csv", "", "local path to aircraftDatabase.csv; empty = download from OpenSky")
	cacheDir := fs.String("cache-dir", ".cache/importer", "Directory for downloaded source cache; empty disables cache")
	refresh := fs.Bool("refresh", false, "Re-download remote sources even when a cached copy exists")
	batchSize := fs.Int("batch", 500, "rows per COPY-less batched INSERT")
	fs.Parse(args)

	db.LoadDotEnv("../.env", ".env")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	var src io.ReadCloser
	if *csvPath != "" {
		f, err := os.Open(*csvPath)
		if err != nil {
			log.Fatalf("open: %v", err)
		}
		src = f
		log.Printf("reading local file: %s", *csvPath)
	} else {
		fetcher := fetchcache.CachedFetcher{Dir: *cacheDir, Refresh: *refresh, Logf: log.Printf}
		buf, err := fetcher.GetBytes(ctx, openSkyURL, nil)
		if err != nil {
			log.Fatalf("fetch: %v", err)
		}
		src = io.NopCloser(bytes.NewReader(buf))
	}
	defer src.Close()

	cr := csv.NewReader(src)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	header, err := cr.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}
	get := func(row []string, key string) string {
		i, ok := col[key]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	batch := make([][]any, 0, *batchSize)
	var total, empty, skipped int
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := upsertBatch(ctx, pool, batch); err != nil {
			log.Printf("batch upsert error: %v", err)
		}
		batch = batch[:0]
	}

	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		total++
		hex := strings.ToLower(strings.TrimSpace(get(row, "icao24")))
		if hex == "" {
			empty++
			continue
		}
		// Skip rows that are entirely empty beyond icao24 — OpenSky CSV has
		// placeholder rows for hexes with no identified airframe.
		reg := get(row, "registration")
		op := get(row, "operator")
		own := get(row, "owner")
		mdl := get(row, "model")
		if reg == "" && op == "" && own == "" && mdl == "" {
			skipped++
			continue
		}
		batch = append(batch, []any{
			hex,
			nilOr(reg),
			nilOr(get(row, "manufacturericao")),
			nilOr(get(row, "manufacturername")),
			nilOr(mdl),
			nilOr(get(row, "typecode")),
			nilOr(get(row, "serialnumber")),
			nilOr(get(row, "icaoaircrafttype")),
			nilOr(op),
			nilOr(get(row, "operatorcallsign")),
			nilOr(get(row, "operatoricao")),
			nilOr(get(row, "operatoriata")),
			nilOr(own),
			nilOr(get(row, "built")),
			nilOr(get(row, "status")),
			nilOr(get(row, "notes")),
		})
		if len(batch) >= *batchSize {
			flush()
		}
	}
	flush()

	log.Printf("done — total=%d empty=%d skipped(only-hex)=%d inserted/updated=%d",
		total, empty, skipped, total-empty-skipped)
}

func nilOr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func upsertBatch(ctx context.Context, pool *pgxpool.Pool, rows [][]any) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const stmt = `
		INSERT INTO aircraft_registry
			(icao24, registration, manufacturer_icao, manufacturer_name,
			 model, typecode, serial_number, icao_aircraft_type,
			 operator, operator_callsign, operator_icao, operator_iata,
			 owner, built, status, notes, source, imported_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'opensky',now())
		ON CONFLICT (icao24) DO UPDATE SET
			registration       = COALESCE(EXCLUDED.registration, aircraft_registry.registration),
			manufacturer_icao  = COALESCE(EXCLUDED.manufacturer_icao, aircraft_registry.manufacturer_icao),
			manufacturer_name  = COALESCE(EXCLUDED.manufacturer_name, aircraft_registry.manufacturer_name),
			model              = COALESCE(EXCLUDED.model, aircraft_registry.model),
			typecode           = COALESCE(EXCLUDED.typecode, aircraft_registry.typecode),
			serial_number      = COALESCE(EXCLUDED.serial_number, aircraft_registry.serial_number),
			icao_aircraft_type = COALESCE(EXCLUDED.icao_aircraft_type, aircraft_registry.icao_aircraft_type),
			operator           = COALESCE(EXCLUDED.operator, aircraft_registry.operator),
			operator_callsign  = COALESCE(EXCLUDED.operator_callsign, aircraft_registry.operator_callsign),
			operator_icao      = COALESCE(EXCLUDED.operator_icao, aircraft_registry.operator_icao),
			operator_iata      = COALESCE(EXCLUDED.operator_iata, aircraft_registry.operator_iata),
			owner              = COALESCE(EXCLUDED.owner, aircraft_registry.owner),
			built              = COALESCE(EXCLUDED.built, aircraft_registry.built),
			status             = COALESCE(EXCLUDED.status, aircraft_registry.status),
			notes              = COALESCE(EXCLUDED.notes, aircraft_registry.notes),
			imported_at        = now()`
	for _, r := range rows {
		if _, err := tx.Exec(ctx, stmt, r...); err != nil {
			return fmt.Errorf("upsert %v: %w", r[0], err)
		}
	}
	return tx.Commit(ctx)
}
