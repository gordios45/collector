// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package featureseeds populates the `features` table from local GeoJSON files and a
// handful of external-static sources. Run once at provisioning and/or
// periodically via cron.
package featureseeds

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/fetchcache"
	"github.com/gordios45/collector/internal/seeders"
)

type sourceSpec struct {
	name   string
	origin string
	fn     func(ctx context.Context) ([]features.Feature, error)
}

func Main(args []string) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	fs := flag.NewFlagSet("features", flag.ExitOnError)
	dataDir := fs.String("data", "data", "Path to local collection seed data")
	cacheDir := fs.String("cache-dir", ".cache/features", "Directory for downloaded source cache; empty disables cache")
	refresh := fs.Bool("refresh", false, "Re-download remote sources even when a cached copy exists")
	listSources := fs.Bool("list", false, "List available seed source IDs and exit")
	fs.Parse(args)

	var fetcher fetchcache.BytesFetcher = fetchcache.CachedFetcher{
		Dir:     *cacheDir,
		Refresh: *refresh,
		Logf:    log.Printf,
	}
	if *cacheDir == "" {
		fetcher = fetchcache.HTTPFetcher{}
	}
	specs := sourceSpecs(*dataDir, fetcher)
	if *listSources {
		for _, s := range specs {
			fmt.Printf("%-20s %s\n", s.name, s.origin)
		}
		return
	}

	db.LoadDotEnv("../.env", ".env")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	// Optional CLI filter: only seed specified source IDs.
	filter := map[string]bool{}
	for _, a := range fs.Args() {
		filter[a] = true
	}

	okCount := 0
	for _, s := range specs {
		if len(filter) > 0 && !filter[s.name] {
			continue
		}
		// Register source first (FK target) — idempotent.
		if _, err := pool.Exec(ctx, `
			INSERT INTO sources (id, kind, enabled, config)
			VALUES ($1, 'polygon_static', TRUE, '{}'::jsonb)
			ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind`, s.name); err != nil {
			log.Printf("[features] %s: REGISTER FAIL: %v", s.name, err)
			continue
		}

		feats, err := s.fn(ctx)
		if err != nil {
			log.Printf("[features] %s: FETCH FAIL: %v", s.name, err)
			continue
		}
		n, err := features.Upsert(ctx, pool, s.name, feats)
		if err != nil {
			log.Printf("[features] %s: UPSERT FAIL: %v", s.name, err)
			continue
		}
		log.Printf("[features] %s: %d features", s.name, n)
		okCount++
	}

	fmt.Printf("seed complete: %d/%d sources OK\n", okCount, len(specs))
}

func sourceSpecs(dataDir string, fetcher fetchcache.BytesFetcher) []sourceSpec {
	localFiles := []struct {
		source string
		file   string
	}{
		{"chokepoints", "chokepoints.geojson"},
		{"pipelines", "pipelines.geojson"},
		{"oil_refineries", "oil_refineries.geojson"},
		{"desal_plants", "desal_plants.geojson"},
	}

	specs := []sourceSpec{}
	for _, local := range localFiles {
		src, filename := local.source, local.file
		specs = append(specs, sourceSpec{
			name:   src,
			origin: "local:" + filename,
			fn: func(ctx context.Context) ([]features.Feature, error) {
				return seeders.LocalFile(dataDir, filename, src)
			},
		})
	}
	remoteURLs := seeders.RemoteSourceURLs()
	specs = append(specs,
		sourceSpec{"cables", "remote:" + remoteURLs["cables"], func(ctx context.Context) ([]features.Feature, error) {
			return seeders.CablesWithFetcher(ctx, fetcher)
		}},
		sourceSpec{"nuclear_facilities", "remote:" + remoteURLs["nuclear_facilities"], func(ctx context.Context) ([]features.Feature, error) {
			return seeders.NuclearFacilitiesWithFetcher(ctx, fetcher)
		}},
		sourceSpec{"power_plants", "remote:" + remoteURLs["power_plants"], func(ctx context.Context) ([]features.Feature, error) {
			return seeders.PowerPlantsWithFetcher(ctx, fetcher)
		}},
	)
	return specs
}
