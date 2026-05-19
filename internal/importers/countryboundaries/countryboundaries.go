// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package countryboundaries imports Natural Earth country boundaries into PostGIS.
//
// The default source is Natural Earth's GitHub GeoJSON mirror:
// https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_0_countries.geojson
package countryboundaries

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/fetchcache"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultURL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_0_countries.geojson"

type featureCollection struct {
	Type     string    `json:"type"`
	Features []feature `json:"features"`
}

type feature struct {
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
	Geometry   map[string]any `json:"geometry"`
}

func Main(args []string) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	fs := flag.NewFlagSet("country-boundaries", flag.ExitOnError)
	url := fs.String("url", defaultURL, "GeoJSON URL to download when --geojson is empty")
	path := fs.String("geojson", "", "local GeoJSON file; empty downloads --url")
	cacheDir := fs.String("cache-dir", ".cache/importer", "Directory for downloaded source cache; empty disables cache")
	refresh := fs.Bool("refresh", false, "Re-download remote sources even when a cached copy exists")
	fs.Parse(args)

	db.LoadDotEnv("../.env", ".env", "../../.env")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	fetcher := fetchcache.CachedFetcher{Dir: *cacheDir, Refresh: *refresh, Logf: log.Printf}
	raw, err := readGeoJSON(ctx, *path, *url, fetcher)
	if err != nil {
		log.Fatalf("load geojson: %v", err)
	}

	var fc featureCollection
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&fc); err != nil {
		log.Fatalf("decode geojson: %v", err)
	}
	if !strings.EqualFold(fc.Type, "FeatureCollection") || len(fc.Features) == 0 {
		log.Fatalf("not a usable FeatureCollection: type=%q features=%d", fc.Type, len(fc.Features))
	}

	var imported, skipped int
	for _, f := range fc.Features {
		row, ok := boundaryRowFromFeature(f)
		if !ok {
			skipped++
			continue
		}
		if err := upsertBoundary(ctx, pool, row); err != nil {
			log.Printf("upsert %s/%s: %v", row.isoA2, row.name, err)
			skipped++
			continue
		}
		imported++
	}
	log.Printf("done — imported=%d skipped=%d source=%s", imported, skipped, sourceName(*path, *url))
}

func readGeoJSON(ctx context.Context, path, url string, fetcher fetchcache.BytesFetcher) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	return fetcher.GetBytes(ctx, url, nil)
}

type boundaryRow struct {
	id, isoA2, isoA3  string
	name, sovereign   string
	region, subregion string
	geomJSON          string
	source            string
}

func boundaryRowFromFeature(f feature) (boundaryRow, bool) {
	if f.Geometry == nil {
		return boundaryRow{}, false
	}
	geomRaw, err := json.Marshal(f.Geometry)
	if err != nil || len(geomRaw) == 0 || string(geomRaw) == "null" {
		return boundaryRow{}, false
	}

	name := propString(f.Properties, "NAME", "ADMIN", "NAME_LONG", "SOVEREIGNT")
	if name == "" {
		return boundaryRow{}, false
	}
	isoA2 := validA2(propString(f.Properties, "ISO_A2", "ISO_A2_EH", "WB_A2", "POSTAL"))
	isoA3 := propString(f.Properties, "ADM0_A3", "ISO_A3", "ISO_A3_EH", "WB_A3")
	if isoA2 == "" {
		isoA2 = isoA2FromA3(isoA3)
	}
	id := propString(f.Properties, "NE_ID")
	if id == "" {
		id = strings.ToUpper(firstNonEmpty(isoA3, isoA2, sanitizeID(name)))
	}
	return boundaryRow{
		id:        id,
		isoA2:     isoA2,
		isoA3:     isoA3,
		name:      name,
		sovereign: propString(f.Properties, "SOVEREIGNT"),
		region:    propString(f.Properties, "REGION_UN", "REGION_WB"),
		subregion: propString(f.Properties, "SUBREGION"),
		geomJSON:  string(geomRaw),
		source:    "natural_earth_50m_admin0",
	}, true
}

func upsertBoundary(ctx context.Context, pool *pgxpool.Pool, row boundaryRow) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO country_boundaries
		  (id, iso_a2, iso_a3, name, sovereign, region, subregion, source, updated_at, geom)
		SELECT $1, NULLIF($2,''), NULLIF($3,''), $4, NULLIF($5,''), NULLIF($6,''), NULLIF($7,''), $8, now(),
		       ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON($9), 4326)), 3))::geometry(MultiPolygon,4326)
		ON CONFLICT (id) DO UPDATE
		   SET iso_a2     = EXCLUDED.iso_a2,
		       iso_a3     = EXCLUDED.iso_a3,
		       name       = EXCLUDED.name,
		       sovereign  = EXCLUDED.sovereign,
		       region     = EXCLUDED.region,
		       subregion  = EXCLUDED.subregion,
		       source     = EXCLUDED.source,
		       updated_at = now(),
		       geom       = EXCLUDED.geom`,
		row.id, row.isoA2, row.isoA3, row.name, row.sovereign, row.region, row.subregion, row.source, row.geomJSON)
	return err
}

func propString(props map[string]any, keys ...string) string {
	for _, k := range keys {
		v, ok := props[k]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case string:
			if s := strings.TrimSpace(x); s != "" && s != "-99" {
				return s
			}
		case json.Number:
			return x.String()
		default:
			s := strings.TrimSpace(fmt.Sprint(x))
			if s != "" && s != "<nil>" && s != "-99" {
				return s
			}
		}
	}
	return ""
}

func validA2(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) == 2 && s != "-99" {
		return s
	}
	return ""
}

func isoA2FromA3(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TWN":
		return "TW"
	case "KOS":
		return "XK"
	default:
		return ""
	}
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return strings.TrimSpace(x)
		}
	}
	return ""
}

func sanitizeID(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	repl := strings.NewReplacer(" ", "_", ".", "", ",", "", "'", "", "(", "", ")", "", "/", "_")
	return repl.Replace(s)
}

func sourceName(path, url string) string {
	if path != "" {
		return path
	}
	return url
}
