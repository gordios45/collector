// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Seeders that fetch from external static-ish sources and return features.
package seeders

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/fetchcache"
)

const (
	cablesURL            = "https://www.submarinecablemap.com/api/v3/cable/cable-geo.json"
	nuclearFacilitiesURL = "https://raw.githubusercontent.com/cristianst85/GeoNuclearData/master/data/json/denormalized/nuclear_power_plants.json"
	powerPlantsURL       = "https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv"
)

// Cables: TeleGeography submarine cable routes (LineString).
// Raw geocoded data is licensed by upstream.
// https://www.submarinecablemap.com/api/v3/cable/cable-geo.json
func Cables(ctx context.Context) ([]features.Feature, error) {
	return CablesWithFetcher(ctx, fetchcache.HTTPFetcher{})
}

func CablesWithFetcher(ctx context.Context, fetcher fetchcache.BytesFetcher) ([]features.Feature, error) {
	var fc struct {
		Features []struct {
			ID         any             `json:"id"`
			Geometry   json.RawMessage `json:"geometry"`
			Properties map[string]any  `json:"properties"`
		} `json:"features"`
	}
	if err := fetchJSON(ctx, fetcher, cablesURL, nil, &fc); err != nil {
		return nil, err
	}
	out := make([]features.Feature, 0, len(fc.Features))
	seen := map[string]int{}
	for i, f := range fc.Features {
		wkt, err := geomToWKT(f.Geometry)
		if err != nil || wkt == "" {
			continue
		}
		props := f.Properties
		if props == nil {
			props = map[string]any{}
		}
		baseID := fmt.Sprintf("%v", firstNonEmpty(f.ID, props["id"], i))
		// Submarine cables occasionally re-use the same `id` for multiple
		// segments of the same cable — dedupe with a suffix.
		uniqueID := baseID
		if n := seen[baseID]; n > 0 {
			uniqueID = fmt.Sprintf("%s#%d", baseID, n)
		}
		seen[baseID]++
		out = append(out, features.Feature{
			ExtID:   uniqueID,
			GeomWKT: wkt,
			Props:   props,
		})
	}
	return out, nil
}

// NuclearFacilities: GeoNuclearData JSON dataset. Each row has latitude,
// longitude, and reactor metadata.
// https://raw.githubusercontent.com/cristianst85/GeoNuclearData/master/data/json/denormalized/nuclear_power_plants.json
func NuclearFacilities(ctx context.Context) ([]features.Feature, error) {
	return NuclearFacilitiesWithFetcher(ctx, fetchcache.HTTPFetcher{})
}

func NuclearFacilitiesWithFetcher(ctx context.Context, fetcher fetchcache.BytesFetcher) ([]features.Feature, error) {
	var rows []map[string]any
	if err := fetchJSON(ctx, fetcher, nuclearFacilitiesURL, nil, &rows); err != nil {
		return nil, err
	}
	return fromPointArray(rows), nil
}

// PowerPlants: WRI global power plant database (CSV). Geolocated rows.
// https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv
func PowerPlants(ctx context.Context) ([]features.Feature, error) {
	return PowerPlantsWithFetcher(ctx, fetchcache.HTTPFetcher{})
}

func PowerPlantsWithFetcher(ctx context.Context, fetcher fetchcache.BytesFetcher) ([]features.Feature, error) {
	buf, err := fetchBytes(ctx, fetcher, powerPlantsURL, nil)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(buf))
	hdr, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, h := range hdr {
		idx[h] = i
	}
	lati, lon1 := idx["latitude"], idx["longitude"]
	nameI, gppdI := idx["name"], idx["gppd_idnr"]
	capI, fuelI, countryI := idx["capacity_mw"], idx["primary_fuel"], idx["country"]

	out := []features.Feature{}
	for {
		row, err := r.Read()
		if err != nil {
			break
		}
		lat, _ := strconv.ParseFloat(row[lati], 64)
		lng, _ := strconv.ParseFloat(row[lon1], 64)
		if lat == 0 && lng == 0 {
			continue
		}
		capacity, _ := strconv.ParseFloat(row[capI], 64)
		ext := row[gppdI]
		if ext == "" {
			ext = row[nameI]
		}
		props := map[string]any{
			"name":         row[nameI],
			"gppd_idnr":    row[gppdI],
			"capacity_mw":  capacity,
			"primary_fuel": row[fuelI],
			"country":      row[countryI],
			"latitude":     lat,
			"longitude":    lng,
		}
		out = append(out, features.Feature{
			ExtID:   ext,
			GeomWKT: fmt.Sprintf("POINT(%f %f)", lng, lat),
			Props:   props,
		})
	}
	return out, nil
}

func RemoteSourceURLs() map[string]string {
	return map[string]string{
		"cables":             cablesURL,
		"nuclear_facilities": nuclearFacilitiesURL,
		"power_plants":       powerPlantsURL,
	}
}

func fetchJSON(ctx context.Context, fetcher fetchcache.BytesFetcher, url string, headers map[string]string, out any) error {
	buf, err := fetchBytes(ctx, fetcher, url, headers)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}

func fetchBytes(ctx context.Context, fetcher fetchcache.BytesFetcher, url string, headers map[string]string) ([]byte, error) {
	if fetcher == nil {
		fetcher = fetchcache.HTTPFetcher{}
	}
	return fetcher.GetBytes(ctx, url, headers)
}
