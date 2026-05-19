// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package worldpop_exposure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gordios45/collector/internal/events"

	"github.com/jackc/pgx/v5/pgxpool"
)

type catalogProduct struct {
	ID               string
	Name             string
	Role             string
	Endpoint         string
	ProductURL       string
	Resolution       string
	DatasetYear      int
	IntegrationState string
	License          string
}

type CatalogCollector struct {
	pool    *pgxpool.Pool
	product catalogProduct
}

func NewGHSLPopulation(pool *pgxpool.Pool) (*CatalogCollector, error) {
	return newCatalogCollector(pool, catalogProduct{
		ID:               "ghsl_population",
		Name:             "GHSL GHS-POP",
		Role:             "canonical_population_baseline",
		Endpoint:         "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/",
		ProductURL:       "https://human-settlement.emergency.copernicus.eu/ghs_pop2023.php",
		Resolution:       "100m global GeoTIFF; 3 arc-second and 30 arc-second WGS84 packages available",
		DatasetYear:      2020,
		IntegrationState: "catalog_ready_preaggregation_required",
		License:          "European Commission reuse with attribution",
	})
}

func NewGHSLSettlementModel(pool *pgxpool.Pool) (*CatalogCollector, error) {
	return newCatalogCollector(pool, catalogProduct{
		ID:               "ghsl_smod",
		Name:             "GHSL GHS-SMOD",
		Role:             "settlement_model_baseline",
		Endpoint:         "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_SMOD_GLOBE_R2023A/",
		ProductURL:       "https://human-settlement.emergency.copernicus.eu/ghs_smod2023.php",
		Resolution:       "1km settlement model, WGS84 packages available",
		DatasetYear:      2020,
		IntegrationState: "catalog_ready_preaggregation_required",
		License:          "European Commission reuse with attribution",
	})
}

func NewHRSLPopulation(pool *pgxpool.Pool) (*CatalogCollector, error) {
	return newCatalogCollector(pool, catalogProduct{
		ID:               "hrsl_population",
		Name:             "HRSL Population",
		Role:             "high_resolution_population_optional",
		Endpoint:         "https://registry.opendata.aws/dataforgood-fb-hrsl/",
		ProductURL:       "s3://dataforgood-fb-hrsl/",
		Resolution:       "approximately 30m where country products are available",
		DatasetYear:      2015,
		IntegrationState: "catalog_ready_country_coverage_varies",
		License:          "open data; verify country/product terms before redistribution",
	})
}

func newCatalogCollector(pool *pgxpool.Pool, product catalogProduct) (*CatalogCollector, error) {
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	envName := "GORDIOS_DISABLE_" + stringsUpper(product.ID)
	if os.Getenv(envName) == "1" {
		return nil, fmt.Errorf("disabled via %s=1", envName)
	}
	return &CatalogCollector{
		pool:    pool,
		product: product,
	}, nil
}

func (c *CatalogCollector) ID() string               { return c.product.ID }
func (c *CatalogCollector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *CatalogCollector) Fetch(ctx context.Context) ([]events.Event, error) {
	_ = ctx
	_ = c.pool
	now := time.Now().UTC()
	props := map[string]any{
		"source_dataset":    c.product.Name,
		"source_role":       c.product.Role,
		"source_endpoint":   c.product.Endpoint,
		"product_url":       c.product.ProductURL,
		"resolution":        c.product.Resolution,
		"dataset_year":      c.product.DatasetYear,
		"integration_state": c.product.IntegrationState,
		"license":           c.product.License,
	}
	return []events.Event{{
		Ts:     now,
		Source: c.product.ID,
		ExtID:  fmt.Sprintf("%s:%d", c.product.ID, c.product.DatasetYear),
		Props:  props,
	}}, nil
}

func stringsUpper(s string) string {
	out := make([]byte, len(s))
	for i := range s {
		b := s[i]
		if b >= 'a' && b <= 'z' {
			b -= 'a' - 'A'
		}
		out[i] = b
	}
	return string(out)
}
