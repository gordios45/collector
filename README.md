# Gordios45 Collector

Gordios45 Collector ingests external sources and exposes the raw-data gateway. It owns the
`raw` database schema: source metadata, raw events, raw features, ingestion AOIs,
ingest runs, and H3 projections.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the ingestion, runtime, database,
and gateway design.

## Getting Started

Requirements:

- Go 1.26
- Docker with Docker Compose
- `curl`; `jq` is useful for examples

Start from a fresh local Docker database:

```sh
make docker-reset
make docker-up
make smoke
```

This creates a clearly named local test stack:

- database container: `gordios-collection-test-db`
- database: `gordios_collection_test`
- volume: `gordios_collection_test_dbdata`
- Postgres port: `16432`
- raw gateway: `http://localhost:18080`

Stop the stack:

```sh
make docker-down
```

Use `make docker-reset` only when you intentionally want to delete the local
test database volume.

## Makefile

- `make docker-up`: build and start Postgres, migrations, gateway, and ingester.
- `make smoke`: wait for gateway health and exercise key raw endpoints.
- `make docker-logs`: follow gateway and ingester logs.
- `make db-up`: start only the isolated test Postgres.
- `make migrate`: apply `migrations/*.sql` to the test DB.
- `make psql`: open `psql` in `gordios-collection-test-db`.
- `make build`: build collection binaries into `bin/`.
- `make test`, `make vet`, `make quality`: standard checks.

## Configuration

Copy `.env.example` to `.env` only when you need local overrides.

The Docker test stack creates local-only database roles from
`docker/init/00-local-roles.sql`. Production deployments should create roles and
credentials outside the collection migration, then run `migrations/*.sql`.

## API Keys

The base Docker smoke test and the examples below do not require API keys.

Optional collectors use these credentials or enable flags:

| Env var | Collector(s) |
| --- | --- |
| `ACLED_EMAIL`, `ACLED_PASSWORD` | `acled` |
| `CF_RADAR_TOKEN` | `cloudflare_radar` |
| `OPENAQ_KEY` | `openaq` |
| `PURPLEAIR_API_KEY` | `purpleair` |
| `UCDP_ACCESS_TOKEN` | `ucdp` |
| `NVD_API_KEY` | optional NVD enrichment for `cisa_kev` |
| `FIRMS_MAP_KEY` | `geothermal`; main `firms` uses keyless public mirrors |
| `FAA_SDR_YEAR`, `FAA_SDR_MAX_ROWS` | optional FAA SDR year and row-limit overrides |
| `AIRLINE_REPORT_URLS` | optional configured airline report monitor URLs |
| `AISSTREAM_KEY` | `maritime` stream |
| `GLOBALPING_TOKEN` | optional Globalping API access |
| `OPENSKY_USER`, `OPENSKY_PASS` | optional OpenSky credentials for `flights` |
| `CDSE_ACCESS_TOKEN` or `CDSE_REFRESH_TOKEN` or `CDSE_USERNAME`/`CDSE_PASSWORD` | Copernicus/CDSE-backed collectors |
| `SPACETRACK_USER`, `SPACETRACK_PASS` | `spacetrack` |
| `ENTSOE_TOKEN` | `entsoe_outage` |
| `GFW_TOKEN` | `gfw_events` |
| `EIA_API_KEY` | `eia930` |
| `NOTAM_CLIENT_ID`, `NOTAM_CLIENT_SECRET` | `notam_faa` |
| `RELIEFWEB_APPNAME` | preferred ReliefWeb API identification |
| `GTFS_RT_*` | configured GTFS realtime feeds |
| `GTFS_CATALOG_MAX_FEEDS` | optional MobilityData GTFS-RT catalog discovery limit |
| `OPEN511_FEEDS`, `DATEX2_FEEDS` | optional road traffic-flow feed overrides |
| `NETWORK_RAIL_USER`, `NETWORK_RAIL_PASS` | `network_rail_train_movements` |
| `NOAA_COOPS_*`, `NOAA_COASTAL_FORECAST_*` | optional NOAA port/coastal forecast tuning |
| `PEDESTRIAN_COUNTS_*` | optional pedestrian counter tuning |
| `MARINECADASTRE_AIS_*` | optional MarineCadastre AIS catalog tuning |
| `BMKG_*`, `MALAYSIA_WEATHER_WARNINGS_*`, `THAILAND_TMD_*`, `SINGAPORE_REALTIME_*`, `PAGASA_PHIVOLCS_*`, `PHIVOLCS_*`, `ASMC_HAZE_*` | optional Southeast Asia source tuning; no key required by default |
| `GORDIOS_ENABLE_MARTRACK`, `MARTRACK_USER`, `MARTRACK_PASS` | `martrack` |
| `GORDIOS_ENABLE_MARITIME_STREAM` | `maritime` stream |

See [SOURCES.md](SOURCES.md) for source coverage and source-data
terms.

## Importers

Static/background feature loading is explicit:

```sh
go run ./cmd/importer features -list
go run ./cmd/importer features -cache-dir=.cache/features chokepoints nuclear_facilities power_plants
go run ./cmd/importer features -refresh power_plants
go run ./cmd/importer country-boundaries
go run ./cmd/importer aircraft-registry
go run ./cmd/importer carrier-advisories --dir=../tmp_resources
```

Remote importers cache downloads and use `ETag`/`Last-Modified` validators when
upstreams provide them. If an upstream has no validators, the importer keeps the
cached copy and logs which cache file to remove; pass `-refresh` for a fresh
download. Source-data terms are listed in [SOURCES.md](SOURCES.md).

## Gateway Examples

After `make docker-up && make smoke`, these keyless examples should work.

Source health:

```sh
curl -s http://localhost:18080/api/sources/status \
  | jq '.sources[] | select(.id=="rf_presence" or .id=="military" or .id=="gdacs") | {id:.id,status:.status,last_ok_at:.last_ok_at,last_err:.last_err}'
```

Recent RF presence bins:

```sh
curl -s 'http://localhost:18080/api/latest?source=rf_presence&max_age_min=10080&limit=5' \
  | jq '.assets[] | {ts:.ts,lat:.lat,lon:.lon,network:.props.network,role:.props.role,spots:.props.spots,h3_r4:.h3_r4}'
```

Recent public ADS-B military snapshots:

```sh
curl -s 'http://localhost:18080/api/latest?source=military&max_age_min=10080&limit=5' \
  | jq '.assets[] | {ts:.ts,flight:.props.flight,hex:.props.hex,type:.props.t,lat:.lat,lon:.lon,h3_r4:.h3_r4}'
```

Camera inventory features:

```sh
curl -s 'http://localhost:18080/api/features?source=cctv_cameras' \
  | jq '.features[0:5][] | {id:.id,provider:.properties.source_provider,h3_r4:.properties._h3_r4,stream:(.properties.stream_url != null)}'
```

AOI configuration:

```sh
curl -s http://localhost:18080/api/ingestion/aois \
  | jq '.aois[] | {id:.id,label:.label,collectors:.collectors,metadata:.metadata}'

curl -s -X POST http://localhost:18080/api/ingestion/aois \
  -H 'Content-Type: application/json' \
  -d '{"id":"test-sicily","label":"Sicily test AOI","kind":"test","lat":37.5,"lon":14.0,"radius_m":50000,"collectors":["tropomi_no2","black_marble_nightlights"],"metadata":{"purpose":"local smoke test"}}'

curl -s -X DELETE http://localhost:18080/api/ingestion/aois/test-sicily
```

## Data Rules

The collection DB starts with schema plus migration-seeded AOI metadata only.
Raw event and feature rows are created by live collectors or explicit importer
commands. Local seed files are documented in [data/README.md](data/README.md).
