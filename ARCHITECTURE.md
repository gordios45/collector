# Architecture

Gordios45 Collector is the raw-data ingestion module. It owns external source
collection, raw storage, source freshness metadata, H3 projections, AOI
configuration, explicit background importers, and the raw gateway API.

It does not own signal/candidate generation, analyst scenarios, UI rendering, or
media proxying.

## System Shape

```text
External feeds, APIs, streams, seed files
        |
        v
cmd/ingester
  - scheduled collectors
  - feature collectors
  - streaming collectors
        |
        v
internal/events.Event or internal/features.Feature
        |
        v
raw schema in Postgres/Timescale/PostGIS/H3
  - sources
  - source_ingest_runs
  - events
  - features
  - event_h3_bins
  - ingestion_aois
  - lookup/import tables
        |
        +--> pg_notify('events_changed', source)
        |
        v
cmd/gateway
  - raw REST API
  - source health
  - AOI CRUD
  - WebSocket refresh hints
        |
        v
Raw consumers
  - (External) UI reads raw events/features
  - (External) AI reads raw tables, but writes its own space
```

The database is the contract between collection and downstream systems.
Collection writes only collection-owned raw tables. Downstream systems may read
those raw tables, but should not mutate them.

## Binaries

### `cmd/ingester`

`cmd/ingester` is the long-running ingestion process.

Responsibilities:

- Opens the database using `DATABASE_URL` or `INGESTER_DB_URL`.
- Registers collectors from `internal/collectors`.
- Marks sources enabled or skipped in `raw.sources`.
- Runs cadence-based collectors through `internal/sources.Scheduler`.
- Runs streaming collectors in separate goroutines.
- Writes events, features, source run metrics, and H3 aggregate bins.
- Emits `pg_notify('events_changed', source)` after successful writes.

The ingester does not serve HTTP and does not generate candidates.

### `cmd/gateway`

`cmd/gateway` is the raw-data API server.

Responsibilities:

- Opens the database using `DATABASE_URL` or `RAW_GATEWAY_DB_URL`.
- Serves health and readiness endpoints.
- Serves raw events, raw features, source status, lookup tables, and AOIs.
- Listens to `events_changed` notifications and exposes WebSocket refresh hints.
- Allows CRUD on `raw.ingestion_aois`.

The gateway intentionally does not expose signal/candidate endpoints. Those
belong to the signal service.

### `cmd/importer`

`cmd/importer` contains explicit one-shot import jobs for background data:

- `features`: local and remote static feature seeds.
- `country-boundaries`: Natural Earth country boundaries.
- `aircraft-registry`: OpenSky aircraft registry CSV.
- `carrier-advisories`: carrier advisory workbooks.

Importers are operational tools. They are not hidden startup side effects. If a
fresh database needs static context, run the relevant importer explicitly.

Remote feature importers use `internal/fetchcache` so repeated runs do not
unnecessarily download the same upstream resource.

## Collector Interfaces

Collection has three ingestion shapes.

### Event collectors

Defined by `internal/sources.Collector`:

```go
type Collector interface {
	ID() string
	PollEvery() time.Duration
	Fetch(ctx context.Context) ([]events.Event, error)
}
```

Event collectors produce append-style observations. Examples include seismic
events, alerts, aircraft snapshots, weather products, cyber indicators, and
source availability monitors.

The normalized event shape is intentionally small:

```go
type Event struct {
	Ts     time.Time
	Source string
	ExtID  string
	Lat    float64
	Lon    float64
	Geom   string
	Props  map[string]any
}
```

`Source` is the internal source ID. `ExtID` is the upstream/native identifier.
`Props` preserves source-native fields for provenance and display.

If `Geom` is empty, insert code builds a point from `Lat` and `Lon`. If `Geom`
is present, it is expected to be WKT in EPSG:4326.

### Feature collectors

Defined by `internal/sources.FeatureCollector`:

```go
type FeatureCollector interface {
	ID() string
	PollEvery() time.Duration
	FetchFeatures(ctx context.Context) ([]features.Feature, error)
}
```

Feature collectors maintain inventories or static-ish geospatial context. The
main example is `cctv_cameras`, which writes source-backed camera features.

Feature rows are keyed by `(source, ext_id)` and represent latest-known state,
not an append-only timeline.

### Streaming collectors

Streaming collectors maintain long-lived upstream connections and push into
`EventSink` or `FeatureSink`.

Current examples include:

- `lightning`: Blitzortung event stream.
- `bluesky`: Jetstream feed-post stream.
- `wikipedia_surge`: Wikimedia recent-change stream.
- `ripe_ris`: RIPE RIS Live BGP stream.
- `maritime`: optional AIS stream when enabled.

Sinks batch writes by size or time, then update source run metadata and emit one
notification per flush.

## Scheduler And Writes

`internal/sources.Scheduler` runs one goroutine per registered scheduled
collector. Startup is staggered deterministically by source ID to avoid a large
fetch burst after restart.

For each collector tick:

1. Fetch with a bounded timeout.
2. Record errors in `raw.sources.last_err` and `raw.source_ingest_runs`.
3. Insert events in chunks.
4. Maintain H3 aggregate bins for inserted rows.
5. Mark the source healthy if freshness checks pass.
6. Emit `pg_notify('events_changed', source)` if rows were inserted.

Event idempotency is enforced by:

```text
UNIQUE (source, ext_id, ts)
```

Retransmitted observations are ignored by `ON CONFLICT DO NOTHING`, and H3 bin
counts are only updated for rows that were actually inserted.

Feature writes use two semantics:

- `features.Upsert`: replace the complete feature set for a source.
- `features.UpsertBatch`: update only the provided `(source, ext_id)` rows.

## Database Ownership

The collection migration creates and owns the `raw` schema.

Main raw tables:

| Table | Purpose |
| --- | --- |
| `raw.sources` | Source registry, cadence, enabled state, freshness config, last status. |
| `raw.source_ingest_runs` | Per-fetch/per-flush run metrics and errors. |
| `raw.events` | Append-style raw events and observations. |
| `raw.features` | Latest-state geospatial features and inventories. |
| `raw.event_h3_bins` | Compact source/H3/time-bin event counts. |
| `raw.ingestion_aois` | AOI metadata used by AOI-aware collectors. |
| `raw.aircraft_registry` | Imported aircraft lookup data. |
| `raw.carrier_advisories` | Imported carrier advisory data. |
| `raw.country_boundaries` | Imported country boundary polygons. |
| `raw.sanctioned_entities` | Imported sanctions lookup data. |
| `raw.kev_enrichment_cache` | Cache for vulnerability enrichment payloads. |

The migration also creates compatibility views in `public` for the raw tables.
This keeps older SQL paths working while the authoritative ownership is `raw`.

Database roles:

| Role | Intended access |
| --- | --- |
| `gordios_ingester` | Read/write collection-owned raw tables and sequences. |
| `gordios_raw_gateway` | Read raw tables; insert/update/delete only AOI rows. |

Production should create roles and credentials outside the migration. The
Docker test stack creates local roles for developer convenience.

## H3 Projection

Collection maintains two H3 projections.

### Row-level `h3_r4`

`raw.events` and `raw.features` both have an `h3_r4` column. Database triggers
compute it from the geometry centroid at H3 resolution 4.

This is used for efficient gateway and SQL filtering:

```text
events_h3_r4_source_ts_idx
features_h3_r4_source_idx
```

For polygon rows, `h3_r4` is the centroid cell, not full polygon coverage.

### Aggregate `event_h3_bins`

`raw.event_h3_bins` is maintained in Go during event insertion. It groups newly
inserted point events by:

```text
source, h3_res, h3_cell, 15-minute bin_start
```

The current ingester writes resolution 4 bins. The table is shaped to allow
additional resolutions later without changing the primary key.

This table is a raw collection projection. It exists so downstream consumers can
query coarse activity baselines without scanning raw event rows.

## AOIs

Collection is not globally constrained by AOIs. Many collectors ingest global or
source-native feeds. AOIs are used by AOI-aware collectors that need bounded
queries or expensive geospatial sampling.

AOIs live in `raw.ingestion_aois` and include:

- stable ID
- label
- kind
- lat/lon
- optional radius
- priority
- collector allow-list
- metadata
- enabled flag

The gateway exposes:

```text
GET    /api/ingestion/aois
GET    /api/ingestion/aois/{id}
POST   /api/ingestion/aois
PUT    /api/ingestion/aois/{id}
DELETE /api/ingestion/aois/{id}
```

Collectors that use AOIs should read configured AOIs from the database at fetch
time. This keeps AOI configuration operational rather than compiled into the
collector binary.

## Gateway API

The raw gateway is intentionally narrow: it exposes collection-owned raw data
and raw metadata.

Important endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /readyz` | Process readiness. |
| `GET /healthz` | DB, Timescale, and source freshness health. |
| `GET /api/sources` | Registered sources. |
| `GET /api/sources/status` | Source health, cadence, freshness, and recent runs. |
| `GET /api/latest` | Latest raw event per `ext_id` for a source. |
| `GET /api/events/geojson` | Raw event GeoJSON output. |
| `GET /api/features` | Raw feature GeoJSON output. |
| `GET /api/bgp/snapshot` | BGP visibility snapshot. |
| `GET /api/sanctions/lookup` | Sanctions lookup. |
| `GET /api/carriers` | Carrier advisory list. |
| `GET /api/carriers/lookup` | Carrier lookup. |
| `GET /api/military/lookup` | Military callsign lookup. |
| `GET /api/aircraft/lookup` | Aircraft registry lookup. |
| `GET /stream` | WebSocket refresh hints. |

`/stream` is not a durable event bus. It tells clients that a source changed so
they can refetch via REST. Consumers must tolerate missed notifications and use
polling/recovery when correctness matters.

## Freshness And Health

Every fetch or streaming flush writes a `raw.source_ingest_runs` row. This gives
source-level operational history:

- start and finish time
- success or failure
- rows fetched
- rows inserted
- payload byte estimate
- duration
- error text

`raw.sources` stores current status fields:

- `last_fetch_at`
- `last_ok_at`
- `last_err`
- freshness contract settings

`/healthz` evaluates freshness contracts for enabled sources. Required
freshness violations make health fail. Degraded freshness violations are
reported but do not make the gateway unavailable.

## Source Data And Licensing

The code is Apache-2.0. Upstream data is not automatically Apache-2.0.

`docs/SOURCES.md` is the source catalog and source-data terms inventory. It
records:

- internal source IDs
- upstream/source names
- descriptions
- optional credentials
- importer origins
- verified source-data license or terms where known

Blank license fields are intentional. They mean the source has not been given a
verified redistribution claim in this repository.

## Adding A Source

Minimal source addition checklist:

1. Create a package under `internal/collectors/<source_id>`.
2. Implement `sources.Collector`, `sources.FeatureCollector`, or a streaming
   collector with a sink.
3. Preserve upstream-native fields in `Props`.
4. Use stable `Source` and `ExtID` values.
5. Register the collector in `cmd/ingester`.
6. Add required credentials or enable flags to `.env.example` and `README.md`
   if needed.
7. Add the source to `docs/SOURCES.md`.
8. Add tests for parsing, event shape, and edge cases.
9. Run `go test ./...`.

Source IDs should be stable once public. They are part of the database and API
contract.
