-- Collection-owned database bootstrap and contract.
--
-- Collection owns raw ingestion tables, AOI configuration, source run metadata,
-- raw H3 projections, and the raw-facing roles.

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis_raster WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS h3 WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS h3_postgis WITH SCHEMA public;

CREATE SCHEMA IF NOT EXISTS raw;

DO $$
DECLARE
  dbname text := current_database();
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_ingester') THEN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO gordios_ingester', dbname);
    EXECUTE format('ALTER ROLE gordios_ingester IN DATABASE %I SET search_path = raw, public', dbname);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_raw_gateway') THEN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO gordios_raw_gateway', dbname);
    EXECUTE format('ALTER ROLE gordios_raw_gateway IN DATABASE %I SET search_path = raw, public', dbname);
  END IF;
END
$$;

-- Move collection tables to raw.
DO $$
DECLARE
  rel text;
  relkind "char";
BEGIN
  FOREACH rel IN ARRAY ARRAY[
    'aircraft_registry',
    'carrier_advisories',
    'country_boundaries',
    'event_h3_bins',
    'events',
    'events_of_interest',
    'features',
    'icao24_country_ranges',
    'ingestion_aois',
    'kev_enrichment_cache',
    'military_callsigns',
    'sanctioned_entities',
    'source_ingest_runs',
    'sources'
  ] LOOP
    SELECT c.relkind INTO relkind
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = rel;

    IF relkind IN ('r', 'p') THEN
      IF to_regclass(format('raw.%I', rel)) IS NOT NULL THEN
        RAISE EXCEPTION 'cannot move public.%: raw.% already exists', rel, rel;
      END IF;
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA raw', rel);
    ELSIF relkind IS NOT NULL AND relkind <> 'v' THEN
      RAISE EXCEPTION 'public.% exists with unsupported relkind %', rel, relkind;
    END IF;
  END LOOP;

  FOREACH rel IN ARRAY ARRAY[
    'events_of_interest_id_seq',
    'source_ingest_runs_id_seq'
  ] LOOP
    SELECT c.relkind INTO relkind
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = rel;

    IF relkind = 'S' THEN
      IF to_regclass(format('raw.%I', rel)) IS NOT NULL THEN
        RAISE EXCEPTION 'cannot move public.%: raw.% already exists', rel, rel;
      END IF;
      EXECUTE format('ALTER SEQUENCE public.%I SET SCHEMA raw', rel);
    ELSIF relkind IS NOT NULL THEN
      RAISE EXCEPTION 'public.% exists with unsupported relkind %', rel, relkind;
    END IF;
  END LOOP;
END
$$;

CREATE TABLE IF NOT EXISTS raw.sources (
  id text NOT NULL,
  kind text NOT NULL,
  poll_every_s integer,
  enabled boolean DEFAULT true NOT NULL,
  config jsonb DEFAULT '{}'::jsonb NOT NULL,
  last_fetch_at timestamptz,
  last_ok_at timestamptz,
  last_err text,
  freshness_contract_enabled boolean DEFAULT false NOT NULL,
  expected_min_rows_per_window integer DEFAULT 0 NOT NULL,
  expected_window interval,
  expected_max_lag interval,
  freshness_contract_severity text DEFAULT 'degraded'::text NOT NULL,
  freshness_contract_note text
);

ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'event';
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS poll_every_s integer;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS enabled boolean DEFAULT true NOT NULL;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS config jsonb DEFAULT '{}'::jsonb NOT NULL;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS last_fetch_at timestamptz;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS last_ok_at timestamptz;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS last_err text;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS freshness_contract_enabled boolean DEFAULT false NOT NULL;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS expected_min_rows_per_window integer DEFAULT 0 NOT NULL;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS expected_window interval;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS expected_max_lag interval;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS freshness_contract_severity text DEFAULT 'degraded' NOT NULL;
ALTER TABLE raw.sources ADD COLUMN IF NOT EXISTS freshness_contract_note text;

CREATE TABLE IF NOT EXISTS raw.events (
  ts timestamptz NOT NULL,
  source text NOT NULL,
  ext_id text DEFAULT '' NOT NULL,
  geom public.geography(Geometry,4326),
  h3_r4 text,
  props jsonb DEFAULT '{}'::jsonb NOT NULL,
  ingested_at timestamptz DEFAULT now()
);

ALTER TABLE raw.events ADD COLUMN IF NOT EXISTS h3_r4 text;
ALTER TABLE raw.events ADD COLUMN IF NOT EXISTS ingested_at timestamptz;
ALTER TABLE raw.events ALTER COLUMN ingested_at SET DEFAULT now();

CREATE OR REPLACE FUNCTION raw.gordios_events_set_h3_r4() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.geom IS NULL OR public.ST_IsEmpty((NEW.geom)::public.geometry) THEN
    NEW.h3_r4 := NULL;
  ELSE
    NEW.h3_r4 := public.h3_latlng_to_cell(public.ST_Centroid((NEW.geom)::public.geometry), 4)::text;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS events_set_h3_r4_before_write ON raw.events;
CREATE TRIGGER events_set_h3_r4_before_write
  BEFORE INSERT OR UPDATE OF geom ON raw.events
  FOR EACH ROW EXECUTE FUNCTION raw.gordios_events_set_h3_r4();

SELECT public.create_hypertable('raw.events', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS raw.event_h3_bins (
  source text NOT NULL,
  h3_res integer NOT NULL,
  h3_cell text NOT NULL,
  bin_start timestamptz NOT NULL,
  events_count integer DEFAULT 0 NOT NULL,
  sample_lat double precision,
  sample_lon double precision,
  updated_at timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.features (
  source text NOT NULL,
  ext_id text NOT NULL,
  geom public.geography(Geometry,4326),
  h3_r4 text,
  props jsonb DEFAULT '{}'::jsonb NOT NULL,
  updated_at timestamptz DEFAULT now() NOT NULL
);

ALTER TABLE raw.features ADD COLUMN IF NOT EXISTS h3_r4 text;

CREATE OR REPLACE FUNCTION raw.gordios_features_set_h3_r4() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.geom IS NULL OR public.ST_IsEmpty((NEW.geom)::public.geometry) THEN
    NEW.h3_r4 := NULL;
  ELSE
    NEW.h3_r4 := public.h3_latlng_to_cell(public.ST_Centroid((NEW.geom)::public.geometry), 4)::text;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS features_set_h3_r4_before_write ON raw.features;
CREATE TRIGGER features_set_h3_r4_before_write
  BEFORE INSERT OR UPDATE OF geom ON raw.features
  FOR EACH ROW EXECUTE FUNCTION raw.gordios_features_set_h3_r4();

UPDATE raw.features
   SET h3_r4 = public.h3_latlng_to_cell(public.ST_Centroid((geom)::public.geometry), 4)::text
 WHERE geom IS NOT NULL
   AND h3_r4 IS NULL;

CREATE TABLE IF NOT EXISTS raw.source_ingest_runs (
  id bigserial,
  source_id text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz DEFAULT now() NOT NULL,
  ok boolean NOT NULL,
  rows_fetched integer DEFAULT 0 NOT NULL,
  rows_inserted integer DEFAULT 0 NOT NULL,
  payload_bytes bigint DEFAULT 0 NOT NULL,
  duration_ms integer DEFAULT 0 NOT NULL,
  error text
);

CREATE TABLE IF NOT EXISTS raw.ingestion_aois (
  id text NOT NULL,
  label text NOT NULL,
  kind text DEFAULT 'manual' NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  priority double precision DEFAULT 1 NOT NULL,
  radius_m double precision,
  collectors text[] DEFAULT '{}'::text[] NOT NULL,
  metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
  enabled boolean DEFAULT true NOT NULL,
  created_at timestamptz DEFAULT now() NOT NULL,
  updated_at timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.aircraft_registry (
  icao24 text NOT NULL,
  registration text,
  manufacturer_icao text,
  manufacturer_name text,
  model text,
  typecode text,
  serial_number text,
  icao_aircraft_type text,
  operator text,
  operator_callsign text,
  operator_icao text,
  operator_iata text,
  owner text,
  built text,
  status text,
  notes text,
  source text DEFAULT 'opensky' NOT NULL,
  imported_at timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.carrier_advisories (
  id uuid NOT NULL,
  version text,
  name text NOT NULL,
  iata text,
  icao text,
  country text,
  rating text,
  operational_status text,
  summary text,
  description_en text,
  notes text,
  last_updated_at timestamptz,
  region text,
  content jsonb DEFAULT '{}'::jsonb NOT NULL,
  imported_at timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.country_boundaries (
  id text NOT NULL,
  iso_a2 text,
  iso_a3 text,
  name text NOT NULL,
  sovereign text,
  region text,
  subregion text,
  source text DEFAULT 'natural_earth_admin0' NOT NULL,
  updated_at timestamptz DEFAULT now() NOT NULL,
  geom public.geometry(MultiPolygon,4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.events_of_interest (
  id integer GENERATED BY DEFAULT AS IDENTITY,
  slug text NOT NULL,
  name text NOT NULL,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  region text,
  bbox public.geography(Polygon,4326),
  description text,
  sources text[] DEFAULT '{}'::text[],
  created_at timestamptz DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.icao24_country_ranges (
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  country_code text,
  country_name text NOT NULL,
  notes text
);

CREATE TABLE IF NOT EXISTS raw.kev_enrichment_cache (
  cve text NOT NULL,
  nvd jsonb,
  nvd_at timestamptz,
  vulnrichment jsonb,
  vuln_at timestamptz
);

CREATE TABLE IF NOT EXISTS raw.military_callsigns (
  prefix text NOT NULL,
  operator text NOT NULL,
  country_code text,
  role text,
  aircraft_types text[] DEFAULT '{}'::text[],
  notes text
);

CREATE TABLE IF NOT EXISTS raw.sanctioned_entities (
  list text NOT NULL,
  ref_id text NOT NULL,
  kind text NOT NULL,
  name text NOT NULL,
  aliases text[] DEFAULT '{}'::text[] NOT NULL,
  programs text[] DEFAULT '{}'::text[] NOT NULL,
  identifiers jsonb DEFAULT '{}'::jsonb NOT NULL,
  updated_at timestamptz DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS sources_id_uidx ON raw.sources (id);
CREATE UNIQUE INDEX IF NOT EXISTS events_source_extid_ts_uidx ON raw.events (source, ext_id, ts);
CREATE INDEX IF NOT EXISTS events_h3_r4_source_ts_idx ON raw.events (h3_r4, source, ts DESC) WHERE h3_r4 IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_geom_gix ON raw.events USING gist (geom);
CREATE INDEX IF NOT EXISTS events_props_gin ON raw.events USING gin (props);
CREATE INDEX IF NOT EXISTS events_source_ingested_at_idx ON raw.events (source, ingested_at DESC);
CREATE INDEX IF NOT EXISTS events_source_ts_geom_gix ON raw.events USING gist (source, ts, geom) WHERE geom IS NOT NULL;
CREATE INDEX IF NOT EXISTS events_source_ts_idx ON raw.events (source, ts DESC);
CREATE INDEX IF NOT EXISTS events_ts_idx ON raw.events (ts DESC);
CREATE UNIQUE INDEX IF NOT EXISTS event_h3_bins_uidx ON raw.event_h3_bins (source, h3_res, h3_cell, bin_start);
CREATE INDEX IF NOT EXISTS event_h3_bins_cell_time_idx ON raw.event_h3_bins (h3_res, h3_cell, bin_start DESC);
CREATE INDEX IF NOT EXISTS event_h3_bins_source_time_idx ON raw.event_h3_bins (source, bin_start DESC);
CREATE UNIQUE INDEX IF NOT EXISTS features_source_extid_uidx ON raw.features (source, ext_id);
CREATE INDEX IF NOT EXISTS features_h3_r4_source_idx ON raw.features (h3_r4, source) WHERE h3_r4 IS NOT NULL;
CREATE INDEX IF NOT EXISTS features_source_geom_gix ON raw.features USING gist (source, geom) WHERE geom IS NOT NULL;
CREATE INDEX IF NOT EXISTS features_source_updated_geom_gix ON raw.features USING gist (source, updated_at, geom) WHERE geom IS NOT NULL;
CREATE INDEX IF NOT EXISTS features_source_updated_idx ON raw.features (source, updated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS source_ingest_runs_id_uidx ON raw.source_ingest_runs (id);
CREATE INDEX IF NOT EXISTS source_ingest_runs_source_started_idx ON raw.source_ingest_runs (source_id, started_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ingestion_aois_id_uidx ON raw.ingestion_aois (id);
CREATE UNIQUE INDEX IF NOT EXISTS aircraft_registry_icao24_uidx ON raw.aircraft_registry (icao24);
CREATE UNIQUE INDEX IF NOT EXISTS carrier_advisories_id_uidx ON raw.carrier_advisories (id);
CREATE UNIQUE INDEX IF NOT EXISTS country_boundaries_id_uidx ON raw.country_boundaries (id);
CREATE INDEX IF NOT EXISTS country_boundaries_geom_gix ON raw.country_boundaries USING gist (geom);
CREATE UNIQUE INDEX IF NOT EXISTS events_of_interest_id_uidx ON raw.events_of_interest (id);
CREATE UNIQUE INDEX IF NOT EXISTS events_of_interest_slug_uidx ON raw.events_of_interest (slug);
CREATE INDEX IF NOT EXISTS events_of_interest_time_idx ON raw.events_of_interest (started_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS icao24_country_ranges_range_start_uidx ON raw.icao24_country_ranges (range_start);
CREATE UNIQUE INDEX IF NOT EXISTS kev_enrichment_cache_cve_uidx ON raw.kev_enrichment_cache (cve);
CREATE UNIQUE INDEX IF NOT EXISTS military_callsigns_prefix_uidx ON raw.military_callsigns (prefix);
CREATE UNIQUE INDEX IF NOT EXISTS sanctioned_entities_list_ref_uidx ON raw.sanctioned_entities (list, ref_id);

-- Seed AOIs are collection metadata. Event/feature rows remain collector- or
-- explicit importer-owned.
INSERT INTO raw.ingestion_aois
  (id, label, kind, lat, lon, priority, radius_m, collectors, metadata, enabled)
VALUES
  ('suez_canal', 'Suez Canal', 'maritime_chokepoint', 30.55, 32.34, 0.95, 60000,
   ARRAY['maritime','portwatch_disruptions','portwatch_port_activity','rf_presence'],
   '{"seed":"collection_migration","region":"egypt","reason":"major maritime chokepoint"}'::jsonb, TRUE),
  ('bab_al_mandab', 'Bab al-Mandab', 'maritime_chokepoint', 12.63, 43.32, 1.0, 85000,
   ARRAY['maritime','portwatch_disruptions','portwatch_port_activity','rf_presence'],
   '{"seed":"collection_migration","region":"red_sea","reason":"major maritime chokepoint"}'::jsonb, TRUE),
  ('strait_of_hormuz', 'Strait of Hormuz', 'maritime_chokepoint', 26.58, 56.25, 1.0, 90000,
   ARRAY['maritime','portwatch_disruptions','portwatch_port_activity','rf_presence'],
   '{"seed":"collection_migration","region":"persian_gulf","reason":"energy transit chokepoint"}'::jsonb, TRUE),
  ('taiwan_strait', 'Taiwan Strait', 'regional_watch', 24.0, 119.5, 0.9, 150000,
   ARRAY['maritime','flights','adsb_lol','rf_presence','satnogs'],
   '{"seed":"collection_migration","region":"east_asia","reason":"shipping and air activity watch"}'::jsonb, TRUE),
  ('panama_canal', 'Panama Canal', 'maritime_chokepoint', 9.08, -79.68, 0.85, 60000,
   ARRAY['maritime','portwatch_disruptions','portwatch_port_activity'],
   '{"seed":"collection_migration","region":"central_america","reason":"major maritime chokepoint"}'::jsonb, TRUE),
  ('turkish_straits', 'Turkish Straits', 'maritime_chokepoint', 41.12, 29.06, 0.8, 90000,
   ARRAY['maritime','portwatch_disruptions','portwatch_port_activity','rf_presence'],
   '{"seed":"collection_migration","region":"black_sea","reason":"major maritime chokepoint"}'::jsonb, TRUE)
ON CONFLICT (id) DO NOTHING;

INSERT INTO raw.sources
  (id, kind, poll_every_s, enabled, config, freshness_contract_enabled,
   expected_min_rows_per_window, expected_window, expected_max_lag,
   freshness_contract_severity, freshness_contract_note)
VALUES
  ('cctv_cameras', 'feature_inventory', 86400, TRUE,
   '{"name":"CCTV camera inventory","sources":["tfl_jamcam","open_traffic_cam_map","camera_seed_file","osm_overpass_aoi"]}'::jsonb,
   FALSE, 0, interval '7 days', interval '7 days', 'degraded',
   'Feature inventory freshness is tracked via source_ingest_runs and sources.last_ok_at.')
ON CONFLICT (id) DO UPDATE
   SET kind = EXCLUDED.kind,
       poll_every_s = EXCLUDED.poll_every_s,
       enabled = TRUE,
       config = EXCLUDED.config;

DO $$
DECLARE
  rel text;
  relkind "char";
BEGIN
  FOREACH rel IN ARRAY ARRAY[
    'aircraft_registry',
    'carrier_advisories',
    'country_boundaries',
    'event_h3_bins',
    'events',
    'events_of_interest',
    'features',
    'icao24_country_ranges',
    'ingestion_aois',
    'kev_enrichment_cache',
    'military_callsigns',
    'sanctioned_entities',
    'source_ingest_runs',
    'sources'
  ] LOOP
    SELECT c.relkind INTO relkind
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = rel;

    IF relkind IS NULL OR relkind = 'v' THEN
      EXECUTE format('CREATE OR REPLACE VIEW public.%I AS SELECT * FROM raw.%I', rel, rel);
    ELSE
      RAISE EXCEPTION 'cannot create compatibility view public.% over raw.% because public object relkind is %', rel, rel, relkind;
    END IF;
  END LOOP;

END
$$;

DO $$
BEGIN
  IF to_regprocedure('public.gordios_prune_chatty_sources(integer, jsonb)') IS NOT NULL THEN
    ALTER PROCEDURE public.gordios_prune_chatty_sources(integer, jsonb)
      SET search_path = raw, public;
  END IF;
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA raw FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA raw FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_ingester') THEN
    GRANT USAGE ON SCHEMA public TO gordios_ingester;
    GRANT USAGE ON SCHEMA raw TO gordios_ingester;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO gordios_ingester;
    GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA raw TO gordios_ingester;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO gordios_ingester;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw
      GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO gordios_ingester;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_raw_gateway') THEN
    GRANT USAGE ON SCHEMA public TO gordios_raw_gateway;
    GRANT USAGE ON SCHEMA raw TO gordios_raw_gateway;
    GRANT SELECT ON ALL TABLES IN SCHEMA raw TO gordios_raw_gateway;
    GRANT INSERT, UPDATE, DELETE ON raw.ingestion_aois TO gordios_raw_gateway;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw
      GRANT SELECT ON TABLES TO gordios_raw_gateway;
  END IF;

  IF to_regclass('public.spatial_ref_sys') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_ingester') THEN
      GRANT SELECT ON public.spatial_ref_sys TO gordios_ingester;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_raw_gateway') THEN
      GRANT SELECT ON public.spatial_ref_sys TO gordios_raw_gateway;
    END IF;
  END IF;
END
$$;
