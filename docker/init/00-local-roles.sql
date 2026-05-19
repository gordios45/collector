-- Local Docker bootstrap only. Production deployments should create roles and
-- credentials outside the collection schema migration.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_ingester') THEN
    CREATE ROLE gordios_ingester LOGIN PASSWORD 'gordios_ingester';
  ELSE
    ALTER ROLE gordios_ingester LOGIN PASSWORD 'gordios_ingester';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gordios_raw_gateway') THEN
    CREATE ROLE gordios_raw_gateway LOGIN PASSWORD 'gordios_raw_gateway';
  ELSE
    ALTER ROLE gordios_raw_gateway LOGIN PASSWORD 'gordios_raw_gateway';
  END IF;
END
$$;
