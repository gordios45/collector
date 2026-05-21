// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Ingester: long-running process that schedules collectors and inserts
// normalized events into the time-series store.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gordios45/collector/internal/collectors/acled"
	"github.com/gordios45/collector/internal/collectors/adsb_lol"
	"github.com/gordios45/collector/internal/collectors/airline_safety"
	"github.com/gordios45/collector/internal/collectors/airnow_alerts"
	"github.com/gordios45/collector/internal/collectors/asean_haze_hotspots"
	"github.com/gordios45/collector/internal/collectors/avalanche_alerts"
	"github.com/gordios45/collector/internal/collectors/bgp_visibility"
	"github.com/gordios45/collector/internal/collectors/bgpstream_broker"
	"github.com/gordios45/collector/internal/collectors/blackmarble"
	"github.com/gordios45/collector/internal/collectors/bluesky"
	"github.com/gordios45/collector/internal/collectors/bmkg_indonesia"
	"github.com/gordios45/collector/internal/collectors/cams_atmosphere"
	"github.com/gordios45/collector/internal/collectors/cctv_cameras"
	"github.com/gordios45/collector/internal/collectors/cems_rapid_mapping"
	"github.com/gordios45/collector/internal/collectors/cisa_kev"
	"github.com/gordios45/collector/internal/collectors/cloudflare_radar"
	"github.com/gordios45/collector/internal/collectors/copernicus_gdo_drought"
	"github.com/gordios45/collector/internal/collectors/copernicus_sentinel"
	"github.com/gordios45/collector/internal/collectors/cyber_threats"
	"github.com/gordios45/collector/internal/collectors/deepstate_frontlines"
	"github.com/gordios45/collector/internal/collectors/direct_weather_alerts"
	"github.com/gordios45/collector/internal/collectors/eia930"
	"github.com/gordios45/collector/internal/collectors/eia_wpsr"
	"github.com/gordios45/collector/internal/collectors/emsc_seismic"
	"github.com/gordios45/collector/internal/collectors/entsoe"
	"github.com/gordios45/collector/internal/collectors/eonet"
	"github.com/gordios45/collector/internal/collectors/epa_radnet"
	"github.com/gordios45/collector/internal/collectors/eurdep"
	"github.com/gordios45/collector/internal/collectors/faa_status"
	"github.com/gordios45/collector/internal/collectors/fews_net_food_security"
	"github.com/gordios45/collector/internal/collectors/firms"
	"github.com/gordios45/collector/internal/collectors/flights"
	"github.com/gordios45/collector/internal/collectors/flood_coverage"
	"github.com/gordios45/collector/internal/collectors/gdacs"
	"github.com/gordios45/collector/internal/collectors/gdelt"
	"github.com/gordios45/collector/internal/collectors/geothermal"
	"github.com/gordios45/collector/internal/collectors/gfw"
	"github.com/gordios45/collector/internal/collectors/global_cap_alerts"
	"github.com/gordios45/collector/internal/collectors/global_disaster_reports"
	"github.com/gordios45/collector/internal/collectors/globalping"
	"github.com/gordios45/collector/internal/collectors/gps_jamming"
	"github.com/gordios45/collector/internal/collectors/gtfs_feed_catalog"
	"github.com/gordios45/collector/internal/collectors/gtfs_realtime"
	"github.com/gordios45/collector/internal/collectors/gwis_fire_danger"
	"github.com/gordios45/collector/internal/collectors/health_outbreaks"
	"github.com/gordios45/collector/internal/collectors/hms_smoke"
	"github.com/gordios45/collector/internal/collectors/iata_fuel_monitor"
	"github.com/gordios45/collector/internal/collectors/ibtracs"
	"github.com/gordios45/collector/internal/collectors/ifrc_go"
	"github.com/gordios45/collector/internal/collectors/inform_risk"
	"github.com/gordios45/collector/internal/collectors/internetdb_exposure"
	"github.com/gordios45/collector/internal/collectors/ioda"
	"github.com/gordios45/collector/internal/collectors/jma_rsmc"
	"github.com/gordios45/collector/internal/collectors/jtwc"
	"github.com/gordios45/collector/internal/collectors/launches"
	"github.com/gordios45/collector/internal/collectors/lhasa_landslide"
	"github.com/gordios45/collector/internal/collectors/lightning"
	"github.com/gordios45/collector/internal/collectors/malaysia_weather_warnings"
	"github.com/gordios45/collector/internal/collectors/maritime"
	"github.com/gordios45/collector/internal/collectors/maritime_baselines"
	"github.com/gordios45/collector/internal/collectors/martrack"
	"github.com/gordios45/collector/internal/collectors/meteoalarm"
	"github.com/gordios45/collector/internal/collectors/military"
	"github.com/gordios45/collector/internal/collectors/ndbc_buoys"
	"github.com/gordios45/collector/internal/collectors/netblocks"
	"github.com/gordios45/collector/internal/collectors/network_rail"
	"github.com/gordios45/collector/internal/collectors/nga_warnings"
	"github.com/gordios45/collector/internal/collectors/nhc"
	"github.com/gordios45/collector/internal/collectors/nhc_gis_cones"
	"github.com/gordios45/collector/internal/collectors/nifc_fire_perimeters"
	"github.com/gordios45/collector/internal/collectors/nifc_wildfires"
	"github.com/gordios45/collector/internal/collectors/noaa_coastal_forecast"
	"github.com/gordios45/collector/internal/collectors/noaa_coops_ports"
	"github.com/gordios45/collector/internal/collectors/noaa_cpc_gth"
	"github.com/gordios45/collector/internal/collectors/noaa_nwm"
	"github.com/gordios45/collector/internal/collectors/noaa_sua"
	"github.com/gordios45/collector/internal/collectors/noaa_tsunami"
	"github.com/gordios45/collector/internal/collectors/notam"
	"github.com/gordios45/collector/internal/collectors/nws_alerts"
	"github.com/gordios45/collector/internal/collectors/nws_sigmet"
	"github.com/gordios45/collector/internal/collectors/official_advisories"
	"github.com/gordios45/collector/internal/collectors/oil_prices"
	"github.com/gordios45/collector/internal/collectors/ooni"
	"github.com/gordios45/collector/internal/collectors/open_meteo_anomalies"
	"github.com/gordios45/collector/internal/collectors/openaq"
	"github.com/gordios45/collector/internal/collectors/opera_dist"
	"github.com/gordios45/collector/internal/collectors/overture"
	"github.com/gordios45/collector/internal/collectors/pagasa_phivolcs_monitor"
	"github.com/gordios45/collector/internal/collectors/pedestrian_counts"
	"github.com/gordios45/collector/internal/collectors/peeringdb"
	"github.com/gordios45/collector/internal/collectors/planned_protests"
	"github.com/gordios45/collector/internal/collectors/portwatch_disruptions"
	"github.com/gordios45/collector/internal/collectors/portwatch_port_activity"
	"github.com/gordios45/collector/internal/collectors/public_facilities"
	"github.com/gordios45/collector/internal/collectors/public_safety"
	"github.com/gordios45/collector/internal/collectors/purpleair"
	"github.com/gordios45/collector/internal/collectors/regional_seismic"
	"github.com/gordios45/collector/internal/collectors/regional_wildfires"
	"github.com/gordios45/collector/internal/collectors/reliefweb"
	"github.com/gordios45/collector/internal/collectors/rf_presence"
	"github.com/gordios45/collector/internal/collectors/ripe_ris"
	"github.com/gordios45/collector/internal/collectors/road_incidents"
	"github.com/gordios45/collector/internal/collectors/road_traffic_flow"
	"github.com/gordios45/collector/internal/collectors/safecast"
	"github.com/gordios45/collector/internal/collectors/sanctions"
	"github.com/gordios45/collector/internal/collectors/satnogs"
	"github.com/gordios45/collector/internal/collectors/saveecobot"
	"github.com/gordios45/collector/internal/collectors/seismic"
	"github.com/gordios45/collector/internal/collectors/sentinel_plan"
	"github.com/gordios45/collector/internal/collectors/singapore_realtime"
	"github.com/gordios45/collector/internal/collectors/space_weather"
	"github.com/gordios45/collector/internal/collectors/spacetrack"
	"github.com/gordios45/collector/internal/collectors/spc_storm_reports"
	"github.com/gordios45/collector/internal/collectors/swdi_radar_signatures"
	"github.com/gordios45/collector/internal/collectors/thailand_tmd_alerts"
	"github.com/gordios45/collector/internal/collectors/tle"
	"github.com/gordios45/collector/internal/collectors/tor_exit_nodes"
	"github.com/gordios45/collector/internal/collectors/tor_metrics"
	"github.com/gordios45/collector/internal/collectors/travel_advisories"
	"github.com/gordios45/collector/internal/collectors/tropomi"
	"github.com/gordios45/collector/internal/collectors/ucdp"
	"github.com/gordios45/collector/internal/collectors/unhcr_displacement"
	"github.com/gordios45/collector/internal/collectors/usgs_shakemap"
	"github.com/gordios45/collector/internal/collectors/utility_outages"
	"github.com/gordios45/collector/internal/collectors/vaac_global"
	"github.com/gordios45/collector/internal/collectors/vaac_tokyo"
	"github.com/gordios45/collector/internal/collectors/vaac_washington"
	"github.com/gordios45/collector/internal/collectors/volcano_notices"
	"github.com/gordios45/collector/internal/collectors/volcanoes"
	"github.com/gordios45/collector/internal/collectors/water_gauges"
	"github.com/gordios45/collector/internal/collectors/wfp_food_prices"
	"github.com/gordios45/collector/internal/collectors/wikipedia"
	"github.com/gordios45/collector/internal/collectors/wikipedia_pageviews"
	"github.com/gordios45/collector/internal/collectors/wmo_alert_hub"
	"github.com/gordios45/collector/internal/collectors/wmo_cap_alert_areas"
	"github.com/gordios45/collector/internal/collectors/worldpop_exposure"
	"github.com/gordios45/collector/internal/db"
	"github.com/gordios45/collector/internal/sources"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Load .env from the repository root or from ./.env if present.
	db.LoadDotEnv("../.env", ".env")
	defaultDatabaseURL("INGESTER_DB_URL")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()
	log.Println("[ingester] db pool ready")

	sch := sources.NewScheduler(pool)
	pollEverySec := func(d time.Duration) any {
		if d < time.Second {
			return nil
		}
		return int(d / time.Second)
	}
	markSourceActiveKind := func(id, kind string, pollEvery time.Duration) {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sources (id, kind, poll_every_s, enabled, config, last_err)
			VALUES ($1, $2, $3, TRUE, '{}'::jsonb, NULL)
			ON CONFLICT (id) DO UPDATE
			   SET kind = EXCLUDED.kind,
			       poll_every_s = COALESCE(EXCLUDED.poll_every_s, sources.poll_every_s),
			       enabled = TRUE,
			       last_err = NULL`, id, kind, pollEverySec(pollEvery)); err != nil {
			log.Printf("[ingester] mark active %s: %v", id, err)
		}
	}
	markSourceActive := func(id string, pollEvery time.Duration) {
		markSourceActiveKind(id, "event", pollEvery)
	}
	markSourceSkippedKind := func(id, kind, reason string) {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sources (id, kind, enabled, config, last_err)
			VALUES ($1, $2, FALSE, '{}'::jsonb, $3)
			ON CONFLICT (id) DO UPDATE
			   SET enabled = FALSE,
			       last_err = EXCLUDED.last_err`, id, kind, reason); err != nil {
			log.Printf("[ingester] mark skipped %s: %v", id, err)
		}
	}
	markSourceSkipped := func(id, reason string) {
		markSourceSkippedKind(id, "event", reason)
	}

	// Martrack: creds in .env currently don't authenticate upstream; skip
	// registration until a working user/pass is confirmed. Code path is
	// intact — flipping GORDIOS_ENABLE_MARTRACK=1 in the env re-enables it.
	if envIsOne("GORDIOS_ENABLE_MARTRACK", "WV_ENABLE_MARTRACK") {
		if mc, err := martrack.New(); err == nil {
			sch.Register(mc)
			markSourceActive(mc.ID(), mc.PollEvery())
			log.Println("[ingester] registered: martrack")
		} else {
			log.Printf("[ingester] martrack disabled: %v", err)
			markSourceSkipped("martrack", err.Error())
		}
	} else {
		msg := "skipped: set GORDIOS_ENABLE_MARTRACK=1 to enable"
		markSourceSkipped("martrack", msg)
		log.Printf("[ingester] martrack %s", msg)
	}

	if fc, err := flights.New(); err == nil {
		sch.Register(fc)
		markSourceActive(fc.ID(), fc.PollEvery())
		log.Println("[ingester] registered: flights")
	} else {
		log.Printf("[ingester] flights disabled: %v", err)
		markSourceSkipped("flights", err.Error())
	}

	reg := func(name string, c sources.Collector, err error) {
		if err != nil {
			log.Printf("[ingester] %s disabled: %v", name, err)
			markSourceSkipped(name, err.Error())
			return
		}
		sch.Register(c)
		markSourceActive(c.ID(), c.PollEvery())
		log.Printf("[ingester] registered: %s", c.ID())
	}
	regFeature := func(name string, c sources.FeatureCollector, err error) {
		if err != nil {
			log.Printf("[ingester] %s disabled: %v", name, err)
			markSourceSkippedKind(name, "feature_inventory", err.Error())
			return
		}
		sch.RegisterFeature(c)
		markSourceActiveKind(c.ID(), "feature_inventory", c.PollEvery())
		log.Printf("[ingester] registered feature source: %s", c.ID())
	}
	{
		c, err := adsb_lol.New()
		reg("adsb_lol", c, err)
	}
	{
		c, err := gtfs_realtime.New()
		reg("gtfs_realtime", c, err)
	}
	{
		c, err := gtfs_feed_catalog.New()
		reg("gtfs_feed_catalog", c, err)
	}
	{
		c, err := road_incidents.New()
		reg("road_incidents", c, err)
	}
	{
		c, err := road_traffic_flow.New()
		reg("road_traffic_flow", c, err)
	}
	{
		c, err := network_rail.New()
		reg("network_rail_train_movements", c, err)
	}
	{
		c, err := utility_outages.New()
		reg("utility_outages", c, err)
	}
	{
		c, err := public_safety.New()
		reg("public_safety_incidents", c, err)
	}
	{
		c, err := pedestrian_counts.New()
		reg("pedestrian_counts", c, err)
	}
	{
		c, err := peeringdb.New(pool)
		reg("peeringdb", c, err)
	}
	{
		c, err := overture.New(pool)
		reg("overture_maps_context", c, err)
	}
	{
		c, err := public_facilities.New(pool)
		reg("public_facilities_context", c, err)
	}
	{
		c, err := cctv_cameras.New(pool)
		regFeature("cctv_cameras", c, err)
	}
	{
		c, err := deepstate_frontlines.New()
		regFeature("deepstate_frontlines", c, err)
	}
	{
		c, err := noaa_sua.New(pool)
		reg("noaa_military_sua", c, err)
	}
	{
		c, err := seismic.New()
		reg("seismic", c, err)
	}
	{
		c, err := emsc_seismic.New()
		reg("emsc_seismic", c, err)
	}
	{
		c, err := regional_seismic.New()
		reg("regional_seismic", c, err)
	}
	{
		c, err := bmkg_indonesia.New()
		reg("bmkg_indonesia", c, err)
	}
	{
		c, err := pagasa_phivolcs_monitor.New()
		reg("pagasa_phivolcs_monitor", c, err)
	}
	{
		c, err := gdacs.New()
		reg("gdacs", c, err)
	}
	{
		c, err := eonet.New()
		reg("eonet", c, err)
	}
	{
		c, err := nws_alerts.New()
		reg("nws_alerts", c, err)
	}
	{
		c, err := meteoalarm.New()
		reg("meteoalarm", c, err)
	}
	{
		c, err := nhc.New()
		reg("nhc", c, err)
	}
	{
		c, err := nhc_gis_cones.New()
		reg("nhc_gis_cones", c, err)
	}
	{
		c, err := jtwc.New()
		reg("jtwc", c, err)
	}
	{
		c, err := ibtracs.New()
		reg("ibtracs", c, err)
	}
	{
		c, err := jma_rsmc.New()
		reg("jma_rsmc", c, err)
	}
	{
		c, err := nifc_wildfires.New()
		reg("nifc_wildfires", c, err)
	}
	{
		c, err := nifc_fire_perimeters.New()
		reg("nifc_fire_perimeters", c, err)
	}
	{
		c, err := regional_wildfires.New()
		reg("regional_wildfires", c, err)
	}
	{
		c, err := usgs_shakemap.New()
		reg("usgs_shakemap", c, err)
	}
	{
		c, err := noaa_tsunami.New()
		reg("noaa_tsunami", c, err)
	}
	{
		c, err := noaa_coops_ports.New()
		reg("noaa_coops_ports", c, err)
	}
	{
		c, err := noaa_coastal_forecast.New()
		reg("noaa_coastal_forecast", c, err)
	}
	{
		c, err := cems_rapid_mapping.New()
		reg("cems_rapid_mapping", c, err)
	}
	{
		c, err := wmo_alert_hub.New()
		reg("wmo_alert_hub", c, err)
	}
	{
		c, err := wmo_cap_alert_areas.New()
		reg("wmo_cap_alert_areas", c, err)
	}
	{
		c, err := global_cap_alerts.New()
		reg("global_cap_alerts", c, err)
	}
	{
		c, err := direct_weather_alerts.New()
		reg("direct_weather_alerts", c, err)
	}
	{
		c, err := malaysia_weather_warnings.New()
		reg("malaysia_weather_warnings", c, err)
	}
	{
		c, err := thailand_tmd_alerts.New()
		reg("thailand_tmd_alerts", c, err)
	}
	{
		c, err := singapore_realtime.New()
		reg("singapore_realtime", c, err)
	}
	{
		c, err := vaac_tokyo.New()
		reg("vaac_tokyo", c, err)
	}
	{
		c, err := vaac_washington.New()
		reg("vaac_washington", c, err)
	}
	{
		c, err := vaac_global.New()
		reg("vaac_global", c, err)
	}
	{
		c, err := spc_storm_reports.New()
		reg("spc_storm_reports", c, err)
	}
	{
		c, err := hms_smoke.New()
		reg("hms_smoke", c, err)
	}
	{
		c, err := swdi_radar_signatures.New()
		reg("swdi_radar_signatures", c, err)
	}
	{
		c, err := noaa_nwm.New()
		reg("noaa_nwm", c, err)
	}
	{
		c, err := flood_coverage.NewCEMSGFM(pool)
		reg("cems_gfm", c, err)
	}
	{
		c, err := flood_coverage.NewGloFAS(pool)
		reg("glofas_flood_forecast", c, err)
	}
	{
		c, err := flood_coverage.NewNASALANCEFlood(pool)
		reg("nasa_lance_flood", c, err)
	}
	{
		c, err := flood_coverage.NewGDACSGFDS()
		reg("gdacs_gfds", c, err)
	}
	{
		c, err := flood_coverage.NewIMERGPrecip(pool)
		reg("imerg_precip", c, err)
	}
	{
		c, err := flood_coverage.NewHydrologyStaticContext(pool)
		reg("hydrology_static_context", c, err)
	}
	{
		c, err := flood_coverage.NewGlobalPrecipMonitor(pool)
		reg("global_precip_monitor", c, err)
	}
	{
		c, err := flood_coverage.NewDirectFloodCAP()
		reg("direct_flood_cap", c, err)
	}
	{
		c, err := lhasa_landslide.New(pool)
		reg("lhasa_landslide", c, err)
	}
	{
		c, err := open_meteo_anomalies.New(pool)
		reg("open_meteo_anomalies", c, err)
	}
	{
		c, err := cams_atmosphere.New(pool)
		reg("cams_atmosphere", c, err)
	}
	{
		c, err := space_weather.New()
		reg("space_weather", c, err)
	}
	{
		c, err := firms.New()
		reg("firms", c, err)
	}
	{
		c, err := asean_haze_hotspots.New()
		reg("asean_haze_hotspots", c, err)
	}
	{
		c, err := geothermal.New()
		reg("geo_thermal", c, err)
	}
	// FAA NAS airport status. Enabled by default; can be toggled off if the
	// upstream starts rejecting automated clients again.
	if os.Getenv("GORDIOS_DISABLE_FAA") != "1" {
		c, err := faa_status.New()
		reg("faa_status", c, err)
	} else {
		msg := "disabled via GORDIOS_DISABLE_FAA=1"
		markSourceSkipped("faa_status", msg)
		log.Printf("[ingester] faa_status %s", msg)
	}
	{
		c, err := volcanoes.New()
		reg("volcanoes", c, err)
	}
	{
		c, err := volcano_notices.New()
		reg("volcano_notices", c, err)
	}
	{
		c, err := launches.New()
		reg("launches", c, err)
	}
	// ReliefWeb v2 requires an approved appname. Auto-enables when
	// RELIEFWEB_APPNAME is set (or GORDIOS_ENABLE_RELIEFWEB=1 to try with the
	// unregistered default, which will 403).
	if os.Getenv("RELIEFWEB_APPNAME") != "" || os.Getenv("GORDIOS_ENABLE_RELIEFWEB") == "1" {
		c, err := reliefweb.New()
		reg("reliefweb", c, err)
	} else {
		msg := "skipped: set RELIEFWEB_APPNAME after registering"
		markSourceSkipped("reliefweb", msg)
		log.Printf("[ingester] reliefweb %s", msg)
	}
	{
		c, err := ifrc_go.New()
		reg("ifrc_go", c, err)
	}
	{
		c, err := global_disaster_reports.New()
		reg("global_disaster_reports", c, err)
	}
	{
		c, err := fews_net_food_security.New()
		reg("fews_net_food_security", c, err)
	}
	{
		c, err := wfp_food_prices.New()
		reg("wfp_food_prices", c, err)
	}
	{
		c, err := copernicus_gdo_drought.New(pool)
		reg("copernicus_gdo_drought", c, err)
	}
	{
		c, err := noaa_cpc_gth.New()
		reg("noaa_cpc_global_tropics_hazards", c, err)
	}
	{
		c, err := gwis_fire_danger.New(pool)
		reg("gwis_fire_danger", c, err)
	}
	{
		c, err := inform_risk.New()
		reg("inform_risk_severity", c, err)
	}

	// Phase 3 batch B + military:
	{
		c, err := military.New()
		reg("military", c, err)
	}
	{
		c, err := nga_warnings.New()
		reg("nga_warnings", c, err)
	}
	// GDELT 2.0 — 15-min CSV dumps (no auth, no billing).
	{
		c, err := gdelt.New()
		reg("gdelt", c, err)
	}
	// UCDP GED API now requires an access token. Auto-enables when
	// UCDP_ACCESS_TOKEN is present in the env.
	if os.Getenv("UCDP_ACCESS_TOKEN") != "" {
		c, err := ucdp.New()
		reg("ucdp", c, err)
	} else {
		msg := "skipped: set UCDP_ACCESS_TOKEN in .env to enable"
		markSourceSkipped("ucdp", msg)
		log.Printf("[ingester] ucdp %s", msg)
	}
	// GPSJAM: daily H3-cell CSV, URL verified. Enabled by default; safe to
	// toggle off with GORDIOS_DISABLE_GPSJAM=1.
	if os.Getenv("GORDIOS_DISABLE_GPSJAM") != "1" {
		c, err := gps_jamming.New()
		reg("gps_jamming", c, err)
	} else {
		log.Println("[ingester] gps_jamming disabled via GORDIOS_DISABLE_GPSJAM=1")
	}

	// Phase 4: authenticated feeds.
	{
		c, err := acled.New()
		reg("acled", c, err)
	}

	// Phase 6: oil prices timeseries.
	{
		c, err := oil_prices.New()
		reg("oil_prices", c, err)
	}
	{
		c, err := iata_fuel_monitor.New()
		reg("iata_fuel_monitor", c, err)
	}
	{
		c, err := airline_safety.NewIATAIOSARegistry()
		reg("iata_iosa_registry", c, err)
	}
	{
		c, err := airline_safety.NewIATAISSARegistry()
		reg("iata_issa_registry", c, err)
	}
	{
		c, err := airline_safety.NewEUAirSafetyList()
		reg("eu_air_safety_list", c, err)
	}
	{
		c, err := airline_safety.NewFAAIASA()
		reg("faa_iasa", c, err)
	}
	{
		c, err := airline_safety.NewICAOUSOAP()
		reg("icao_usoap", c, err)
	}
	{
		c, err := airline_safety.NewFAASDR()
		reg("faa_sdr", c, err)
	}
	{
		c, err := airline_safety.NewNTSBAviationAccidents()
		reg("ntsb_aviation_accidents", c, err)
	}
	{
		c, err := airline_safety.NewEASACZIB()
		reg("easa_czib", c, err)
	}
	{
		c, err := airline_safety.NewFAAFlightRestrictions()
		reg("faa_flight_restrictions", c, err)
	}
	{
		c, err := airline_safety.NewDOTCertificatedCarriers()
		reg("dot_certificated_carriers", c, err)
	}
	{
		c, err := airline_safety.NewAirlineReportsMonitor()
		reg("airline_reports_monitor", c, err)
	}

	// Phase 3 B leftovers — travel advisories (Overpass traffic is in gateway/overpass.go).
	{
		c, err := travel_advisories.New()
		reg("travel_advisories", c, err)
	}
	{
		c, err := official_advisories.New()
		reg("official_advisories", c, err)
	}
	{
		c, err := planned_protests.New()
		reg("planned_protests", c, err)
	}
	{
		c, err := netblocks.New()
		reg("netblocks_rss", c, err)
	}
	{
		c, err := health_outbreaks.New()
		reg("health_outbreaks", c, err)
	}

	// NOTAM: FAA TFR (keyless) + FAA NOTAM API (keyed) + generic RSS.
	{
		c, err := notam.NewTFR()
		reg("notam_tfr", c, err)
	}
	{
		c, err := notam.NewAPI()
		reg("notam_faa", c, err)
	}
	{
		c, err := notam.NewRSS()
		reg("notam_rss", c, err)
	}

	// Phase 3 batch B remainder.
	// IODA: raw country signals endpoint, keyless. Safe to toggle off if the
	// upstream API shape changes again.
	if os.Getenv("GORDIOS_DISABLE_IODA") != "1" {
		c, err := ioda.New()
		reg("ioda", c, err)
	} else {
		msg := "disabled via GORDIOS_DISABLE_IODA=1"
		markSourceSkipped("ioda", msg)
		log.Printf("[ingester] ioda skipped (%s)", msg)
	}
	{
		c, err := cyber_threats.New()
		reg("cyber_threats", c, err)
	}
	{
		c, err := internetdb_exposure.New()
		reg("internetdb_exposure", c, err)
	}
	{
		c, err := tor_exit_nodes.New()
		reg("tor_exit_nodes", c, err)
	}
	// Cloudflare Radar — auto-registers when CF_RADAR_TOKEN is set.
	if os.Getenv("CF_RADAR_TOKEN") != "" {
		c, err := cloudflare_radar.New()
		reg("cloudflare_radar", c, err)
	} else {
		msg := "skipped: set non-empty CF_RADAR_TOKEN in .env to enable"
		markSourceSkipped("cloudflare_radar", msg)
		log.Printf("[ingester] cloudflare_radar %s", msg)
	}
	// OpenAQ v3 now enforces X-API-Key. Disabled until OPENAQ_KEY is set.
	if os.Getenv("OPENAQ_KEY") != "" {
		c, err := openaq.New()
		reg("openaq", c, err)
	} else {
		msg := "skipped: set OPENAQ_KEY in .env to enable"
		markSourceSkipped("openaq", msg)
		log.Printf("[ingester] openaq %s", msg)
	}
	{
		c, err := safecast.New()
		reg("safecast", c, err)
	}
	{
		c, err := epa_radnet.New()
		reg("epa_radnet", c, err)
	}
	{
		c, err := saveecobot.New()
		reg("saveecobot_radiation", c, err)
	}
	{
		c, err := eurdep.New()
		reg("eurdep_radiation", c, err)
	}
	{
		c, err := purpleair.New()
		reg("purpleair", c, err)
	}
	{
		c, err := airnow_alerts.New()
		reg("airnow_alerts", c, err)
	}
	{
		c, err := avalanche_alerts.New()
		reg("avalanche_alerts", c, err)
	}
	{
		c, err := water_gauges.New()
		reg("water_gauges", c, err)
	}
	{
		c, err := ndbc_buoys.New()
		reg("ndbc_buoys", c, err)
	}
	{
		c, err := portwatch_disruptions.New()
		reg("portwatch_disruptions", c, err)
	}
	{
		c, err := portwatch_port_activity.New()
		reg("portwatch_port_activity", c, err)
	}
	{
		c, err := maritime_baselines.NewMarineCadastreAISCatalog()
		reg("marinecadastre_ais_catalog", c, err)
	}
	{
		c, err := maritime_baselines.NewEMODnetVesselDensity()
		reg("emodnet_vessel_density", c, err)
	}
	{
		c, err := unhcr_displacement.New()
		reg("unhcr_displacement", c, err)
	}
	{
		c, err := worldpop_exposure.NewGHSLPopulation(pool)
		reg("ghsl_population", c, err)
	}
	{
		c, err := worldpop_exposure.NewGHSLSettlementModel(pool)
		reg("ghsl_smod", c, err)
	}
	{
		c, err := worldpop_exposure.New(pool)
		reg("worldpop_exposure", c, err)
	}
	{
		c, err := worldpop_exposure.NewHRSLPopulation(pool)
		reg("hrsl_population", c, err)
	}
	{
		c, err := worldpop_exposure.NewPopulationH3Exposure(pool)
		reg("population_h3_exposure", c, err)
	}
	{
		c, err := worldpop_exposure.NewOSMSettlementContext(pool)
		reg("osm_settlement_context", c, err)
	}

	// Phase D: Tier S situational-awareness sources.
	{
		c, err := cisa_kev.New(pool)
		reg("cisa_kev", c, err)
	}
	{
		c, err := nws_sigmet.New()
		reg("nws_sigmet", c, err)
	}
	{
		c, err := bgp_visibility.New()
		reg("bgp_visibility", c, err)
	}
	{
		c, err := globalping.New()
		reg("globalping_measurements", c, err)
	}
	{
		c, err := bgpstream_broker.New()
		reg("bgpstream_broker", c, err)
	}
	{
		c, err := ooni.New()
		reg("ooni", c, err)
	}
	{
		c, err := tor_metrics.New()
		reg("tor_metrics", c, err)
	}
	{
		c, err := rf_presence.New()
		reg("rf_presence", c, err)
	}
	{
		c, err := sanctions.New(pool)
		reg("sanctions", c, err)
	}

	// Phase E: Tier A sources.
	// Auth-gated: auto-enable when creds present.
	if os.Getenv("SPACETRACK_USER") != "" && os.Getenv("SPACETRACK_PASS") != "" {
		c, err := spacetrack.New()
		reg("spacetrack", c, err)
	} else {
		msg := "skipped: set SPACETRACK_USER / SPACETRACK_PASS"
		markSourceSkipped("spacetrack", msg)
		log.Printf("[ingester] spacetrack %s", msg)
	}
	if os.Getenv("ENTSOE_TOKEN") != "" {
		c, err := entsoe.New()
		reg("entsoe_outage", c, err)
	} else {
		msg := "skipped: set ENTSOE_TOKEN"
		markSourceSkipped("entsoe_outage", msg)
		log.Printf("[ingester] entsoe_outage %s", msg)
	}
	{
		c, err := eia930.New()
		reg("eia930", c, err)
	}
	{
		c, err := eia_wpsr.New()
		reg("eia_wpsr", c, err)
	}
	if os.Getenv("GFW_TOKEN") != "" {
		c, err := gfw.New()
		reg("gfw_events", c, err)
	} else {
		msg := "skipped: set GFW_TOKEN in .env to enable"
		markSourceSkipped("gfw_events", msg)
		log.Printf("[ingester] gfw_events %s", msg)
	}

	// Phase C: TLE catalogue (cadence). Consumed by satellite/orbit context
	// layers and downstream analytic jobs.
	{
		c, err := tle.New()
		reg("tle", c, err)
	}
	{
		c, err := satnogs.New()
		reg("satnogs", c, err)
	}
	{
		c, err := copernicus_sentinel.New(pool)
		reg("copernicus_sentinel", c, err)
	}
	{
		c, err := sentinel_plan.New(pool)
		reg("sentinel_acquisition_plan", c, err)
	}
	{
		c, err := opera_dist.New(pool)
		reg("opera_dist_alert", c, err)
	}
	{
		c, err := tropomi.NewNO2(pool)
		reg("tropomi_no2", c, err)
	}
	{
		c, err := tropomi.NewSO2(pool)
		reg("tropomi_so2", c, err)
	}
	{
		c, err := blackmarble.New(pool)
		reg("black_marble_nightlights", c, err)
	}

	// Phase C: streaming collectors — long-lived WebSocket ingest, run in
	// their own goroutines alongside the cadence scheduler. Tracked via a
	// WaitGroup so `main` can wait for them to drain their sinks on SIGTERM
	// before tearing the pool down.
	type streamer interface {
		ID() string
		Run(context.Context) error
	}
	var streamWG sync.WaitGroup
	startStream := func(name string, sc streamer) {
		markSourceActive(name, 0)
		streamWG.Add(1)
		go func() {
			defer streamWG.Done()
			if err := sc.Run(ctx); err != nil {
				log.Printf("[%s] stream stopped: %v", name, err)
			}
		}()
		log.Printf("[ingester] streaming: %s", name)
	}
	startStream("lightning", lightning.New(pool))
	if envIsOne("GORDIOS_ENABLE_MARITIME_STREAM", "WV_ENABLE_MARITIME_STREAM") {
		if mc, err := maritime.New(pool); err == nil {
			startStream("maritime", mc)
		} else {
			log.Printf("[ingester] maritime skipped: %v", err)
			markSourceSkipped("maritime", err.Error())
		}
	} else {
		msg := "skipped: set GORDIOS_ENABLE_MARITIME_STREAM=1 to enable"
		markSourceSkipped("maritime", msg)
		log.Printf("[ingester] maritime %s", msg)
	}
	startStream("bluesky", bluesky.New(pool))
	startStream("wikipedia_surge", wikipedia.New(pool))
	{
		c, err := wikipedia_pageviews.New()
		reg("wikipedia_pageviews", c, err)
	}
	startStream("ripe_ris", ripe_ris.New(pool))

	log.Println("[ingester] running")
	if err := sch.Run(ctx); err != nil {
		log.Fatalf("scheduler: %v", err)
	}
	// Scheduler returns only when ctx is cancelled. Wait for streamers to
	// exit their Run loops (sinks auto-flush on ctx.Done via context.Background
	// so the last buffer still lands even after cancellation).
	streamWG.Wait()
	log.Println("[ingester] shutting down")
}

func defaultDatabaseURL(key string) {
	if os.Getenv("DATABASE_URL") != "" {
		return
	}
	if v := os.Getenv(key); v != "" {
		_ = os.Setenv("DATABASE_URL", v)
	}
}

func envIsOne(keys ...string) bool {
	for _, key := range keys {
		if os.Getenv(key) == "1" {
			return true
		}
	}
	return false
}
