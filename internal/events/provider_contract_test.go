// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type providerContract struct {
	requiredProps []string
}

type providerFixture struct {
	ExtID   string         `json:"ext_id"`
	TS      string         `json:"ts"`
	HasGeom bool           `json:"has_geom"`
	Props   map[string]any `json:"props"`
}

var storedProviderContracts = map[string]providerContract{
	"acled":                     {[]string{"event_id_cnty", "event_type", "sub_event_type", "country", "latitude", "longitude"}},
	"airnow_alerts":             {[]string{"source_provider", "title", "area_desc", "air_quality_score", "source_api_endpoint"}},
	"avalanche_alerts":          {[]string{"source_provider", "title", "danger_level", "avalanche_score", "source_api_endpoint"}},
	"bgp_visibility":            {[]string{"country", "registered", "routed", "routed_ratio", "outage_severity"}},
	"black_marble_nightlights":  {[]string{"h3_cell", "current_date", "anomaly_z", "blackout_score"}},
	"bluesky":                   {[]string{"did", "rkey", "text", "url"}},
	"bgpstream_broker":          {[]string{"project", "collector", "latest_update_time", "update_lag_seconds"}},
	"cams_atmosphere":           {[]string{"watch_aoi_id", "observed_at", "air_quality_pressure_score", "dominant_pollutant"}},
	"cems_rapid_mapping":        {[]string{"code", "name", "category", "n_products"}},
	"cisa_kev":                  {[]string{"cve", "vendor", "product", "date_added", "description"}},
	"copernicus_sentinel":       {[]string{"product_id", "product_name", "product_family", "acquisition_source"}},
	"cems_gfm":                  {[]string{"state", "integration_state", "source_api_endpoint"}},
	"cyber_threats":             {[]string{"ip_address", "malware", "first_seen", "status"}},
	"direct_flood_cap":          {[]string{"feed_id", "event", "headline"}},
	"eonet":                     {[]string{"id", "title", "categories"}},
	"emsc_seismic":              {[]string{"unid", "time", "mag", "flynn_region"}},
	"epa_radnet":                {[]string{"station_id", "station_name", "value", "unit", "z_score"}},
	"eurdep_radiation":          {[]string{"station_id", "value", "unit", "observed_at", "radiation_value_score"}},
	"eia930":                    {[]string{"respondent", "metric", "period", "value_mw", "grid_anomaly_score"}},
	"faa_status":                {[]string{"category", "reason", "feed_update_time"}},
	"firms":                     {[]string{"latitude", "longitude", "acq_date", "acq_time", "frp", "frp_score", "brightness_score"}},
	"flights":                   {[]string{"icao24", "source_provider", "time_position"}},
	"gdacs":                     {[]string{"eventid", "eventtype", "eventname", "alertlevel"}},
	"gdacs_gfds":                {[]string{"product", "integration_state", "source_api_endpoint"}},
	"gdelt":                     {[]string{"global_event_id", "sql_date", "country", "source_url"}},
	"ghsl_population":           {[]string{"source_dataset", "source_role", "integration_state"}},
	"ghsl_smod":                 {[]string{"source_dataset", "source_role", "integration_state"}},
	"global_precip_monitor":     {[]string{"watch_aoi_id", "cmorph_state", "persiann_state", "precip_pressure_score"}},
	"globalping_measurements":   {[]string{"measurement_id", "target", "probe_country", "packet_loss", "reachability_loss_score"}},
	"glofas_flood_forecast":     {[]string{"watch_aoi_id", "forecast_peak_discharge_m3s", "flood_pressure_score"}},
	"gps_jamming":               {[]string{"h3_cell", "count_bad_aircraft", "intensity", "intensity_score", "date"}},
	"health_outbreaks":          {[]string{"source", "country", "title", "disease", "alert_level"}},
	"hms_smoke":                 {[]string{"density", "start_time", "end_time", "satellite"}},
	"hrsl_population":           {[]string{"source_dataset", "source_role", "integration_state"}},
	"hydrology_static_context":  {[]string{"context_kind", "source_family", "source_api_endpoint"}},
	"ibtracs":                   {[]string{"sid", "iso_time", "basin", "wind_kt"}},
	"ioda":                      {[]string{"country", "outage_score", "material_reason", "source_api_endpoint"}},
	"imerg_precip":              {[]string{"watch_aoi_id", "precip_pressure_score", "source_api_endpoint"}},
	"jtwc":                      {[]string{"id", "basin", "warn_time", "vmax_kt"}},
	"launches":                  {[]string{"id", "name", "net", "pad", "status"}},
	"lhasa_landslide":           {[]string{"watch_aoi_id", "l_haz", "landslide_hazard_score", "source_api_endpoint"}},
	"lightning":                 {[]string{"stations", "pol"}},
	"meteoalarm":                {[]string{"event", "severity", "urgency", "country"}},
	"military":                  {[]string{"hex", "lat", "lon", "t"}},
	"nifc_fire_perimeters":      {[]string{"GlobalID", "poly_IncidentName", "poly_GISAcres", "poly_IRWINID"}},
	"nifc_wildfires":            {[]string{"IrwinID", "IncidentName", "IncidentTypeCategory", "ModifiedOnDateTime_dt"}},
	"nhc_gis_cones":             {[]string{"storm_id", "storm_name", "product_type", "product_url"}},
	"nasa_lance_flood":          {[]string{"watch_aoi_id", "product", "product_url", "integration_state"}},
	"ndbc_buoys":                {[]string{"station_id", "station_name", "observed_at", "gust_m_s", "pressure_hpa"}},
	"netblocks_rss":             {[]string{"title", "link", "country", "outage_type", "severity_score"}},
	"noaa_nwm":                  {[]string{"product", "domain", "cycle", "product_url"}},
	"noaa_tsunami":              {[]string{"title", "category", "center", "bulletin_url"}},
	"notam_tfr":                 {[]string{"notam_id", "description", "state", "type"}},
	"nws_alerts":                {[]string{"id", "event", "severity", "severity_score", "headline"}},
	"nws_sigmet":                {[]string{"icaoId", "hazard", "validTimeFrom", "validTimeTo"}},
	"oil_prices":                {[]string{"symbol", "date", "close"}},
	"ooni":                      {[]string{"country", "test_name", "measurement_count", "blocking_score"}},
	"osm_settlement_context":    {[]string{"watch_aoi_id", "building_count_1km", "settlement_presence_score"}},
	"official_advisories":       {[]string{"source", "country", "title", "advisory_type"}},
	"opera_dist_alert":          {[]string{"watch_aoi_id", "granule_id", "time_start", "disturbance_product_available_score"}},
	"population_h3_exposure":    {[]string{"watch_aoi_id", "h3_cell", "impact_prior_score"}},
	"planned_protests":          {[]string{"source_provider", "title", "location", "planned_protest_score", "source_api_endpoint"}},
	"public_safety_incidents":   {[]string{"source_provider", "incident_type", "incident_score", "source_api_endpoint"}},
	"rf_presence":               {[]string{"network", "role", "spots", "bucket"}},
	"open_meteo_anomalies":      {[]string{"watch_aoi_id", "severity", "anomaly_type", "anomaly_score"}},
	"portwatch_disruptions":     {[]string{"eventid", "eventtype", "eventname", "alertlevel"}},
	"portwatch_port_activity":   {[]string{"portid", "portname", "iso3", "tanker_calls_14d"}},
	"purpleair":                 {[]string{"sensor_index", "aoi_label", "pm25_atm_ugm3", "air_particulate_score"}},
	"regional_seismic":          {[]string{"source_provider", "mag", "seismic_authority_score", "source_api_endpoint"}},
	"regional_wildfires":        {[]string{"source_provider", "title", "wildfire_context_score", "source_api_endpoint"}},
	"ripe_ris":                  {[]string{"country", "updates", "withdrawals", "withdrawal_ratio", "routing_instability_score"}},
	"road_incidents":            {[]string{"source_provider", "incident_type", "road_disruption_score", "source_api_endpoint"}},
	"safecast":                  {[]string{"id", "captured_at", "latitude", "longitude", "value"}},
	"satnogs":                   {[]string{"observation_id", "norad_cat_id", "start", "end", "status"}},
	"saveecobot_radiation":      {[]string{"city_slug", "gamma_nsv_h", "unit", "observed_at", "radiation_value_score"}},
	"seismic":                   {[]string{"code", "mag", "place", "time", "title"}},
	"sentinel_acquisition_plan": {[]string{"watch_aoi_id", "satellite_id", "datatake_id", "planned_start", "planned_acquisition_score"}},
	"space_weather":             {[]string{"product_id", "issue_datetime", "message"}},
	"spc_storm_reports":         {[]string{"type", "time", "magnitude", "state"}},
	"swdi_radar_signatures":     {[]string{"dataset", "ztime", "wsr_id", "cell_id"}},
	"tle":                       {[]string{"name", "line1", "line2"}},
	"tor_metrics":               {[]string{"country", "latest_date", "direct_users", "direct_connecting_drop_score", "bridge_demand_surge_score"}},
	"travel_advisories":         {[]string{"source", "country", "title", "link"}},
	"tropomi_no2":               {[]string{"gas", "h3_cell", "anomaly_score", "state"}},
	"tropomi_so2":               {[]string{"gas", "h3_cell", "anomaly_score", "state"}},
	"unhcr_displacement":        {[]string{"country_code", "country", "year", "total_displaced"}},
	"usgs_shakemap":             {[]string{"event_id", "title", "has_shakemap", "has_pager", "usgs_url"}},
	"utility_outages":           {[]string{"source_provider", "affected_customers", "outage_score", "source_api_endpoint"}},
	"volcano_notices":           {[]string{"notice_identifier", "volcano_name", "alert_level", "color_code"}},
	"vaac_tokyo":                {[]string{"volcano", "vaac", "advisory_nr", "advisory_url"}},
	"vaac_washington":           {[]string{"volcano", "vaac", "advisory_nr", "advisory_url"}},
	"water_gauges":              {[]string{"site_code", "site_name", "gage_height_ft"}},
	"wikipedia_surge":           {[]string{"wiki", "title", "edits_10min", "url"}},
	"wikipedia_pageviews":       {[]string{"wiki", "title", "article", "mode", "views"}},
	"wmo_alert_hub":             {[]string{"id", "event", "headline", "member_country", "severity_score", "urgency_score", "certainty_score"}},
	"wmo_cap_alert_areas":       {[]string{"identifier", "event", "severity", "areaDesc", "severity_score", "urgency_score", "certainty_score"}},
	"worldpop_exposure":         {[]string{"watch_aoi_id", "population_1km", "population_5km", "population_25km"}},
}

func TestProviderFixtureContracts(t *testing.T) {
	raw, err := os.ReadFile("testdata/provider_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]providerFixture
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) == 0 {
		t.Fatal("provider fixture file is empty")
	}
	for source := range storedProviderContracts {
		fx, ok := fixtures[source]
		if !ok {
			t.Fatalf("provider fixture missing source %q", source)
		}
		validateProviderFixture(t, source, fx)
	}
	for source := range fixtures {
		if _, ok := storedProviderContracts[source]; !ok {
			t.Fatalf("fixture source %q has no provider contract", source)
		}
	}
}

func TestStoredProviderContracts(t *testing.T) {
	if os.Getenv("GORDIOS_PROVIDER_CONTRACTS") != "1" {
		t.Skip("set GORDIOS_PROVIDER_CONTRACTS=1 to validate stored provider rows against the live DB")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://gordios:gordios@127.0.0.1:15432/gordios?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	seen := map[string]int{}
	sources := make([]string, 0, len(storedProviderContracts))
	for source := range storedProviderContracts {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		rows, err := pool.Query(ctx, `
			SELECT ext_id, ts, props::text, geom IS NOT NULL
			  FROM events
			 WHERE source = $1
			 ORDER BY ts DESC, ext_id
			 LIMIT 5`, source)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var extID, rawProps string
			var ts time.Time
			var hasGeom bool
			if err := rows.Scan(&extID, &ts, &rawProps, &hasGeom); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			seen[source]++
			props := map[string]any{}
			if err := json.Unmarshal([]byte(rawProps), &props); err != nil {
				rows.Close()
				t.Fatalf("%s/%s props are not JSON: %v", source, extID, err)
			}
			validateProviderFixture(t, source, providerFixture{
				ExtID:   extID,
				TS:      ts.Format(time.RFC3339Nano),
				HasGeom: hasGeom,
				Props:   props,
			})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}

	if len(seen) == 0 {
		t.Fatal("no provider rows found in events")
	}
	for source := range seen {
		t.Run(source, func(t *testing.T) {
			t.Logf("validated %d recent rows", seen[source])
		})
	}
}

func validateProviderFixture(t *testing.T, source string, fx providerFixture) {
	t.Helper()
	contract, ok := storedProviderContracts[source]
	if !ok {
		t.Fatalf("source %q has stored events but no provider contract", source)
	}
	if strings.TrimSpace(fx.ExtID) == "" {
		t.Fatalf("%s row has empty ext_id", source)
	}
	if strings.TrimSpace(fx.TS) == "" {
		t.Fatalf("%s/%s row has empty timestamp", source, fx.ExtID)
	}
	if _, err := time.Parse(time.RFC3339Nano, fx.TS); err != nil {
		t.Fatalf("%s/%s timestamp %q is not RFC3339: %v", source, fx.ExtID, fx.TS, err)
	}
	if !fx.HasGeom {
		t.Fatalf("%s/%s row has null geometry", source, fx.ExtID)
	}
	if fx.Props == nil {
		t.Fatalf("%s/%s has nil props", source, fx.ExtID)
	}
	for _, key := range contract.requiredProps {
		if _, ok := fx.Props[key]; !ok {
			t.Fatalf("%s/%s missing required prop %q; props keys=%v", source, fx.ExtID, key, sortedKeys(fx.Props))
		}
	}
}

func TestStoredProviderContractsAreNamed(t *testing.T) {
	for source, contract := range storedProviderContracts {
		if strings.TrimSpace(source) == "" {
			t.Fatal("empty source name in provider contracts")
		}
		if len(contract.requiredProps) == 0 {
			t.Fatalf("%s has no required props", source)
		}
		seen := map[string]struct{}{}
		for _, key := range contract.requiredProps {
			if strings.TrimSpace(key) == "" {
				t.Fatalf("%s has blank required prop", source)
			}
			if _, ok := seen[key]; ok {
				t.Fatalf("%s repeats required prop %q", source, key)
			}
			seen[key] = struct{}{}
		}
	}
}

func sortedKeys(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("%v", keys)
}
