# Source Catalog

Collection owns the raw/event and feature source IDs listed below. This catalog
was initially verified against `GET /api/sources/status` on 2026-04-18 and is
updated as collectors and importers are added.

Runtime status is intentionally not recorded here. Use `/api/sources/status`
for freshness, enabled/disabled state, and last error.

License and terms are not inferred in this catalog. Verified source-data terms
are listed in the source-data section below.

## Complete Source Catalog

| Source ID | Original / upstream name | Kind | Description |
| --- | --- | --- | --- |
| `acled` | ACLED | `event` | Armed Conflict Location & Event Data API, using authenticated recent global conflict events. |
| `adsb_lol` | ADSB.lol | `event` | Public ADS-B aircraft snapshots; defaults to military-registered aircraft and supports configured AOIs. |
| `airnow_alerts` | AIRNow / EnviroFlash | `event` | Public CAP aggregate for U.S. air quality alerts. |
| `airline_reports_monitor` | Configured airline report URLs | `event` | Optional monitor for configured airline annual, safety, sustainability, or financial report URLs. |
| `asean_haze_hotspots` | ASEAN Specialised Meteorological Centre VIIRS Hotspot Count | `event` | Daily high-confidence VIIRS hotspot counts for selected Southeast Asia regions. |
| `avalanche_alerts` | Avalanche.org | `event` | Public avalanche forecast polygons. |
| `bgp_visibility` | RIPEstat | `event` | Country routed/registered ASN ratio for configured BGP watchlists. |
| `bgpstream_broker` | CAIDA BGPStream Broker | `event` | Public BGPStream project metadata freshness for RouteViews and RIPE RIS archives. |
| `bmkg_indonesia` | BMKG Data Terbuka Gempabumi | `event` | Official Indonesia earthquake, felt-earthquake, and recent M5+ BMKG feeds. |
| `black_marble_nightlights` | NASA Black Marble | `event` | Night-light AOI samples from VIIRS Black Marble products. |
| `bluesky` | Bluesky Jetstream | `event` | Keyword-filtered feed-post firehose. |
| `cables` | TeleGeography Submarine Cable Map | `polygon_static` | Remote static submarine cable route features imported by `cmd/importer features`. |
| `cams_atmosphere` | CAMS via Open-Meteo Air Quality API | `event` | CO, NO2, SO2, PM, aerosol, and dust samples over active AOIs. |
| `capitol_buildings_us` | USGS / ArcGIS State Capitol | `feature_static` | U.S. state capitol building features. |
| `cctv_cameras` | TfL JamCam, OpenTrafficCamMap, camera seed file, OSM Overpass | `feature_inventory` | Camera inventory collector that writes camera features and provenance. |
| `cems_gfm` | Copernicus EMS Global Flood Monitoring | `event` | AOI flood-monitoring product API with explicit token-required states. |
| `cems_rapid_mapping` | Copernicus EMS Rapid Mapping | `event` | Public activations with AOI/product counts, categories, countries, and centroids. |
| `chokepoints` | Project-authored chokepoint seed | `polygon_static` | Approximate maritime chokepoint polygons from bundled seed data. |
| `cisa_kev` | CISA Known Exploited Vulnerabilities | `event` | KEV catalog enriched per CVE with EPSS, NVD, and CISA Vulnrichment records. |
| `cloudflare_radar` | Cloudflare Radar | `event` | Internet outage and DDoS-origin observations. |
| `copernicus_gdo_drought` | Copernicus Global Drought Observatory | `event` | Combined Drought Indicator raster samples over active AOIs. |
| `copernicus_sentinel` | Copernicus Data Space Ecosystem Sentinel catalogue | `event` | Sentinel-1/2 catalogue product intersections for active candidate AOIs. |
| `cruise_terminals_us` | ArcGIS Open Data cruise terminals | `feature_static` | U.S. cruise line terminal features. |
| `cyber_threats` | abuse.ch, C2IntelFeeds, Ransomware.live | `event` | OSINT cyber indicator feeds including C2 IPs, malware URLs, and recent ransomware victims where available. |
| `desal_plants` | Project-authored desalination plant seed | `polygon_static` | Approximate desalination plant features from bundled seed data. |
| `direct_flood_cap` | NOAA/NWS active alerts | `event` | Direct flood-warning CAP slice, currently starting from active NOAA/NWS flood alerts. |
| `direct_weather_alerts` | Canada GeoMet, MET Norway MetAlerts, JMA warning map | `event` | Direct national weather alert feeds used to supplement WMO and MeteoAlarm coverage. |
| `dot_certificated_carriers` | U.S. DOT Certificated Air Carriers List | `event` | Document monitor for the DOT certificated air-carrier list PDF and publication metadata. |
| `easa_czib` | EASA Conflict Zone Information Bulletins | `event` | Public EASA CZIB JSON export with active conflict-zone aviation safety advisories. |
| `eia930` | EIA-930 | `event` | U.S. balancing-authority demand and interchange anomaly observations. |
| `eia_wpsr` | EIA Weekly Petroleum Status Report | `event` | Keyless WPSR CSV table snapshots for U.S. petroleum stocks, production, refinery inputs, and PAD District weekly estimates. |
| `emsc_seismic` | EMSC SeismicPortal | `event` | Near-real-time global earthquakes with magnitude, depth, and Flynn region metadata. |
| `emodnet_vessel_density` | EMODnet Human Activities Vessel Density via ERDDAP | `event` | Metadata monitor for EMODnet vessel-density raster products. |
| `entsoe_outage` | ENTSO-E Transparency Platform | `event` | Production-unit unavailability records. |
| `eonet` | NASA EONET | `event` | Open natural-event feed for fires, storms, floods, and related hazards. |
| `epa_radnet` | EPA RadNet | `event` | Fixed-station gamma readings with local baseline delta and z-score. |
| `eurdep_radiation` | EURDEP / JRC REMap | `event` | Radiation monitoring service wiring for permitted machine-readable station sampling. |
| `eu_air_safety_list` | European Commission EU Air Safety List | `event` | Public Excel list of airlines banned or restricted from operating in the European Union. |
| `faa_flight_restrictions` | FAA Prohibitions, Restrictions and Notices | `event` | Public FAA page of country/FIR aviation prohibitions, advisories, SFARs, and pointer NOTAM documents. |
| `faa_iasa` | FAA International Aviation Safety Assessment Program Results | `event` | FAA IASA results document monitor for country civil-aviation authority safety-oversight categories. |
| `faa_sdr` | FAA Service Difficulty Reports | `event` | Public current-year Service Difficulty Report CSV rows, bounded by `FAA_SDR_MAX_ROWS`. |
| `faa_status` | FAA NAS Status | `event` | Airport delay, closure, ground-stop, and airspace-flow status from FAA NAS XML. |
| `fews_net_food_security` | FEWS NET Data Warehouse | `event` | Public food-security population and market-price indicators. |
| `firms` | NASA FIRMS | `event` | Keyless MODIS and VIIRS active-fire detections for the last 24 hours. |
| `flights` | OpenSky Network | `event` | Aircraft state vectors from the OpenSky all-states endpoint. |
| `gdacs` | GDACS | `event` | Global Disaster Alert and Coordination System event list. |
| `gdacs_gfds` | GDACS / JRC Global Flood Detection System | `event` | Daily global flood signal and magnitude product availability. |
| `gdelt` | GDELT 2.0 Events | `event` | Public 15-minute event CSV dumps. |
| `geo_thermal` | NASA FIRMS geostationary NRT | `event` | Optional geostationary active-fire detections for fast thermal watch cues. |
| `gfw_events` | Global Fishing Watch | `event` | Derived vessel events including gaps, loitering, encounters, port visits, and fishing. |
| `ghsl_population` | GHSL GHS-POP | `event` | Global population baseline catalog for candidate AOIs. |
| `ghsl_smod` | GHSL GHS-SMOD | `event` | Global settlement model catalog for candidate AOIs. |
| `global_cap_alerts` | Alert-Hub / Esri CAP Alerts Feed | `event` | Active global CAP alert feature layer. |
| `global_disaster_reports` | ReliefWeb, HDX/UNOSAT, Disaster Charter, Smithsonian/USGS | `event` | Humanitarian, satellite-assessment, and volcano report feeds. |
| `global_precip_monitor` | NOAA CMORPH, CHRS PERSIANN / PDIR-Now | `event` | Product-directory monitor for later raster precipitation sampling. |
| `globalping_measurements` | Globalping | `event` | Active ping probes over configured strategic locations. |
| `glofas_flood_forecast` | GloFAS via Open-Meteo Flood API | `event` | AOI river-discharge forecast samples and derived flood-pressure score. |
| `gps_jamming` | GPSJAM | `event` | Daily H3 resolution-4 cells of possible GPS interference. |
| `ground_truth` | Legacy curated validation dataset | `polygon_static` | Curated local validation features present in the current local database. |
| `gtfs_feed_catalog` | MobilityData Mobility Database Catalogs | `event` | GTFS Realtime feed discovery metadata, including entity types, auth state, license URL, and occupancy feature hints. |
| `gtfs_realtime` | GTFS Realtime | `event` | Agency vehicle positions and service alerts from configured realtime feeds, preserving occupancy and congestion fields where provided. |
| `gwis_fire_danger` | Copernicus/JRC GWIS | `event` | ECMWF Fire Weather Index WMS samples over active AOIs. |
| `health_outbreaks` | WHO, CDC, ECDC | `event` | Official disease and outbreak advisory context. |
| `hms_smoke` | NOAA Hazard Mapping System | `event` | Multisatellite smoke polygons with density and analysis-window metadata. |
| `hospitals_us` | HIFLD / ArcGIS US Hospitals | `feature_static` | U.S. hospital facility features. |
| `hrsl_population` | Data for Good HRSL | `event` | High-resolution population catalog where country products are available. |
| `hydrology_static_context` | JRC Global Surface Water, HydroSHEDS / HydroRIVERS | `event` | Static hydrology context placeholders for flood-prior enrichment. |
| `iata_fuel_monitor` | IATA Jet Fuel Price Monitor | `event` | Public weekly global jet fuel price summary from the IATA/Platts monitor page. |
| `iata_iosa_registry` | IATA Operational Safety Audit Registry | `event` | Official IOSA registry landing-page monitor; the IATA Connect row-level registry is Cloudflare-protected from this runtime. |
| `iata_issa_registry` | IATA Standard Safety Assessment Registry | `event` | Public ISSA registered-airline table with airline, country, expiry, ICAO, and IATA codes. |
| `ibtracs` | NOAA/NCEI IBTrACS | `event` | NRT last-three-years tropical cyclone records merged from RSMCs. |
| `icao_usoap` | ICAO USOAP Safety Audit Results | `event` | ICAO public USOAP interactive-viewer monitor with iSTARS API-service reference metadata. |
| `ifrc_go` | IFRC GO | `event` | Public disaster and emergency event feed. |
| `imerg_precip` | NASA GPM IMERG | `event` | Precipitation catalog/watch integration with explicit PPS download-auth state. |
| `inform_risk_severity` | INFORM Risk and INFORM Severity | `event` | Humanitarian risk and severity spreadsheets. |
| `ioda` | CAIDA IODA | `event` | Raw country-level internet outage signals for configured watchlists. |
| `jma_rsmc` | JMA RSMC Tokyo | `event` | Northwest Pacific active tropical cyclone list and storm specifications. |
| `jtwc` | JTWC via NRL ATCF mirror | `event` | Active tropical cyclone tracks outside the NHC basin. |
| `launches` | Launch Library 2 / TheSpaceDevs | `event` | Upcoming rocket launch calendar with pad locations. |
| `lhasa_landslide` | NASA LHASA | `event` | Landslide exposure polygons near active AOIs. |
| `lightning` | Blitzortung | `event` | Real-time lightning strike WebSocket stream. |
| `marinecadastre_ais_catalog` | NOAA / BOEM MarineCadastre.gov AIS | `event` | Catalog monitor for daily historical AIS CSV files; does not download bulk AIS files during scheduled ingestion. |
| `maritime` | AISStream.io | `event` | AIS WebSocket stream. |
| `martrack` | Big Ocean Data / Martrack | `point_timeseries` | Fleet position point time series. |
| `malaysia_weather_warnings` | METMalaysia via data.gov.my Weather Warning API | `event` | Official Malaysia weather, marine, and tropical-cyclone warning records. |
| `meteoalarm` | MeteoAlarm / EUMETNET | `event` | Severe weather warnings across European countries. |
| `military` | ADSB.lol and Airplanes.live military feeds | `event` | Public military aircraft snapshots. |
| `nasa_lance_flood` | NASA LANCE MODIS/VIIRS NRT flood products | `event` | Global flood product catalog URLs by watch AOI tile. |
| `ndbc_buoys` | NOAA NDBC | `event` | Real-time buoy wind, wave, and pressure observations. |
| `netblocks_rss` | NetBlocks RSS | `event` | Interpreted network disruption reports for outage context. |
| `network_rail_train_movements` | Network Rail Open Data Train Movements | `event` | Authenticated bounded STOMP samples from the TRUST train movement topic. |
| `nga_warnings` | NGA Maritime Safety Information | `event` | Broadcast maritime and hydrographic warnings when usable coordinates are present. |
| `nhc` | NOAA National Hurricane Center | `event` | Tropical cyclone advisories and track products. |
| `nhc_gis_cones` | NOAA National Hurricane Center GIS cones | `event` | Active-storm forecast cone KMZ/KML polygons. |
| `nifc_fire_perimeters` | NIFC WFIGS fire perimeters | `event` | Current wildland-fire perimeter polygons with acreage and containment metadata. |
| `nifc_wildfires` | NIFC WFIGS wildfire incidents | `event` | Active U.S. wildfire incidents. |
| `noaa_cpc_global_tropics_hazards` | NOAA CPC Global Tropics Hazards | `event` | Week-2 and week-3 KML outlook layers. |
| `noaa_coastal_forecast` | NOAA NOS/STOFS via NOMADS | `event` | Product-availability monitor for NOAA coastal operational forecast systems. |
| `noaa_coops_ports` | NOAA CO-OPS / PORTS | `event` | Latest water-level, meteorological, and port-condition observations from selected NOAA tide stations. |
| `noaa_military_sua` | NOAA MarineCadastre / U.S. Navy COP | `event` | Military Special Use Airspace polygons used as static proximity context. |
| `noaa_nwm` | NOAA National Water Model | `event` | Product-availability monitor. |
| `noaa_tsunami` | NOAA/NWS tsunami warning centers | `event` | National and Pacific Tsunami Warning Center Atom messages. |
| `notam_faa` | FAA NOTAM API | `event` | Authenticated FAA NOTAM records. |
| `notam_rss` | Configured NOTAM RSS feeds | `event` | Generic aeronautical RSS mirror from `NOTAM_RSS_FEEDS`. |
| `notam_tfr` | FAA Temporary Flight Restrictions | `event` | Keyless FAA TFR export records. |
| `ntsb_aviation_accidents` | NTSB aviation accident data download directory | `event` | Public NTSB aviation accident dataset and weekly update-file publication metadata. |
| `nuclear_facilities` | GeoNuclearData | `polygon_static` | Remote static nuclear facility features imported by `cmd/importer features`. |
| `nws_alerts` | NOAA/NWS API | `event` | Active U.S. weather alerts. |
| `nws_sigmet` | NWS Aviation Weather Center | `event` | SIGMET and AIRMET GeoJSON. |
| `official_advisories` | Australia Smartraveller and U.S. embassy alert RSS feeds | `event` | Official advisories with deterministic city, severity, action, and hazard extraction. |
| `oil_prices` | Stooq | `event` | Crude oil futures price snapshots for configured symbols. |
| `oil_refineries` | Project-authored oil refinery seed | `polygon_static` | Approximate major refinery features from bundled seed data. |
| `oil_refineries_us` | EIA / ArcGIS petroleum refineries | `feature_static` | U.S. petroleum refinery facility features. |
| `ooni` | OONI | `event` | Country/test aggregation for censorship and reachability anomalies. |
| `open_meteo_anomalies` | Open-Meteo ERA5 archive API | `event` | Daily temperature, precipitation, and wind anomalies over active watch AOIs. |
| `openaq` | OpenAQ | `event` | Global air-quality monitoring station locations. |
| `opera_dist_alert` | NASA CMR OPERA DIST-ALERT-HLS | `event` | OPERA disturbance product coverage over active AOIs. |
| `osm_settlement_context` | OpenStreetMap via Overpass | `event` | Bounded building-count settlement proxy around top active candidate AOIs. |
| `overture_maps_context` | Overture Maps | `geospatial_static` | AOI places, buildings, and transportation context loaded as static features. |
| `pagasa_phivolcs_monitor` | PAGASA and PHIVOLCS official status pages | `event` | Philippines tropical-cyclone bulletin status, PHIVOLCS earthquake rows, and volcano alert levels. |
| `pedestrian_counts` | City of Melbourne Pedestrian Counting System | `event` | Aggregate pedestrian counter observations from public city sensors; no device- or person-level tracking. |
| `peeringdb` | PeeringDB | `event` | Facility inventory refresh source for `peeringdb_facilities`. |
| `peeringdb_facilities` | PeeringDB facilities | `point_static` | Static network-infrastructure facility proximity context. |
| `pipelines` | Project-authored pipeline seed | `polygon_static` | Approximate strategic pipeline route features from bundled seed data. |
| `planned_protests` | DC Action ICS and Philly Protest calendar | `event` | Public planned demonstration calendars with city-level geospatial fallback. |
| `population_h3_exposure` | Derived H3 exposure layer | `event` | H3 exposure context derived from population sources for signal consumption. |
| `portwatch_disruptions` | IMF PortWatch disruptions | `event` | Active and recent port disruption events. |
| `portwatch_port_activity` | IMF PortWatch port activity | `event` | Recent tanker-call activity aggregates for selected busy ports. |
| `power_plants` | WRI Global Power Plant Database | `polygon_static` | Remote static global power plant features imported by `cmd/importer features`. |
| `public_facilities_context` | Public facility source specs | `event` | Refreshes static public facility context into the features table. |
| `public_safety_incidents` | Municipal CAD and open-data feeds | `event` | Official local fire, police dispatch, and calls-for-service feeds with coordinates. |
| `purpleair` | PurpleAir | `event` | AOI-gated PM2.5 sensors for smoke and air-release context. |
| `rail_stations_eu` | Trainline EU stations | `feature_static` | European railway station inventory. |
| `rail_stations_us` | U.S. DOT / BTS Amtrak stations | `feature_static` | U.S. Amtrak rail station features. |
| `regional_seismic` | GeoNet NZ, BGS, NRCan, Geoscience Australia, GFZ GEOFON | `event` | Regional seismic authority feeds beyond USGS and EMSC. |
| `regional_wildfires` | CAL FIRE, NSW RFS, Victoria Emergency, SA CFS, Emergency WA, InciWeb | `event` | Regional official wildfire feeds beyond NIFC. |
| `reliefweb` | OCHA ReliefWeb | `event` | Disaster records from the ReliefWeb API. |
| `rf_presence` | WSPRNet via WSPR Live | `event` | Receiver/transmitter density derived from WSPR Live. |
| `ripe_ris` | RIPE RIS Live | `event` | Watched-origin-AS BGP update and withdrawal burst summaries. |
| `road_incidents` | Public road incident feeds | `event` | Official JSON, RSS, GeoJSON, and DATEX road incident feeds from configured jurisdictions. |
| `road_traffic_flow` | Open511 and DATEX II road feeds | `event` | Public road congestion, lane restriction, and configured DATEX II measured traffic-flow signals. |
| `safecast` | Safecast | `event` | Recent citizen-science radiation measurements. |
| `sanctions` | OFAC SDN and UN Consolidated List | `event` | Daily merge into the sanctioned entities table. |
| `satnogs` | SatNOGS Network | `event` | Public satellite RF observations and pass geometry. |
| `saveecobot_radiation` | SaveEcoBot | `event` | Ukrainian city gamma radiation readings. |
| `seismic` | USGS Earthquake Hazards Program | `event` | USGS all-day earthquake GeoJSON feed. |
| `sentinel_acquisition_plan` | ESA Sentinel-1 acquisition plans | `event` | Planned acquisition KML intersected with active AOIs. |
| `sentinel_sar_change` | Sentinel-1 via Sentinel Hub / CDSE | `event` | Repeat-pair SAR backscatter change analysis over candidate AOIs. |
| `singapore_realtime` | data.gov.sg real-time environment and traffic-image APIs | `event` | Official Singapore rainfall, air-temperature, PSI, PM2.5, and traffic-camera snapshot signals. |
| `space_weather` | NOAA SWPC | `event` | Planetary K-index and space-weather alert products. |
| `spacetrack` | Space-Track | `event` | Authenticated TLE/catalogue feed. |
| `spc_storm_reports` | NOAA Storm Prediction Center | `event` | Preliminary filtered storm reports for tornado, wind damage, and hail. |
| `sports_venues_us` | ArcGIS Open Data major sports venues | `feature_static` | U.S. major sports venue features. |
| `strikes` | Legacy curated validation dataset | `polygon_static` | Curated local kinetic-event validation features present in the current local database. |
| `swdi_radar_signatures` | NOAA/NCEI SWDI | `event` | Radar-derived tornado-vortex, hail, and mesocyclone signature point detections. |
| `tle` | CelesTrak | `event` | Multi-group TLE catalogue. |
| `tor_metrics` | Tor Metrics | `event` | Country direct-user and bridge-user deltas. |
| `traffic` | Overpass API | `proxy` | On-demand `/api/overpass` proxy; no ingestion, cached per bbox. |
| `travel_advisories` | U.S. State Department and UK FCDO | `event` | Travel advisory RSS/Atom feeds mapped to country centroids. |
| `thailand_tmd_alerts` | Thai Meteorological Department Warning page | `event` | Official Thailand warning bulletins scraped from the TMD warning page. |
| `tropomi_no2` | Sentinel-5P / TROPOMI NO2 | `event` | Bounded AOI-box statistics from Sentinel-5P NO2 products. |
| `tropomi_so2` | Sentinel-5P / TROPOMI SO2 | `event` | Bounded AOI-box statistics from Sentinel-5P SO2 products. |
| `ucdp` | UCDP GED | `event` | Georeferenced event records from the UCDP API. |
| `unhcr_displacement` | UNHCR Population Statistics API | `event` | Annual displacement and host-country context priors. |
| `usgs_shakemap` | USGS ShakeMap / PAGER | `event` | Significant earthquakes enriched with impact-product metadata. |
| `utility_outages` | Public utility outage maps | `event` | Customer outage map observations from configured utilities. |
| `vaac_global` | BOM recent volcanic ash advisory mirror | `event` | Recent volcanic ash advisories covering multiple global VAACs. |
| `vaac_tokyo` | Tokyo VAAC / JMA | `event` | Volcanic ash advisories parsed from JMA public text pages. |
| `vaac_washington` | NOAA OSPO Washington VAAC | `event` | Current IWXXM volcanic ash advisories with volcano point and ash polygon where present. |
| `volcano_notices` | USGS HANS | `event` | Recent volcano notices parsed into alert/color-code events. |
| `volcanoes` | USGS elevated volcanoes | `event` | Current elevated-volcano list from the HANS public API. |
| `water_gauges` | USGS Water Services | `event` | U.S. stream and river gauge height observations. |
| `wfp_food_prices` | WFP global food prices via HDX | `event` | Food price CSV resources from HDX package metadata. |
| `wikipedia_pageviews` | Wikimedia Pageviews API | `event` | Article pageview demand-side attention over configured watch articles. |
| `wikipedia_surge` | Wikimedia EventStreams | `event` | Wikipedia recent-change edit-rate surge detector. |
| `wmo_alert_hub` | WMO Alert Hub | `event` | Current CAP alert summaries keyed to official member centroids. |
| `wmo_cap_alert_areas` | WMO Alert Hub CAP XML | `event` | Bounded CAP XML fan-out preserving polygons/circles where present. |
| `worldpop_exposure` | WorldPop API | `event` | AOI exposure sampler emitting 1 km, 5 km, and 25 km population metrics. |

## Verified Source Data And Terms

These tables cover collection-owned seed files, importers, and camera inventory
inputs where the source artifact or upstream terms have been checked.

### Bundled Seed Files

| File | Used by | Origin | License / terms | Reference |
| --- | --- | --- | --- | --- |
| `data/chokepoints.geojson` | `cmd/importer features chokepoints` | Project-authored approximate chokepoint polygons. |  |  |
| `data/pipelines.geojson` | `cmd/importer features pipelines` | Project-authored approximate strategic route lines from public route descriptions and open-map cross-checks. |  |  |
| `data/oil_refineries.geojson` | `cmd/importer features oil_refineries` | Project-authored approximate refinery points. |  |  |
| `data/desal_plants.geojson` | `cmd/importer features desal_plants` | Project-authored approximate desalination plant points. |  |  |
| `data/cameras.json` | `cctv_cameras` | 451 internal camera seed rows: 360 `TrafficVision` rows using `oktrafficradar.org` or `stream.oktraffic.org`, and 91 `Insecam` rows. |  | [OU ITS OKTraffic](https://its.ou.edu/pages/p_oktraffic.php), [ODOT disclaimer](https://www.odot.org/disclaimer-engr.htm), [Insecam](https://www.insecam.org/en/) |

### Runtime Camera Sources

| Source | Collector path | License / terms | Reference |
| --- | --- | --- | --- |
| TfL JamCam / Unified API | `cctv_cameras` | TfL Transport Data Service license. Attribution, branding, registration, and rate-limit conditions apply. | [TfL terms](https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service), [TfL data sources](https://tfl.gov.uk/corporate/data-sources) |
| OpenTrafficCamMap | `cctv_cameras` | MIT. | [Repository](https://github.com/AidanWelch/OpenTrafficCamMap), [license](https://github.com/AidanWelch/OpenTrafficCamMap/blob/master/LICENSE) |
| OpenStreetMap via Overpass | `cctv_cameras` AOI cameras | ODbL. Attribution and license notice required. | [OSM copyright](https://www.openstreetmap.org/copyright/attribution-guide/), [OSMF API usage policy](https://operations.osmfoundation.org/policies/api/) |
| TrafficVision / OKTraffic rows in `data/cameras.json` | local camera seed |  | [OU ITS OKTraffic](https://its.ou.edu/pages/p_oktraffic.php), [ODOT disclaimer](https://www.odot.org/disclaimer-engr.htm) |
| Insecam rows in `data/cameras.json` | local camera seed |  | [Insecam](https://www.insecam.org/en/) |

### Runtime Collector Sources

| Source | Collector path | License / terms | Reference |
| --- | --- | --- | --- |
| EIA Weekly Petroleum Status Report | `eia_wpsr` | EIA states its website data, files, databases, reports, graphs, charts, and other information products may be used and distributed; attribution is requested when adapting content. | [WPSR](https://www.eia.gov/petroleum/supply/weekly/), [EIA copyright and reuse](https://www.eia.gov/about/copyrights_reuse.php), [EIA release site](https://ir.eia.gov/) |
| IATA Jet Fuel Price Monitor | `iata_fuel_monitor` | IATA publishes the monitor under license from S&P Global Energy/Platts; the page states the underlying data and methodology are S&P Global Platts intellectual property and the monitor is for general informational purposes. | [IATA Fuel Monitor](https://www.iata.org/en/publications/economics/fuel-monitor/) |
| IATA Operational Safety Audit Registry | `iata_iosa_registry` | IATA-controlled registry landing page; row-level IATA Connect access is monitored as a protected upstream registry URL. | [IOSA](https://www.iata.org/en/programs/safety/audit/iosa/registry/), [IATA Connect IOSA registry](https://ic.iata.org/registry/iosa?page=1) |
| IATA Standard Safety Assessment Registry | `iata_issa_registry` | IATA-controlled public ISSA registry table. | [ISSA registry](https://www.iata.org/en/programs/safety/audit/issa/registry/) |
| European Commission EU Air Safety List | `eu_air_safety_list` | The page states the authentic list is the latest applicable Commission Implementing Regulation as published in the Official Journal. | [EU Air Safety List](https://transport.ec.europa.eu/transport-themes/eu-air-safety-list_en) |
| FAA International Aviation Safety Assessment Program Results | `faa_iasa` | FAA public IASA results page and PDF. | [FAA IASA](https://www.faa.gov/about/initiatives/iasa), [IASA results](https://www.faa.gov/about/initiatives/iasa/iasa-program-results) |
| ICAO USOAP Safety Audit Results | `icao_usoap` | ICAO public interactive viewer; the page references the iSTARS API Data Service for developers. | [ICAO USOAP viewer](https://www.icao.int/safety-audit-results-usoap-interactive-viewer) |
| FAA Service Difficulty Reports | `faa_sdr` | FAA AVInfo public yearly SDR CSV downloads. | [FAA SDR downloads](https://www.faa.gov/av-info/download_SDR) |
| NTSB aviation accident data | `ntsb_aviation_accidents` | NTSB states downloadable aviation accident data sets are available for public use. | [NTSB Accident Data](https://www.ntsb.gov/safety/data/Pages/Data_Stats.aspx), [NTSB download directory](https://data.ntsb.gov/avdata) |
| EASA Conflict Zone Information Bulletins | `easa_czib` | EASA public CZIB page with JSON, CSV, and RSS alternates. | [EASA CZIB](https://www.easa.europa.eu/en/domains/air-operations/czibs) |
| FAA Prohibitions, Restrictions and Notices | `faa_flight_restrictions` | FAA public service page for U.S. flight prohibitions, restrictions, advisories, SFARs, and pointer documents. | [FAA restrictions](https://www.faa.gov/air_traffic/publications/us_restrictions/) |
| U.S. DOT Certificated Air Carriers List | `dot_certificated_carriers` | DOT page states the list includes addresses, phone numbers, and type of authority held. | [DOT carrier list](https://www.transportation.gov/policy/aviation-policy/certificated-air-carriers-list) |
| Configured airline report URLs | `airline_reports_monitor` | Terms are those of each configured report URL. | `AIRLINE_REPORT_URLS` |
| NOAA CO-OPS / PORTS | `noaa_coops_ports` | NOAA CO-OPS Data API access to water levels, predictions, currents, meteorological observations, and station products. | [CO-OPS API](https://api.tidesandcurrents.noaa.gov/api/prod/), [CO-OPS products](https://tidesandcurrents.noaa.gov/products.html) |
| NOAA NOS/STOFS coastal forecast products on NOMADS | `noaa_coastal_forecast` | NOAA NOMADS public product distribution; collector stores product availability metadata only. | [NOAA OFS](https://tidesandcurrents.noaa.gov/models), [NOMADS](https://nomads.ncep.noaa.gov/) |
| MobilityData Mobility Database Catalogs | `gtfs_feed_catalog` | Catalog metadata is CC0; individual transit feeds remain subject to the upstream transit provider's terms. | [Catalog repository](https://github.com/MobilityData/mobility-database-catalogs), [Mobility Database FAQ](https://mobilitydatabase.org/faq) |
| Open511 and configured DATEX II road feeds | `road_traffic_flow` | Terms depend on the configured upstream feed; the default Open511 source is DriveBC's public Open511 API. | [Open511](https://www.open511.org/), [DriveBC Open511 API](https://api.open511.gov.bc.ca/help), [DATEX II](https://datex2.eu/specifications/) |
| Network Rail Open Data Train Movements | `network_rail_train_movements` | Access requires a Network Rail Open Data account and is governed by Network Rail's open-data terms and branding restrictions. | [Network Rail Open Data](https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/), [Open Rail Data Wiki](https://wiki.openraildata.com/index.php/Train_Movements) |
| City of Melbourne Pedestrian Counting System | `pedestrian_counts` | City of Melbourne Open Data dataset metadata reports CC BY. | [City pedestrian data](https://data.melbourne.vic.gov.au/), [Pedestrian visualisation](https://www.pedestrian.melbourne.vic.gov.au/) |
| NOAA / BOEM MarineCadastre.gov AIS catalog | `marinecadastre_ais_catalog` | Collector stores file catalog metadata only; bulk AIS downloads are large and should be imported explicitly. | [MarineCadastre AIS](https://marinecadastre.gov/ais/), [NOAA AccessAIS](https://www.coast.noaa.gov/digitalcoast/tools/ais.html) |
| EMODnet Human Activities Vessel Density | `emodnet_vessel_density` | EMODnet Human Activities describes datasets as free and without restrictions; ERDDAP metadata is retained in event props. | [EMODnet Human Activities](https://emodnet.ec.europa.eu/human-activities), [ERDDAP vessel density](https://erddap.emodnet.eu/erddap/wms/humanactivities_e929_c26d_18a2/index.html) |
| BMKG Data Terbuka Gempabumi | `bmkg_indonesia` |  | [BMKG earthquake data](https://data.bmkg.go.id/gempabumi/) |
| METMalaysia Weather Warning API | `malaysia_weather_warnings` |  | [METMalaysia open data](https://www.met.gov.my/en/info/data-terbuka/), [data.gov.my weather warning API](https://api.data.gov.my/weather/warning/) |
| Thai Meteorological Department warnings | `thailand_tmd_alerts` |  | [TMD warning page](https://www.tmd.go.th/en/warning-and-events/warning-storm) |
| data.gov.sg real-time APIs | `singapore_realtime` | Singapore Open Data Licence applies unless otherwise stated; API access is also subject to data.gov.sg API terms. | [data.gov.sg API guide](https://guide.data.gov.sg/developer-guide), [Singapore Open Data Licence](https://data.gov.sg/open-data-licence), [privacy and terms](https://data.gov.sg/privacy-and-terms) |
| PAGASA and PHIVOLCS official status pages | `pagasa_phivolcs_monitor` |  | [PAGASA tropical cyclone bulletin](https://bagong.pagasa.dost.gov.ph/tropical-cyclone/severe-weather-bulletin), [PHIVOLCS earthquakes](https://earthquake.phivolcs.dost.gov.ph/), [PHIVOLCS WOVOdat](https://wovodat.phivolcs.dost.gov.ph/) |
| ASEAN Specialised Meteorological Centre hotspot counts | `asean_haze_hotspots` | ASMC terms apply; ASMC notes certain products are derived from external agencies and may also be subject to those agencies' terms. | [ASMC hotspot page](https://asmc.asean.org/asmc-haze-hotspot-daily-new/), [ASMC terms](https://asmc.asean.org/terms-of-use/) |

### Remote Feature Importers

| Importer source | Command | License / terms | Reference |
| --- | --- | --- | --- |
| WRI Global Power Plant Database | `cmd/importer features power_plants` | CC BY 4.0 for database release; MIT for source code. | [WRI Global Power Plant Database](https://github.com/wri/global-power-plant-database) |
| GeoNuclearData | `cmd/importer features nuclear_facilities` | ODbL for database; Database Contents License for contents. | [GeoNuclearData](https://github.com/cristianst85/GeoNuclearData) |
| TeleGeography Submarine Cable Map | `cmd/importer features cables` | Raw geocoded map data is available via TeleGeography data license; map references/screenshots are CC BY-SA 4.0. | [TeleGeography Submarine Cable FAQ](https://www2.telegeography.com/submarine-cable-faqs-frequently-asked-questions) |

## Kind Notes

| Kind | Meaning |
| --- | --- |
| `event` | Scheduled or streaming source writing raw event rows. |
| `feature_inventory` | Collector-maintained feature inventory. |
| `geospatial_static` | Configured static geospatial feature source. |
| `feature_static`, `point_static`, `polygon_static` | Explicitly imported or migration-seeded feature source. |
| `point_timeseries` | Point stream or time-series source. |
| `proxy` | Registered compatibility/proxy source ID. |

## Optional Credentials And Enable Flags

| Env var | Source IDs | Purpose |
| --- | --- | --- |
| `ACLED_EMAIL`, `ACLED_PASSWORD` | `acled` | Conflict event API access. |
| `CF_RADAR_TOKEN` | `cloudflare_radar` | Cloudflare Radar API access. |
| `OPENAQ_KEY` | `openaq` | OpenAQ API access. |
| `PURPLEAIR_API_KEY` | `purpleair` | PurpleAir API access. |
| `UCDP_ACCESS_TOKEN` | `ucdp` | UCDP GED API access. |
| `NVD_API_KEY` | `cisa_kev` | Optional NVD enrichment budget. |
| `AISSTREAM_KEY`, `GORDIOS_ENABLE_MARITIME_STREAM=1` | `maritime` | AIS stream access. |
| `GLOBALPING_TOKEN` | `globalping_measurements` | Optional Globalping API access. |
| `OPENSKY_USER`, `OPENSKY_PASS` | `flights` | Optional OpenSky credentials. |
| `CDSE_*`, `SENTINELHUB_*` | `copernicus_sentinel`, `sentinel_acquisition_plan`, `tropomi_no2`, `tropomi_so2` | Copernicus/CDSE and Sentinel Hub access. |
| `SPACETRACK_USER`, `SPACETRACK_PASS` | `spacetrack` | Space-Track login. |
| `ENTSOE_TOKEN` | `entsoe_outage` | ENTSO-E API access. |
| `GFW_TOKEN` | `gfw_events` | Global Fishing Watch API access. |
| `EIA_API_KEY` | `eia930` | EIA grid data. |
| `FIRMS_MAP_KEY` | `geo_thermal` | FIRMS map API access for geothermal hotspot monitoring. |
| `FAA_SDR_YEAR`, `FAA_SDR_MAX_ROWS` | `faa_sdr` | Optional SDR year override and max current-year rows retained per fetch. |
| `AIRLINE_REPORT_URLS` | `airline_reports_monitor` | Comma-separated official report URLs to monitor. |
| `NOTAM_CLIENT_ID`, `NOTAM_CLIENT_SECRET` | `notam_faa` | FAA NOTAM API access. |
| `NOTAM_RSS_FEEDS` | `notam_rss` | Configured NOTAM RSS feeds. |
| `RELIEFWEB_APPNAME` or `GORDIOS_ENABLE_RELIEFWEB=1` | `reliefweb` | ReliefWeb API identification or explicit enable. |
| `GTFS_RT_*` | `gtfs_realtime` | Agency-specific GTFS realtime feeds. |
| `BMKG_*`, `MALAYSIA_WEATHER_WARNINGS_*`, `THAILAND_TMD_*`, `SINGAPORE_REALTIME_*`, `PAGASA_PHIVOLCS_*`, `PHIVOLCS_*`, `ASMC_HAZE_*` | `bmkg_indonesia`, `malaysia_weather_warnings`, `thailand_tmd_alerts`, `singapore_realtime`, `pagasa_phivolcs_monitor`, `asean_haze_hotspots` | Optional source tuning; no API key required by default. |
| `MARTRACK_USER`, `MARTRACK_PASS`, `GORDIOS_ENABLE_MARTRACK=1` | `martrack` | Maritime tracking source. |
| `OVERTURE_MAPS_GEOJSON_URLS`, `OVERTURE_MAPS_GEOJSON_FILES` | `overture_maps_context` | Configured Overture/static geospatial inputs. |
| `GFM_TOKEN`, `CEMS_GFM_TOKEN`, `COPERNICUS_GFM_TOKEN` | `cems_gfm` | Optional Copernicus GFM token. |

## Feature Importers

`cmd/importer features` supports local curated seeds and remote cached seeds.
Current importer source IDs are:

| Source ID | Origin |
| --- | --- |
| `chokepoints` | `local:chokepoints.geojson` |
| `pipelines` | `local:pipelines.geojson` |
| `oil_refineries` | `local:oil_refineries.geojson` |
| `desal_plants` | `local:desal_plants.geojson` |
| `cables` | `remote:https://www.submarinecablemap.com/api/v3/cable/cable-geo.json` |
| `nuclear_facilities` | `remote:https://raw.githubusercontent.com/cristianst85/GeoNuclearData/master/data/json/denormalized/nuclear_power_plants.json` |
| `power_plants` | `remote:https://raw.githubusercontent.com/wri/global-power-plant-database/master/output_database/global_power_plant_database.csv` |

Local seed files are documented in `data/README.md`. Source-data terms are
tracked in this file.

## Verification

To check this catalog against a running collection gateway:

```sh
curl -fsS http://localhost:18080/api/sources/status \
  | jq -r '.sources[].id' \
  | sort
```
