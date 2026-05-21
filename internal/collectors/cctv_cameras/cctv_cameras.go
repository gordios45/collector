// Copyright 2026 Gordios45 contributors
// SPDX-License-Identifier: Apache-2.0

// Package cctv_cameras ingests camera inventory metadata into features.
package cctv_cameras

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gordios45/collector/internal/collectors/collectorutil"
	"github.com/gordios45/collector/internal/features"
	"github.com/gordios45/collector/internal/httpx"
	propx "github.com/gordios45/collector/internal/props"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceID               = "cctv_cameras"
	tflJamCamURL           = "https://api.tfl.gov.uk/Place/Type/JamCam"
	otcmListURL            = "https://api.github.com/repos/AidanWelch/OpenTrafficCamMap/contents/cameras"
	otcmRawFallback        = "https://raw.githubusercontent.com/AidanWelch/OpenTrafficCamMap/master/cameras/USA.json"
	asfinagWebcamsURL      = "https://odo.asfinag.at/odo/rest/sec/resource/001/json/webcams?language=atDE"
	ontario511CamerasURL   = "https://511on.ca/api/v2/get/cameras"
	alberta511CamerasURL   = "https://511.alberta.ca/api/v2/get/cameras"
	travelMidwestCameraURL = "https://www.travelmidwest.com/lmiga/cameraReport.json"
	singaporeTrafficURL    = "https://api.data.gov.sg/v1/transport/traffic-images"
	overpassURL            = "https://overpass-api.de/api/interpreter"
)

type Collector struct {
	pool        *pgxpool.Pool
	client      *http.Client
	cameraFile  string
	maxAOIs     int
	defaultOSMR int
	maxOSMR     int
}

func New(pool *pgxpool.Pool) (*Collector, error) {
	if os.Getenv("GORDIOS_DISABLE_CCTV_CAMERAS") == "1" {
		return nil, errors.New("disabled via GORDIOS_DISABLE_CCTV_CAMERAS=1")
	}
	if pool == nil {
		return nil, errors.New("nil db pool")
	}
	return &Collector{
		pool:        pool,
		client:      collectorutil.HTTPClient(90 * time.Second),
		cameraFile:  strings.TrimSpace(os.Getenv("CCTV_CAMERAS_FILE")),
		maxAOIs:     collectorutil.EnvInt("CCTV_CAMERAS_OSM_MAX_AOIS", 10, 0, 50),
		defaultOSMR: collectorutil.EnvInt("CCTV_CAMERAS_OSM_DEFAULT_RADIUS_M", 5000, 100, 50000),
		maxOSMR:     collectorutil.EnvInt("CCTV_CAMERAS_OSM_MAX_RADIUS_M", 20000, 1000, 100000),
	}, nil
}

func (c *Collector) ID() string               { return sourceID }
func (c *Collector) PollEvery() time.Duration { return 24 * time.Hour }

func (c *Collector) FetchFeatures(ctx context.Context) ([]features.Feature, error) {
	now := time.Now().UTC()
	out := map[string]features.Feature{}

	c.addTfL(ctx, now, out)
	c.addASFINAG(ctx, now, out)
	c.addOntario511(ctx, now, out)
	c.addAlberta511(ctx, now, out)
	c.addTravelMidwest(ctx, now, out)
	c.addSingaporeTrafficImages(ctx, now, out)
	c.addOTCM(ctx, now, out)
	c.addCameraFile(now, out)
	c.addOSMAOIs(ctx, now, out)

	if len(out) == 0 {
		return nil, errors.New("no cctv camera features fetched")
	}
	feats := make([]features.Feature, 0, len(out))
	for _, f := range out {
		feats = append(feats, f)
	}
	return feats, nil
}

func (c *Collector) addTfL(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var rows []struct {
		ID                   string  `json:"id"`
		CommonName           string  `json:"commonName"`
		Lat                  float64 `json:"lat"`
		Lon                  float64 `json:"lon"`
		AdditionalProperties []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"additionalProperties"`
	}
	headers := map[string]string{
		"Accept":     "application/json",
		"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
	}
	if err := httpx.GetJSONWithClient(ctx, c.client, tflJamCamURL, headers, &rows); err != nil {
		return
	}
	for _, row := range rows {
		if !collectorutil.ValidLatLon(row.Lat, row.Lon) {
			continue
		}
		props := map[string]any{}
		for _, p := range row.AdditionalProperties {
			switch p.Key {
			case "imageUrl", "cameraImageUrl":
				propx.SetNonEmpty(props, "thumb_url", p.Value)
			case "videoUrl":
				propx.SetNonEmpty(props, "stream_url", p.Value)
			}
		}
		name := propx.FirstNonEmpty(row.CommonName, "TfL JamCam")
		props = cameraProps(props, now, "tfl_jamcam", "TfL JamCam", tflJamCamURL, "https://api.tfl.gov.uk/", name, "GB")
		put(out, "tfl:"+row.ID, row.Lat, row.Lon, props)
	}
}

func (c *Collector) addASFINAG(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var rows []map[string]any
	headers := map[string]string{
		"Accept":        "application/json",
		"Authorization": "Basic bWFwX3dpZGdldDp0ZWdkaXc=",
		"Referer":       "https://www.asfinag.at/",
		"Origin":        "https://www.asfinag.at",
	}
	if err := httpx.GetJSONWithClient(ctx, c.client, asfinagWebcamsURL, headers, &rows); err != nil {
		return
	}
	addASFINAGRows(now, out, rows)
}

func addASFINAGRows(now time.Time, out map[string]features.Feature, rows []map[string]any) {
	for _, row := range rows {
		id := firstString(row, "wcs_id", "id")
		if strings.HasPrefix(id, "Utinform") {
			continue
		}
		lat, latOK := firstFloat(row, "wgs84_lat", "lat", "latitude")
		lon, lonOK := firstFloat(row, "wgs84_lon", "lon", "lng", "longitude")
		img := firstString(row, "url_campic", "imageUrl", "image_url", "url")
		if id == "" || !latOK || !lonOK || img == "" || !collectorutil.ValidLatLon(lat, lon) {
			continue
		}
		name := propx.FirstNonEmpty(firstString(row, "position_txt", "direction_txt", "name"), "ASFINAG Webcam")
		props := cameraProps(map[string]any{
			"feed_url":   img,
			"thumb_url":  img,
			"camera_id":  id,
			"raw_source": "asfinag_odo",
		}, now, "asfinag_webcams", "ASFINAG", asfinagWebcamsURL, "https://www.asfinag.at/", name, "AT")
		put(out, "asfinag:"+id, lat, lon, props)
	}
}

func (c *Collector) addOntario511(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var raw any
	if err := httpx.GetJSONWithClient(ctx, c.client, ontario511CamerasURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return
	}
	addCameraMapRows(now, out, "ontario511", "ontario_511_cameras", "Ontario 511", ontario511CamerasURL, "https://511on.ca/", "CA", "Ontario 511 Camera", cameraMaps(raw, "cameras", "data"))
}

func (c *Collector) addAlberta511(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var raw any
	if err := httpx.GetJSONWithClient(ctx, c.client, alberta511CamerasURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return
	}
	addCameraMapRows(now, out, "alberta511", "alberta_511_cameras", "Alberta 511", alberta511CamerasURL, "https://511.alberta.ca/", "CA", "Alberta 511 Camera", cameraMaps(raw, "cameras", "data"))
}

func (c *Collector) addTravelMidwest(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var raw any
	if err := httpx.GetJSONWithClient(ctx, c.client, travelMidwestCameraURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return
	}
	addCameraMapRows(now, out, "travelmidwest", "travel_midwest_cameras", "Travel Midwest", travelMidwestCameraURL, "https://www.travelmidwest.com/", "US", "Travel Midwest Camera", cameraMaps(raw, "cameraReports", "cameras", "data"))
}

func (c *Collector) addSingaporeTrafficImages(ctx context.Context, now time.Time, out map[string]features.Feature) {
	var raw struct {
		Items []struct {
			Timestamp string `json:"timestamp"`
			Cameras   []struct {
				Timestamp string `json:"timestamp"`
				Image     string `json:"image"`
				CameraID  string `json:"camera_id"`
				Location  struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"location"`
			} `json:"cameras"`
		} `json:"items"`
	}
	if err := httpx.GetJSONWithClient(ctx, c.client, singaporeTrafficURL, map[string]string{"Accept": "application/json"}, &raw); err != nil {
		return
	}
	for _, item := range raw.Items {
		for _, cam := range item.Cameras {
			lat, lon := cam.Location.Latitude, cam.Location.Longitude
			if cam.CameraID == "" || !collectorutil.ValidLatLon(lat, lon) {
				continue
			}
			props := cameraProps(map[string]any{
				"feed_url":          cam.Image,
				"thumb_url":         cam.Image,
				"camera_id":         cam.CameraID,
				"snapshot_time":     cam.Timestamp,
				"collection_time":   item.Timestamp,
				"source_event_link": "singapore_realtime",
			}, now, "singapore_lta_traffic_images", "data.gov.sg / LTA Singapore", singaporeTrafficURL, "https://data.gov.sg/", "Singapore Traffic Camera "+cam.CameraID, "SG")
			put(out, "sg-lta:"+cam.CameraID, lat, lon, props)
		}
	}
}

func (c *Collector) addOTCM(ctx context.Context, now time.Time, out map[string]features.Feature) {
	files := c.otcmFiles(ctx)
	for _, file := range files {
		var data any
		if err := httpx.GetJSONWithClient(ctx, c.client, file.URL, map[string]string{"Accept": "application/json"}, &data); err != nil {
			continue
		}
		c.walkOTCM(now, out, file.Country, file.URL, data)
	}
}

type otcmFile struct {
	Country string
	URL     string
}

func (c *Collector) otcmFiles(ctx context.Context) []otcmFile {
	var raw []struct {
		Name        string `json:"name"`
		DownloadURL string `json:"download_url"`
	}
	if err := httpx.GetJSONWithClient(ctx, c.client, otcmListURL, map[string]string{"Accept": "application/vnd.github+json"}, &raw); err != nil {
		return []otcmFile{{Country: "USA", URL: otcmRawFallback}}
	}
	out := []otcmFile{}
	for _, f := range raw {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".json") {
			continue
		}
		u := propx.FirstNonEmpty(f.DownloadURL, "https://raw.githubusercontent.com/AidanWelch/OpenTrafficCamMap/master/cameras/"+f.Name)
		out = append(out, otcmFile{Country: strings.ToUpper(strings.TrimSuffix(f.Name, ".json")), URL: u})
		if len(out) >= 15 {
			break
		}
	}
	if len(out) == 0 {
		out = append(out, otcmFile{Country: "USA", URL: otcmRawFallback})
	}
	return out
}

func (c *Collector) walkOTCM(now time.Time, out map[string]features.Feature, country, endpoint string, v any) {
	switch x := v.(type) {
	case []any:
		for _, item := range x {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			lat, latOK := propx.Float(m["latitude"])
			lon, lonOK := propx.Float(m["longitude"])
			if !latOK || !lonOK || !collectorutil.ValidLatLon(lat, lon) {
				continue
			}
			name := propx.FirstNonEmpty(propx.StringAt(m, "description"), propx.StringAt(m, "name"), "Traffic Cam")
			streamURL := propx.StringAt(m, "url")
			props := cameraProps(map[string]any{
				"stream_url": streamURL,
			}, now, "open_traffic_cam_map", "OpenTrafficCamMap", endpoint, "https://github.com/AidanWelch/OpenTrafficCamMap", name, country)
			id := "otcm:" + country + ":" + collectorutil.StableID(fmt.Sprintf("%.6f|%.6f|%s", lat, lon, streamURL))
			put(out, id, lat, lon, props)
		}
	case map[string]any:
		for _, child := range x {
			c.walkOTCM(now, out, country, endpoint, child)
		}
	}
}

func (c *Collector) addCameraFile(now time.Time, out map[string]features.Feature) {
	path, body, ok := c.readCameraFile()
	if !ok {
		return
	}
	var raw struct {
		Cameras []struct {
			Source    string  `json:"source"`
			Name      string  `json:"name"`
			Lat       float64 `json:"lat"`
			Lon       float64 `json:"lon"`
			StreamURL string  `json:"streamUrl"`
			FeedType  string  `json:"feedType"`
			City      string  `json:"city"`
			State     string  `json:"state"`
			Country   string  `json:"country"`
			Road      string  `json:"road"`
			Direction string  `json:"direction"`
		} `json:"cameras"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return
	}
	for _, cam := range raw.Cameras {
		if !collectorutil.ValidLatLon(cam.Lat, cam.Lon) {
			continue
		}
		provider := propx.FirstNonEmpty(cam.Source, "Camera seed")
		props := cameraProps(map[string]any{
			"stream_url": cam.StreamURL,
			"feed_type":  cam.FeedType,
			"city":       cam.City,
			"state":      cam.State,
			"road":       cam.Road,
			"direction":  cam.Direction,
		}, now, "camera_seed_file", provider, "file:"+path, "file:"+path, propx.FirstNonEmpty(cam.Name, "Camera"), cam.Country)
		id := "camera_file:" + strings.ToLower(provider) + ":" + collectorutil.StableID(fmt.Sprintf("%.5f|%.5f|%s", cam.Lat, cam.Lon, cam.StreamURL))
		put(out, id, cam.Lat, cam.Lon, props)
	}
}

func (c *Collector) readCameraFile() (string, []byte, bool) {
	paths := []string{}
	if c.cameraFile != "" {
		paths = append(paths, c.cameraFile)
	}
	paths = append(paths,
		"data/cameras.json",
		filepath.Join("collection", "data", "cameras.json"),
		filepath.Join("..", "..", "data", "cameras.json"),
		filepath.Join("..", "..", "..", "collection", "data", "cameras.json"),
	)
	for _, p := range paths {
		body, err := os.ReadFile(p)
		if err == nil {
			return p, body, true
		}
	}
	return "", nil, false
}

func (c *Collector) addOSMAOIs(ctx context.Context, now time.Time, out map[string]features.Feature) {
	if c.maxAOIs <= 0 {
		return
	}
	aois := collectorutil.ConfiguredAOIs(ctx, c.pool, sourceID, c.maxAOIs)
	for _, a := range aois {
		radiusM := c.defaultOSMR
		if a.RadiusM > 0 {
			radiusM = int(a.RadiusM)
		}
		if radiusM > c.maxOSMR {
			radiusM = c.maxOSMR
		}
		rows, err := c.fetchOSMCameras(ctx, a.Lat, a.Lon, radiusM)
		if err != nil {
			continue
		}
		for _, row := range rows {
			if !collectorutil.ValidLatLon(row.Lat, row.Lon) {
				continue
			}
			name := propx.FirstNonEmpty(row.Tags["name"], row.Tags["description"], "OSM surveillance camera")
			props := cameraProps(map[string]any{
				"watch_aoi_id":       a.ID,
				"watch_aoi_label":    a.Label,
				"watch_aoi_kind":     a.Kind,
				"watch_aoi_radius_m": radiusM,
				"osm_id":             row.ID,
				"osm_tags":           row.Tags,
			}, now, "osm_overpass_aoi", "OpenStreetMap / Overpass", overpassURL, "https://www.openstreetmap.org/", name, "")
			put(out, fmt.Sprintf("osm:%s:%d", a.ID, row.ID), row.Lat, row.Lon, props)
		}
	}
}

type osmElement struct {
	ID   int64             `json:"id"`
	Lat  float64           `json:"lat"`
	Lon  float64           `json:"lon"`
	Tags map[string]string `json:"tags"`
}

func (c *Collector) fetchOSMCameras(ctx context.Context, lat, lon float64, radiusM int) ([]osmElement, error) {
	q := fmt.Sprintf(`[out:json][timeout:25];
node["man_made"="surveillance"](around:%d,%.6f,%.6f);
out body 500;`, radiusM, lat, lon)
	form := url.Values{}
	form.Set("data", q)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, overpassURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gordios-cctv-cameras/0.1")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("overpass status %d", resp.StatusCode)
	}
	var raw struct {
		Elements []osmElement `json:"elements"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return raw.Elements, nil
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if s := strings.TrimSpace(propx.StringAt(m, key)); s != "" {
			return s
		}
	}
	return ""
}

func firstFloat(m map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if f, ok := propx.Float(v); ok {
				return f, true
			}
		}
	}
	return 0, false
}

func cameraMaps(v any, childKeys ...string) []map[string]any {
	switch x := v.(type) {
	case []any:
		out := make([]map[string]any, 0, len(x))
		for _, item := range x {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	case map[string]any:
		for _, key := range childKeys {
			if child, ok := x[key]; ok {
				if rows := cameraMaps(child, childKeys...); len(rows) > 0 {
					return rows
				}
			}
		}
	}
	return nil
}

func firstViewURL(m map[string]any) string {
	for _, key := range []string{"Views", "views"} {
		raw, ok := m[key]
		if !ok {
			continue
		}
		rows, ok := raw.([]any)
		if !ok {
			continue
		}
		for _, item := range rows {
			view, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if s := firstString(view, "Url", "url", "ImageUrl", "imageUrl"); s != "" {
				return s
			}
		}
	}
	return ""
}

func addCameraMapRows(now time.Time, out map[string]features.Feature, idPrefix, inventorySource, provider, endpoint, publicURL, country, defaultName string, rows []map[string]any) {
	for i, row := range rows {
		lat, latOK := firstFloat(row, "latitude", "Latitude", "lat", "Lat")
		lon, lonOK := firstFloat(row, "longitude", "Longitude", "lng", "lon", "Long")
		img := firstString(row, "imageUrl", "ImageUrl", "imageURL", "ImageURL", "Url", "url")
		if img == "" {
			img = firstViewURL(row)
		}
		if !latOK || !lonOK || !collectorutil.ValidLatLon(lat, lon) {
			continue
		}
		id := propx.FirstNonEmpty(firstString(row, "id", "Id", "cameraId", "CameraId", "cameraID", "CameraID"), collectorutil.StableID(fmt.Sprintf("%.6f|%.6f|%s", lat, lon, img)))
		name := propx.FirstNonEmpty(firstString(row, "cameraName", "CameraName", "description", "Description", "Location", "location", "name", "Name", "roadway", "Roadway"), defaultName)
		props := cameraProps(map[string]any{
			"feed_url":     img,
			"thumb_url":    img,
			"camera_id":    id,
			"source_index": i,
		}, now, inventorySource, provider, endpoint, publicURL, name, country)
		put(out, idPrefix+":"+id, lat, lon, props)
	}
}

func cameraProps(base map[string]any, seen time.Time, inventorySource, provider, endpoint, publicURL, name, country string) map[string]any {
	props := map[string]any{
		"name":                  name,
		"inventory_source":      inventorySource,
		"source_provider":       provider,
		"source_api_endpoint":   endpoint,
		"source_public_url":     publicURL,
		"source_kind":           "camera_inventory",
		"last_seen_at":          seen.Format(time.RFC3339),
		"stream_reachable_hint": streamReachable(propx.StringAt(base, "stream_url")),
	}
	propx.SetNonEmpty(props, "country", country)
	for k, v := range base {
		if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		props[k] = v
	}
	return props
}

func put(out map[string]features.Feature, id string, lat, lon float64, props map[string]any) {
	if !collectorutil.ValidLatLon(lat, lon) || strings.TrimSpace(id) == "" {
		return
	}
	out[id] = features.Feature{
		ExtID:   id,
		GeomWKT: fmt.Sprintf("POINT(%f %f)", lon, lat),
		Props:   props,
	}
}

func streamReachable(rawURL string) bool {
	if rawURL == "" {
		return true
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return true
	}
	host := strings.ToLower(u.Hostname())
	for _, token := range []string{"sfs-lr-", ".dot.ga.gov", "insecam.org"} {
		if strings.Contains(host, token) {
			return false
		}
	}
	return true
}
