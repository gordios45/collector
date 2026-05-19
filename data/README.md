# Local Feature Seeds

These files are optional background/context seeds for `cmd/importer features`.
They are approximate fixtures for local analysis.

| File | Feature source | Provenance |
| --- | --- | --- |
| `chokepoints.geojson` | `chokepoints` | Approximate hand-drawn polygons around major maritime chokepoints, based on public geographic knowledge. |
| `pipelines.geojson` | `pipelines` | Approximate strategic pipeline routes from public operator/route descriptions and open map cross-checks. |
| `oil_refineries.geojson` | `oil_refineries` | Approximate major refinery points from public facility names, countries, operators, and capacity references. |
| `desal_plants.geojson` | `desal_plants` | Approximate major desalination plant points from public facility names, countries, operators, technology, and capacity references. |
| `cameras.json` | `cctv_cameras` | Internal static camera seed used by the `cctv_cameras` collector together with TfL, OpenTrafficCamMap, and AOI-scoped OSM. |

Source-data terms are tracked in [`SOURCES.md`](../SOURCES.md).
Prefer remote, cached importers when a stable upstream URL exists. Current
remote feature importers are listed with:

```sh
go run ./cmd/importer features -list
```
