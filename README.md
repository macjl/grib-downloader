# grib-downloader

Containerised GRIB2 downloader for marine weather — feeds
[signalk-grib-weather-provider](https://github.com/macjl/signalk-grib-weather-provider)
by writing forecast files into the directories it scans.

One image, one instance per source: each configured source has its own model,
directory, schedule and failure isolation.

## Supported sources

| Model | Distribution | Subsetting | Typical size |
|---|---|---|---|
| `gfs` (0.25°/0.5°/1°) | NOAA [NOMADS grib_filter](https://nomads.ncep.noaa.gov/) | **server-side bbox + variables** | ~15 kB / step (small bbox) |
| `arome` (0.025°/0.01°) | Météo-France open bucket (no API key) | parameter packages × time-range groups, whole domain | ~56 MB / 6 h group (SP1 0.025°) |
| `arpege` (0.1° Europe / 0.25° global) | Météo-France open bucket | same | 0.1°: tens of MB / group; 0.25°: ~255 MB / 24 h |
| `icon-eu` | DWD [opendata.dwd.de](https://opendata.dwd.de/weather/nwp/icon-eu/grib/) | per-variable files, merged per step | ~14 MB / step (8 vars, full domain) |

Notes:
- AROME `SP1` covers wind, gust, temperature, humidity, MSL pressure and
  precipitation. Cloud cover is in `SP2` — add it to `packages` if needed.
- ICON-EU per-variable files are decompressed and **concatenated into one GRIB
  per forecast step**, as the provider expects all variables of a validity time
  in a single file.
- ICON *global* is on an icosahedral grid and is not supported (the provider
  needs regular lat/lon grids) — use ICON-EU.

## Behaviour

- Discovers the latest published run (with availability probing and fallback
  to the previous cycle), downloads only what is missing.
- Atomic writes (`.part` → `.grb2`) — a concurrent scanner never sees a
  partial file.
- A run is marked complete with a `.run-<stamp>.complete` marker; files from
  older runs are then deleted (`keep_runs`, default 1). The provider purges
  the corresponding caches automatically.
- `--once` for cron/job-style execution, `--loop` for a long-running container
  (poll every `interval_minutes`, default 10).

## Usage

```sh
docker build -t grib-downloader .

# single pass, all sources
docker run --rm \
  -v $PWD/config.yaml:/config.yaml:ro \
  -v /path/to/gribs:/data \
  grib-downloader --once

# one long-running instance per source
docker run -d --restart unless-stopped \
  -v $PWD/config.yaml:/config.yaml:ro \
  -v /path/to/gribs:/data \
  grib-downloader --loop --source gfs-025
```

See [config.example.yaml](config.example.yaml) for the configuration format.

### docker-compose

```yaml
services:
  grib-gfs:
    image: grib-downloader
    command: ["--loop", "--source", "gfs-025"]
    volumes: ["./config.yaml:/config.yaml:ro", "./gribs:/data"]
    restart: unless-stopped
  grib-arome:
    image: grib-downloader
    command: ["--loop", "--source", "arome-0025"]
    volumes: ["./config.yaml:/config.yaml:ro", "./gribs:/data"]
    restart: unless-stopped
  grib-icon:
    image: grib-downloader
    command: ["--loop", "--source", "icon-eu"]
    volumes: ["./config.yaml:/config.yaml:ro", "./gribs:/data"]
    restart: unless-stopped
```

## Bandwidth at sea

GFS via NOMADS is the only source with server-side subsetting — a 15°×12° bbox
at 0.25° costs a few hundred kB for a full short-term run. Météo-France and
DWD distribute whole-domain files; plan accordingly on metered connections
(AROME SP1 0–24 h ≈ 230 MB, ICON-EU 0–48 h ≈ 240 MB).

## License

Apache-2.0
