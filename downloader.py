#!/usr/bin/env python3
"""GRIB downloader for marine weather sources.

Downloads forecast GRIB2 files from public model distributions into local
directories — designed to feed signalk-grib-weather-provider, which scans
those directories and serves the data through the SignalK Weather API.

Supported models:
  gfs       NOAA GFS via the NOMADS grib_filter CGI (server-side bbox +
            variable subsetting — very small downloads)
  arome     Météo-France AROME (0.025° / 0.01°) via the public OVH bucket,
            whole-domain parameter packages (SP1, …)
  arpege    Météo-France ARPEGE (0.25° global / 0.1° Europe), same bucket
  icon-eu   DWD ICON-EU via opendata.dwd.de — per-variable bz2 files,
            merged into one GRIB per forecast step

Files are written atomically (.part → .grb2) so a concurrent scanner never
sees a partial file. A run is marked complete with a `.run-<stamp>.complete`
marker; once a newer run completes, files of older runs are deleted
(keep_runs per source).

Usage:
  downloader.py --config config.yaml [--source NAME] (--once | --loop)
"""
import argparse
import bz2
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
import yaml

log = logging.getLogger("grib-downloader")

UA = {"User-Agent": "grib-downloader/0.1 (signalk-grib-weather-provider)"}
HTTP_TIMEOUT = 60


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def http_ok(url: str) -> bool:
    try:
        r = requests.head(url, headers=UA, timeout=HTTP_TIMEOUT, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def download(url: str, dest: str, retries: int = 3) -> bool:
    """Stream url to dest atomically (.part → rename). True on success."""
    part = dest + ".part"
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=UA, timeout=HTTP_TIMEOUT, stream=True) as r:
                if r.status_code != 200:
                    log.warning("HTTP %d for %s", r.status_code, url)
                    return False
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            if os.path.getsize(part) == 0:
                raise IOError("empty download")
            os.replace(part, dest)
            return True
        except (requests.RequestException, IOError) as e:
            log.warning("attempt %d/%d failed for %s: %s", attempt, retries, url, e)
            time.sleep(2 * attempt)
    if os.path.exists(part):
        os.unlink(part)
    return False


# ── Run/cycle helpers ─────────────────────────────────────────────────────────

def candidate_runs(cadence_h: int, delay_h: float, count: int = 4):
    """Most recent model run datetimes, newest first."""
    now = datetime.now(timezone.utc) - timedelta(hours=delay_h)
    base = now.replace(minute=0, second=0, microsecond=0)
    base = base.replace(hour=(base.hour // cadence_h) * cadence_h)
    return [base - timedelta(hours=cadence_h * i) for i in range(count)]


def run_stamp(run: datetime) -> str:
    return run.strftime("%Y%m%dT%H")


def steps_list(spec) -> list:
    if isinstance(spec, list):
        return [int(s) for s in spec]
    return list(range(int(spec["from"]), int(spec["to"]) + 1, int(spec["by"])))


# ── Per-source state ──────────────────────────────────────────────────────────

def marker_path(directory: str, stamp: str) -> str:
    return os.path.join(directory, f".run-{stamp}.complete")


def latest_complete_stamp(directory: str):
    try:
        stamps = [m.group(1) for f in os.listdir(directory)
                  if (m := re.match(r"\.run-(\d{8}T\d{2})\.complete$", f))]
    except FileNotFoundError:
        return None
    return max(stamps) if stamps else None


def cleanup_old_runs(directory: str, keep_runs: int, log_prefix: str):
    """Keep the newest keep_runs completed runs; delete files of older ones."""
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        return
    stamps = sorted({m.group(1) for f in files
                     if (m := re.match(r"\.run-(\d{8}T\d{2})\.complete$", f))},
                    reverse=True)
    for old in stamps[keep_runs:]:
        for f in files:
            if f"__{old}__" in f or f == f".run-{old}.complete":
                path = os.path.join(directory, f)
                try:
                    os.unlink(path)
                except OSError as e:
                    log.warning("%s: cannot delete %s: %s", log_prefix, f, e)
        log.info("%s: purged run %s", log_prefix, old)


# ── GFS (NOMADS grib_filter) ──────────────────────────────────────────────────

GFS_DEFAULT_VARS = ["UGRD", "VGRD", "GUST", "TMP", "PRMSL", "RH", "APCP", "TCDC"]
GFS_DEFAULT_LEVELS = [
    "10_m_above_ground", "2_m_above_ground", "surface",
    "mean_sea_level", "entire_atmosphere",
]


def gfs_fetch(source: dict) -> bool:
    res = source.get("resolution", "0p25")
    directory = source["directory"]
    steps = steps_list(source.get("steps", {"from": 0, "to": 24, "by": 3}))
    bbox = source.get("bbox")  # [latMin, lonMin, latMax, lonMax]
    variables = source.get("variables", GFS_DEFAULT_VARS)
    levels = source.get("levels", GFS_DEFAULT_LEVELS)
    name = source["name"]

    for run in candidate_runs(cadence_h=6, delay_h=3.5):
        stamp = run_stamp(run)
        if latest_complete_stamp(directory) == stamp:
            return False  # up to date
        # Run available (incl. last step)? Probe the .idx on the pub server.
        date, hh = run.strftime("%Y%m%d"), run.strftime("%H")
        probe = (f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
                 f"gfs.{date}/{hh}/atmos/gfs.t{hh}z.pgrb2.{res}.f{steps[-1]:03d}.idx")
        if not http_ok(probe):
            continue

        log.info("%s: downloading run %s (%d steps)", name, stamp, len(steps))
        params = [f"var_{v}=on" for v in variables] + [f"lev_{l}=on" for l in levels]
        if bbox:
            lat0, lon0, lat1, lon1 = bbox
            params += ["subregion=", f"leftlon={lon0}", f"rightlon={lon1}",
                       f"bottomlat={lat0}", f"toplat={lat1}"]
        ok = True
        for step in steps:
            dest = os.path.join(directory, f"{name}__{stamp}__f{step:03d}.grb2")
            if os.path.exists(dest):
                continue
            url = (f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_{res}.pl?"
                   f"dir=%2Fgfs.{date}%2F{hh}%2Fatmos"
                   f"&file=gfs.t{hh}z.pgrb2.{res}.f{step:03d}&" + "&".join(params))
            if not download(url, dest):
                ok = False
                break
        if ok:
            open(marker_path(directory, stamp), "w").close()
            cleanup_old_runs(directory, source.get("keep_runs", 1), name)
            log.info("%s: run %s complete", name, stamp)
            return True
    return False


# ── Météo-France (AROME / ARPEGE, OVH public bucket) ─────────────────────────

MF_BASE = "https://meteofrance-pnt.s3.rbx.io.cloud.ovh.net/pnt"

MF_MODELS = {
    # (model, resolution) → (url path template, default groups, cadence h, delay h)
    ("arome", "0025"): (
        "{d}/arome/0025/{p}/arome__0025__{p}__{g}__{d}.grib2",
        ["00H06H", "07H12H", "13H18H", "19H24H", "25H30H", "31H36H",
         "37H42H", "43H48H", "49H51H"],
        3, 1.75),
    ("arome", "001"): (
        "{d}/arome/001/{p}/arome__001__{p}__{g}__{d}.grib2",
        [f"{h:02d}H" for h in range(52)],
        3, 1.75),
    ("arpege", "025"): (
        "{d}/arpege/025/{p}/arpege__025__{p}__{g}__{d}.grib2",
        ["000H024H", "025H048H", "049H072H", "073H102H"],
        6, 3.5),
    ("arpege", "01"): (
        "{d}/arpege/01/{p}/arpege__01__{p}__{g}__{d}.grib2",
        ["000H012H", "013H024H", "025H036H", "037H048H", "049H060H",
         "061H072H", "073H084H", "085H096H", "097H102H"],
        6, 3.5),
}


def mf_fetch(source: dict) -> bool:
    model = source["model"]
    res = str(source.get("resolution", "0025" if model == "arome" else "025"))
    key = (model, res)
    if key not in MF_MODELS:
        log.error("%s: unknown %s resolution %r", source["name"], model, res)
        return False
    template, all_groups, cadence, delay = MF_MODELS[key]
    groups = source.get("groups", all_groups)
    packages = source.get("packages", ["SP1"])
    directory = source["directory"]
    name = source["name"]

    for run in candidate_runs(cadence_h=cadence, delay_h=delay):
        stamp = run_stamp(run)
        if latest_complete_stamp(directory) == stamp:
            return False
        d = run.strftime("%Y-%m-%dT%H:00:00Z")
        # Run available? Probe the last requested group of the first package.
        probe = f"{MF_BASE}/" + template.format(d=d, p=packages[0], g=groups[-1])
        if not http_ok(probe):
            continue

        log.info("%s: downloading run %s (%d packages × %d groups)",
                 name, stamp, len(packages), len(groups))
        ok = True
        for p in packages:
            for g in groups:
                dest = os.path.join(directory, f"{name}__{stamp}__{p}_{g}.grb2")
                if os.path.exists(dest):
                    continue
                url = f"{MF_BASE}/" + template.format(d=d, p=p, g=g)
                if not download(url, dest):
                    ok = False
                    break
            if not ok:
                break
        if ok:
            open(marker_path(directory, stamp), "w").close()
            cleanup_old_runs(directory, source.get("keep_runs", 1), name)
            log.info("%s: run %s complete", name, stamp)
            return True
    return False


# ── DWD ICON-EU (opendata.dwd.de) ─────────────────────────────────────────────

ICON_EU_DEFAULT_VARS = [
    "t_2m", "u_10m", "v_10m", "vmax_10m", "pmsl", "relhum_2m",
    "tot_prec", "clct",
]
ICON_EU_BASE = "https://opendata.dwd.de/weather/nwp/icon-eu/grib"


def icon_eu_fetch(source: dict) -> bool:
    directory = source["directory"]
    steps = steps_list(source.get("steps", {"from": 0, "to": 48, "by": 3}))
    variables = source.get("variables", ICON_EU_DEFAULT_VARS)
    name = source["name"]

    for run in candidate_runs(cadence_h=6, delay_h=3.0):
        stamp = run_stamp(run)
        if latest_complete_stamp(directory) == stamp:
            return False
        hh = run.strftime("%H")
        ymdh = run.strftime("%Y%m%d%H")
        # Run available (incl. last step)? Probe the first variable.
        v0 = variables[0]
        probe = (f"{ICON_EU_BASE}/{hh}/{v0}/icon-eu_europe_regular-lat-lon_"
                 f"single-level_{ymdh}_{steps[-1]:03d}_{v0.upper()}.grib2.bz2")
        if not http_ok(probe):
            continue

        log.info("%s: downloading run %s (%d steps × %d vars)",
                 name, stamp, len(steps), len(variables))
        ok = True
        for step in steps:
            dest = os.path.join(directory, f"{name}__{stamp}__f{step:03d}.grb2")
            if os.path.exists(dest):
                continue
            # One GRIB per variable — decompress and concatenate into one
            # file per step (the provider expects all variables of a validity
            # time in a single GRIB file).
            part = dest + ".part"
            try:
                with open(part, "wb") as out:
                    for v in variables:
                        url = (f"{ICON_EU_BASE}/{hh}/{v}/icon-eu_europe_regular-"
                               f"lat-lon_single-level_{ymdh}_{step:03d}_"
                               f"{v.upper()}.grib2.bz2")
                        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
                        if r.status_code != 200:
                            raise IOError(f"HTTP {r.status_code} for {url}")
                        out.write(bz2.decompress(r.content))
                os.replace(part, dest)
            except (requests.RequestException, IOError, OSError) as e:
                log.warning("%s: step %d failed: %s", name, step, e)
                if os.path.exists(part):
                    os.unlink(part)
                ok = False
                break
        if ok:
            open(marker_path(directory, stamp), "w").close()
            cleanup_old_runs(directory, source.get("keep_runs", 1), name)
            log.info("%s: run %s complete", name, stamp)
            return True
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

FETCHERS = {
    "gfs": gfs_fetch,
    "arome": mf_fetch,
    "arpege": mf_fetch,
    "icon-eu": icon_eu_fetch,
}


def process_source(source: dict):
    model = source.get("model")
    fetcher = FETCHERS.get(model)
    if not fetcher:
        log.error("%s: unknown model %r", source.get("name"), model)
        return
    os.makedirs(source["directory"], exist_ok=True)
    try:
        fetcher(source)
    except Exception:
        log.exception("%s: unexpected error", source.get("name"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.environ.get("CONFIG", "/config.yaml"))
    ap.add_argument("--source", help="process only this source name")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="single pass (default)")
    mode.add_argument("--loop", action="store_true",
                      help="poll forever (interval_minutes, default 10)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    sources = config.get("sources", [])
    if args.source:
        sources = [s for s in sources if s.get("name") == args.source]
        if not sources:
            log.error("no source named %r in config", args.source)
            sys.exit(1)

    interval = int(config.get("interval_minutes", 10))
    while True:
        for source in sources:
            process_source(source)
        if not args.loop:
            break
        time.sleep(interval * 60)


if __name__ == "__main__":
    main()
