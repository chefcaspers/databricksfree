#!/usr/bin/env python
"""
Emit ONE Ghost‑Kitchen order in (optionally accelerated) real time,
using pre‑cached OSM graph + node table.

--------------------------------------------------------------------
CLI
--------------------------------------------------------------------
python single_order_rt_route.py <sim_cfg.json> <cache_dir> [output_dir]

• sim_cfg.json  – your original config (speed_up, svc, etc.)
• cache_dir     – folder containing graph.graphml + nodes.parquet
• output_dir    – (optional) write each event as a JSON file there;
                  if omitted, events are printed to STDOUT
--------------------------------------------------------------------
"""

import json, uuid, random, datetime as dt, time, sys
from pathlib import Path
from typing import Callable, Dict

import osmnx as ox
import networkx as nx
import pandas as pd
import numpy as np


# ──────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────
def _gauss(rng: random.Random, mu_sigma):
    """Positive Gaussian sample given [mean, std]."""
    mu, sigma = mu_sigma
    return max(0.1, rng.gauss(mu, sigma))


def stdout_writer(event: Dict):
    print(json.dumps(event))


def dir_writer(out_dir: Path) -> Callable[[Dict], None]:
    out_dir.mkdir(parents=True, exist_ok=True)

    def _write(event: Dict):
        ts = dt.datetime.strptime(event["ts"], "%Y-%m-%d %H:%M:%S.%f")
        fname = f"{ts:%Y%m%d-%H%M%S.%f}-{event['event_id']}.json"
        (out_dir / fname).write_text(json.dumps(event))

    return _write


def shortest_route(graph: nx.MultiDiGraph, gk_node: int, lat: float, lon: float):
    """Return (coords list, meters) for GK → customer shortest path."""
    cust = ox.distance.nearest_nodes(graph, lon, lat)
    try:
        path = nx.shortest_path(graph, gk_node, cust, weight="length")
        g = graph
    except nx.NetworkXNoPath:  # fall back to undirected
        g = graph.to_undirected()
        path = nx.shortest_path(g, gk_node, cust, weight="length")

    coords = [(g.nodes[n]["y"], g.nodes[n]["x"]) for n in path]
    dist_m = sum(
        min(d["length"] for d in g[u][v].values())
        for u, v in zip(path[:-1], path[1:])
    )
    return coords, dist_m


# ──────────────────────────────────────────────────────────────────────
#  Main simulator
# ──────────────────────────────────────────────────────────────────────
def run_single_order_rt(cfg_json: str,
                        cache_dir: Path,
                        write_event: Callable[[Dict], None] = stdout_writer):
    """Emit all lifecycle events for one order in real/accelerated time."""
    cfg = json.loads(cfg_json)
    rng = random.Random(cfg.get("random_seed", 42))
    speed = max(1e-6, cfg.get("speed_up", 1))  # avoid div‑by‑zero

    # --- Load cached assets ------------------------------------------
    graph = ox.load_graphml(cache_dir / "graph.graphml")
    nodes = pd.read_parquet(cache_dir / "nodes.parquet")

    # GK coordinate (try cfg first, fall back to geocoder if present)
    if {"gk_lat", "gk_lon"} <= set(cfg):
        gk_lat, gk_lon = cfg["gk_lat"], cfg["gk_lon"]
    else:  # may require internet – run once & stash lat/lon in cfg for true offline
        gk_lat, gk_lon = ox.geocoder.geocode(cfg["gk_location"])

    gk_node = ox.distance.nearest_nodes(graph, gk_lon, gk_lat)

    # --- Random customer ---------------------------------------------
    cust_row = nodes.sample(1, random_state=rng.randrange(2**32)).iloc[0]
    lat, lon = cust_row.lat, cust_row.lon
    addr = cust_row.addr or f"{rng.randint(1, 9999)} Main St"

    # --- Route + drive time ------------------------------------------
    pts, dist_m = shortest_route(graph, gk_node, lat, lon)
    drive_min = dist_m / 1609.34 / cfg["driver_mph"] * 60

    svc = cfg["svc"]
    now = dt.datetime.utcnow()

    # --- Timestamps ---------------------------------------------------
    ts_created = now
    ts_started = ts_created + dt.timedelta(minutes=_gauss(rng, svc["cs"]))
    ts_finished = ts_started + dt.timedelta(minutes=_gauss(rng, svc["sf"]))
    ts_ready = ts_finished + dt.timedelta(minutes=_gauss(rng, svc["fr"]))
    ts_picked = ts_ready + dt.timedelta(minutes=_gauss(rng, svc["rp"]))
    ts_delivered = ts_picked + dt.timedelta(minutes=drive_min)

    # --- Emit helper --------------------------------------------------
    gk_id = uuid.uuid4().hex
    order_id = uuid.uuid4().hex
    loc_name = cfg.get("location_name", "unknown")
    seq = 0

    def emit(ts: dt.datetime, ev: str, body: Dict):
        nonlocal seq
        # wait until it's time (scaled by speed_up)
        delay = (ts - dt.datetime.utcnow()).total_seconds() / speed
        if delay > 0:
            time.sleep(delay)

        event = {
            "event_id": uuid.uuid4().hex,
            "event_type": ev,
            "ts": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "gk_id": gk_id,
            "location": loc_name,
            "order_id": order_id,
            "sequence": seq,
            "body": json.dumps(body),
        }
        write_event(event)
        seq += 1

    # --- Lifecycle events --------------------------------------------
    emit(ts_created, "order_created",
         dict(customer_lat=lat, customer_lon=lon, customer_addr=addr))

    emit(ts_started, "gk_started", {})
    emit(ts_finished, "gk_finished", {})
    emit(ts_ready, "gk_ready", {})

    emit(ts_picked, "driver_picked_up",
         dict(route_points=pts, eta_mins=round(drive_min, 1)))

    # Driver pings
    ping_sec = cfg.get("ping_sec", 60)
    hops = max(1, int(drive_min * 60 // ping_sec))
    for h in range(1, hops):
        p = h / hops
        lat_i, lon_i = pts[int(p * (len(pts) - 1))]
        emit(ts_picked + dt.timedelta(seconds=h * ping_sec), "driver_ping",
             dict(progress_pct=round(p * 100, 1), loc_lat=lat_i, loc_lon=lon_i))

    emit(ts_delivered, "delivered",
         dict(delivered_lat=lat, delivered_lon=lon))


# ──────────────────────────────────────────────────────────────────────
#  CLI entry‑point
# ──────────────────────────────────────────────────────────────────────
#if __name__ == "__main__":
#    if len(sys.argv) not in (3, 4):
#        print("Usage: python single_order_rt_route.py <sim_cfg.json> <cache_dir> [output_dir]")
#        sys.exit(1)

#    cfg_text = Path(sys.argv[1]).read_text()
#    cache = Path(sys.argv[2])

#    writer = stdout_writer
#    if len(sys.argv) == 4:
#        writer = dir_writer(Path(sys.argv[3]))
#

# run_single_order_rt(Path("./config.json").read_text(), Path("./"), dir_writer(Path("./")))