# build_cache.py
"""
Fetch OSM graph + nearest street addresses once and save to disk.

Usage
-----
$ python build_cache.py sim_cfg.json /dbfs/FileStore/gk_cache
"""
import json, os, sys
from pathlib import Path

import osmnx as ox, networkx as nx, geopandas as gpd, pandas as pd
from shapely.geometry import Point

def main(cfg_path: str, out_dir: str):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    cfg  = json.loads(Path(cfg_path).read_text())

    addr   = cfg["gk_location"]
    radius = cfg["radius_mi"]

    # 1. download driveable street graph
    print("Downloading street graph …")
    graph = ox.graph_from_point(
        ox.geocoder.geocode(addr),
        dist=radius * 1609.34,
        network_type="drive"
    )
    ox.save_graphml(graph, out / "graph.graphml")
    print(f"✔ graph.graphml saved ({len(graph)} nodes)")

    # 2. fetch building footprints that ALREADY have house # + street
    print("Downloading addressed footprints …")
    tags = {"addr:housenumber": True, "addr:street": True}
    bldgs = ox.geometries_from_point(
        ox.geocoder.geocode(addr),
        dist=radius * 1609.34,
        tags=tags
    )
    bldgs = bldgs.dropna(subset=["addr:housenumber", "addr:street"])
    bldgs = bldgs.to_crs("EPSG:4326")[["addr:housenumber", "addr:street", "geometry"]]

    # 3. build node table + nearest join to get best available address
    print("Building node table …")
    rows = [{"node_id": nid,
             "lat": data["y"],
             "lon": data["x"],
             "geometry": Point(data["x"], data["y"])}
            for nid, data in graph.nodes(data=True)]
    gdf_nodes = gpd.GeoDataFrame(rows, crs="EPSG:4326")

    joined = gpd.sjoin_nearest(gdf_nodes, bldgs, how="left")

    def label(r):
        if pd.notna(r["addr:housenumber"]) and pd.notna(r["addr:street"]):
            return f"{int(r['addr:housenumber'])} {r['addr:street']}"
        return None           # fallback handled at runtime

    joined["addr"] = joined.apply(label, axis=1)
    joined[["node_id", "lat", "lon", "addr"]].to_parquet(out / "nodes.parquet")
    print("✔ nodes.parquet saved")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python build_cache.py <sim_cfg.json> <cache_dir>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
