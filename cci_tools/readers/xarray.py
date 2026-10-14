import xarray as xr
import os
import json
from datetime import datetime


def scrape_xarray(location, endpoint, engine, drs, collections):

    engine = engine.split("|")[-1]

    ds = xr.open_dataset(os.path.join(location, endpoint), engine=engine)

    start_datetime = str(
        ds.time.min().dt.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()
    ) or getattr(ds, "time_coverage_start")
    end_datetime = str(
        ds.time.max().dt.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()
    ) or getattr(ds, "time_coverage_start")

    bbox_w = getattr(ds, "geospatial_lon_min", None)
    bbox_e = getattr(ds, "geospatial_lon_max", None)
    bbox_s = getattr(ds, "geospatial_lat_min", None)
    bbox_n = getattr(ds, "geospatial_lat_max", None)

    if bbox_w is None:
        bbox_w = ds.lon.min().to_dict()["data"] - 180  # Transform via flags
        bbox_e = ds.lon.max().to_dict()["data"] - 180
        bbox_s = ds.lat.min().to_dict()["data"]
        bbox_n = ds.lat.max().to_dict()["data"]

    geo_type = "Polygon"
    if bbox_w == bbox_e and bbox_n == bbox_s:
        geo_type = "Point"
    coordinates = [
        [
            [bbox_w, bbox_s],
            [bbox_e, bbox_s],
            [bbox_e, bbox_n],
            [bbox_w, bbox_n],
            [bbox_w, bbox_s],
        ]
    ]
    bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

    vn = None
    versions = ["product_version", "data_specs_version"]
    for v in versions:
        vn = getattr(ds, v, vn)

    platform = getattr(ds, "platform", None)
    if platform is not None:
        platform = [platform]

    properties = {
        "datetime": None,
        "created": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "license": "other",
        "version": vn,
        "aggregation": True,
        "platforms": platform,
        "collections": collections,
        "proj:transform": None,
        "proj:epsg": "4326",
        "proj:shape": (len(ds.time), len(ds.lat), len(ds.lon)),
    }

    return {
        "drs": drs,
        "bbox": bbox,
        "geo_type": geo_type,
        "coordinates": coordinates,
        "properties": properties,
        "format": engine,
    }
