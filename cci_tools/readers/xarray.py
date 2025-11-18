import xarray as xr
import os
from datetime import datetime

def scrape_xarray(location, endpoint, engine, drs, collections):

    engine = engine.split('|')[-1]

    ds = xr.open_dataset(os.path.join(location,endpoint), engine=engine)

    start_datetime = str(ds.time.min().dt.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()) or getattr(ds, 'time_coverage_start')
    end_datetime   = str(ds.time.max().dt.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()) or getattr(ds, 'time_coverage_start')

    bbox_w = ds.geospatial_lon_min
    bbox_e = ds.geospatial_lon_max
    bbox_s = ds.geospatial_lat_min
    bbox_n = ds.geospatial_lat_max

    geo_type = 'Polygon'
    if bbox_w == bbox_e and bbox_n == bbox_s:
        geo_type = 'Point'
    coordinates = [[[bbox_w, bbox_s], [bbox_e, bbox_s], [bbox_e, bbox_n], [bbox_w, bbox_n], [bbox_w, bbox_s]]] 
    bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

    properties = {
        "datetime": None,
        "created": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "license": "other",
        "version": ds.product_version,
        "aggregation": True,
        "platforms": [ds.platform],
        "collections": collections,
        "proj:transform": None,
        "proj:epsg": '4326',
        "proj:shape": (len(ds.time), len(ds.lat), len(ds.lon))
    }

    return {
        "start_datetime":start_datetime, 
        "end_datetime":end_datetime, 
        "version": ds.product_version, 
        "platforms": [ds.platform], 
        "drs": drs, 
        "bbox": bbox, 
        "geo_type": geo_type, 
        "coordinates": coordinates, 
        "properties":properties,
        "format": engine
    }