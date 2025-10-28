import re
import rasterio
from datetime import datetime

from .file import extract_times_from_file, extract_version


def read_geotiff(
        geotiff_file:str, 
        start_time: str = None, 
        end_time: str = None, 
        assume_global: bool = False,
        interval: str = None,
        fill_incomplete: bool = False,
        openeo: bool = False,
    ) -> tuple[dict,dict]:
    """
    Read data from a GeoTiff file to produce a valid set of STAC info."""

    incomplete=False
    bbox_w = None
    
    # Values to feed into proj:transform, proj:epsg, and proj:shape
    with rasterio.open(geotiff_file) as src:

        metadata = src.tags()
        try:
            start_dt=metadata.get('time_coverage_start', start_time)
            dt_object=datetime.strptime(start_dt,"%Y%m%dT%H%M%SZ")
            start_datetime=dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_dt=metadata.get('time_coverage_end', end_time)
            dt_object=datetime.strptime(end_dt,"%Y%m%dT%H%M%SZ")
            end_datetime=dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            start_datetime, end_datetime = extract_times_from_file(geotiff_file, interval)
            if start_datetime is None and fill_incomplete:
                incomplete=True
                start_datetime="0001-01-01T00:00:00Z"
                end_datetime="0001-01-01T00:00:00Z"

        if start_datetime is None and not fill_incomplete:
            raise ValueError("Insufficient Temporal Information")

        # Find version from filename
        version   = metadata.get('product_version',extract_version(geotiff_file))
        platforms = metadata.get('platform','Unknown')
        drs = None
        try:
            bbox_w = float(metadata['geospatial_lon_min']) # west
            bbox_n = float(metadata['geospatial_lat_max']) # north
            bbox_e = float(metadata['geospatial_lon_max']) # east
            bbox_s = float(metadata['geospatial_lat_min']) # south
        except:

            try:
                import xarray as xr
                ds = xr.open_dataset(geotiff_file,engine='rasterio')
                bbox_w = float(ds.x.min())
                bbox_e = float(ds.x.max())
                bbox_n = float(ds.y.max())
                bbox_s = float(ds.y.min())
            except:
                if assume_global or fill_incomplete:
                    bbox_w = -180
                    bbox_n = 90
                    bbox_e = 180
                    bbox_s = -90
                else:
                    raise ValueError("Insufficient Spatial Information")
        
        geo_type = 'Polygon'
        if bbox_w == bbox_e and bbox_n == bbox_s:
            geo_type = 'Point'
        coordinates = [[[bbox_w, bbox_s], [bbox_e, bbox_s], [bbox_e, bbox_n], [bbox_w, bbox_n], [bbox_w, bbox_s]]] 
        bbox = [bbox_w, bbox_s, bbox_e, bbox_n]
        format = 'GeoTIFF'

        try:
            transform = [src.transform[i] for i in range(6)]
            epsg = src.crs.to_epsg()
            shape = [src.height, src.width]
        except:
            if openeo and not fill_incomplete:
                raise ValueError(
                    'Openeo-required params (transform, epsg, shape) could not be identified.'
                )
            incomplete=True
            transform = None
            epsg = None
            shape = None
        
        properties={"proj:transform":transform, "proj:epsg":epsg, "proj:shape":shape}

        if incomplete:
            properties['incomplete']=True

        stac_info = {
            "start_datetime":start_datetime, 
            "end_datetime":end_datetime, 
            "version": version, 
            "platforms": platforms, 
            "drs": drs, 
            "bbox": bbox, 
            "geo_type": geo_type, 
            "coordinates": coordinates,
            "format": format, 
            "transform":transform, 
            "epsg": epsg, 
            "shape":shape
            }
    
    return stac_info