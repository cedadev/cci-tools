#!/usr/bin/env python
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

"""
Create OpenEO records - involves combining multiple asset 'bands'
into the same item."""
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import elasticsearch.helpers
import json 
import requests
import os
import click
import pdb
import rasterio
import re
from dateutil.relativedelta import relativedelta

def get_query(directory):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "prefix": {
                            "info.directory": directory
                        }
                    },
                    {
                        "exists": {
                            "field": "projects.opensearch"
                        }
                    }
                ]
            }
        }, "sort": [{"info.directory": {"order": "asc"}}, {"info.name": {"order": "asc"}}], "size": 10
    }
    return query

def extract_id(es_all_dict:dict):
    """
    Extract filename from OpenSearch record
    """
    fname = str(es_all_dict['info'].get('name'))
    file_id, file_ext = os.path.splitext(fname)

    return fname, file_id, file_ext

def extract_collection(es_all_dict:dict):
    """
    Extract collection from OpenSearch record
    """
    ecv = es_all_dict['projects']['opensearch'].get('ecv')

    if type(ecv) is list:
        if len(ecv) == 1:
            ecv = str(ecv[0]).lower()
        else:
            print("ECV is a list!!")
    
    return ecv

def get_licence(ecv:str):
    '''
    Construct URL to the relevant data license on the CEDA artefacts server
    '''    
    for license in ['_terms_and_conditions_v2.pdf','_terms_and_conditions.pdf','.pdf']:
        r = requests.get(f"https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}")
        if r.status_code == 200:
            break
    url = f"https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}"

    return url

def extract_opensearch(es_all_dict:dict):
    # Extract geospatial bounding box (W, N, E, S)
    coords = es_all_dict['info']['spatial']['coordinates'].get('coordinates')
    bbox_w = coords[0][0] # west
    bbox_n = coords[0][1] # north
    bbox_e = coords[1][0] # east
    bbox_s = coords[1][1] # south
    bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

    geo_type = 'Polygon'
    coordinates = [[bbox_w, bbox_s], [bbox_e, bbox_s], [bbox_e, bbox_n], [bbox_w, bbox_n], [bbox_w, bbox_s]]

    # Extract specific properties required
    sdatetime = str(es_all_dict['info']['temporal'].get('start_time'))
    start_datetime = sdatetime.partition('+')[0]+'Z'
    edatetime = str(es_all_dict['info']['temporal'].get('end_time'))
    end_datetime = edatetime.partition('+')[0]+'Z'
    version = es_all_dict['projects']['opensearch'].get('productVersion')
    platforms = es_all_dict['projects']['opensearch'].get('platform')
    drs = es_all_dict['projects']['opensearch'].get('drsId')

    # Extract all other properties
    properties = dict()
    for property,value in es_all_dict['projects']['opensearch'].items():
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        properties[property] = value
        
    # Extract format
    format = es_all_dict['info'].get('format')
    
    stac_info = {
        "start_datetime":start_datetime, 
        "end_datetime":end_datetime, 
        "version": version, 
        "platforms": platforms, 
        "drs": drs, 
        "bbox": bbox, 
        "geo_type": geo_type, 
        "coordinates": coordinates, 
        "properties":properties,
        "format": format
        }
    
    return stac_info, properties

def end_of_month(dt):
    import calendar
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day)

def extract_times_from_file(geotiff_file, interval):

    filename = geotiff_file.split('/')[-1]

    ymd_two    = re.search("([0-9]{2}[0-9]{2}[0-9]{4})_([0-9]{2}[0-9]{2}[0-9]{4})",filename)
    yyyymmdd   = re.search("([0-9]{2}[0-9]{2}[0-9]{4})",filename)
    yyyy       = re.search("(?<=[^a-zA-Z0-9])([0-9]{4})(?=[^a-zA-Z0-9])",filename)
    yyyy_yyyy  = re.search("([0-9]{4})-([0-9]{4})",filename)
    resolution = re.search("(?<=[^a-zA-Z0-9])(P[0-9]{1,2}Y)(?=[^a-zA-Z0-9])", filename)
    y1 = None
    end_datetime = None

    resolutions = {'Y':'years','M':'months','D':'days'}

    if ymd_two is not None:
        start_datetime = datetime.strptime(
            ymd_two.group().split('_')[0],"%Y%m%d"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_datetime = datetime.strptime(
            ymd_two.group().split('_')[1],"%Y%m%d"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyymmdd is not None:
        dt_object = datetime.strptime(yyyymmdd.group(),"%Y%m%d")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyy_yyyy is not None:
        y0, y1 = yyyy_yyyy.group().split('-')
        dt_object = datetime.strptime(y0,"%Y")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyy is not None:
        dt_object = datetime.strptime(yyyy.group(),"%Y")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return None,None
    
    if end_datetime is not None:
        pass
    elif interval == 'month':
        end_datetime = end_of_month(dt_object).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif y1 is not None:
        fdt_object = datetime.strptime(y1,"%Y")
        end_datetime = (fdt_object + relativedelta(years=1) - relativedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif resolution is not None:
        # P1Y example
        r = resolution.group()
        end_datetime = (dt_object + relativedelta(
            **{resolutions[r[-1]]:int(r[1:-1])}
        ) - relativedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        # Day
        end_datetime = dt_object.strftime("%Y-%m-%d") + "T23:59:59Z"

    print(filename, start_datetime, end_datetime)
    
    return start_datetime, end_datetime

def read_geotiff(
        geotiff_file:str, 
        start_time: str = None, 
        end_time: str = None, 
        assume_global: bool = False,
        interval: str = None):
    # Read in releavent info from GeoTIFF file itself

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

        if start_datetime is None:
            print(" > Insufficient Temporal Information")
            return None, None

        version   = metadata.get('product_version',None)
        if not version:
            try:
                version = re.search("(fv[0-9]{1}.[0-9]{1})",geotiff_file).group()
            except:
                version = 'Unknown'
        platforms = metadata.get('platform','Unknown')
        drs = None
        try:
            bbox_w = float(metadata['geospatial_lon_min']) # west
            bbox_n = float(metadata['geospatial_lat_max']) # north
            bbox_e = float(metadata['geospatial_lon_max']) # east
            bbox_s = float(metadata['geospatial_lat_min']) # south
            bbox = [bbox_w, bbox_s, bbox_e, bbox_n]
        except:

            try:
                import xarray as xr
                ds = xr.open_dataset(geotiff_file,engine='rasterio')
                bbox_w = float(ds.x.min())
                bbox_e = float(ds.x.max())
                bbox_n = float(ds.y.max())
                bbox_s = float(ds.y.min())
            except:
                assert 1==2
                # Greenland specific
                bbox_w = -80
                bbox_e = -10
                bbox_n = 90
                bbox_s = 60

            if assume_global:
                bbox_w = -180
                bbox_n = 90
                bbox_e = 180
                bbox_s = -90
            elif bbox_w is None:
                print(" > Insufficient Spatial Information")
                return None, None
            
        bbox = [bbox_w, bbox_s, bbox_e, bbox_n]
        geo_type = 'Polygon'
        coordinates = [[bbox_w, bbox_s], [bbox_e, bbox_s], [bbox_e, bbox_n], [bbox_w, bbox_n], [bbox_w, bbox_s]]
        format = 'GeoTiff'

        transform = [src.transform[i] for i in range(6)]
        epsg = src.crs.to_epsg()
        shape = [src.height, src.width]
        properties={"proj:transform":transform, "proj:epsg":epsg, "proj:shape":shape}

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
    
    return stac_info, properties

def process_record(
        es_all_dict:dict, 
        STAC_API:str, 
        suffix: str = '', 
        DRS: str = '',
        splitter: dict = None,
        start_time: str = None,
        end_time: str = None,
        assume_global: bool = False,
        interval: str = None,
    )->tuple:

    # Mapping to split files within the same final Item.
    splitter = splitter or None

    # Extract filename, file id, and file extension
    fname, file_id, file_ext = extract_id(es_all_dict)
    print(file_id)

    # Extract file path
    location = es_all_dict['info'].get('directory')

    # Extract collection (ECV/project)
    ecv = extract_collection(es_all_dict)

    # Construct url to license on the CEDA archive assets server
    url = get_licence(ecv)
    
    # Extract dataset ID (UUID)
    uuid = es_all_dict['projects']['opensearch'].get('datasetId')

    if file_ext=='.nc' or file_ext=='.json':
        # === NetCDF ===
        # All information can be extracted from OpenSearch record

        stac_info, properties = extract_opensearch(es_all_dict)

        if not isinstance(stac_info, dict):
            print("Error: OpenSearch record does not contain the required information to create a STAC record.")
            return None, None

    elif file_ext=='.tif':
        # === GeoTIFF ===
        # Information will be extracted from the OpenSearch record and the GeoTIFF file itself

        stac_info, properties = read_geotiff(
            location+"/"+fname, 
            start_time=start_time, 
            end_time=end_time, 
            assume_global=assume_global,
            interval=interval)

        if not isinstance(stac_info, dict):
            print(f"Error: GeoTIFF file does not contain the required information to create a STAC record: {location}/{fname}")
            return None, None

    else:
        print("Error: File format not recognised!")
        return None, None
    
    exts = [
        "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
    ]

    if suffix == '':
        eo_bands = stac_info['format']
    else:
        exts.append("https://stac-extensions.github.io/eo/v1.1.0/schema.json")
        # Temporary to get banding properly
        # Split applied to file_id
        if isinstance(splitter,str):
            eo_bands = splitter
        else:
            eo_bands = None
            for split, mapping in splitter.items():
                if split in file_id:
                    file_id = file_id.replace(split, mapping[0])
                    eo_bands = mapping[1] # Asset label.
            if eo_bands is None:
                print(file_id)
                x=input('No splitter? ')
                

    drs = stac_info.get('drs',None) or DRS or f'{uuid}-main'

    # Create a dictionary of the required STAC output
    stac_dict = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": exts,
        "id": file_id + suffix,
        "collection": (drs + suffix).lower(),
        "geometry": {
            "type": stac_info["geo_type"],
            "coordinates": [stac_info["coordinates"]]
        },
        "bbox": stac_info["bbox"],
        "properties": {
            "datetime": None,
            "start_datetime": stac_info["start_datetime"],
            "end_datetime": stac_info["end_datetime"],
            "license": "other",
            "version": stac_info["version"],
            "aggregation": False,
            "platforms": stac_info["platforms"],
            "collections":[ecv, uuid, drs],
            **properties
        },
        "links": [
            {
            "rel": "self",
            "type": "application/geo+json",
            "href": f"{STAC_API}/collections/{(drs + suffix).lower()}/items/{file_id + suffix}"
            },
            {
            "rel": "parent",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{(drs + suffix).lower()}"
            },
            {
            "rel": "collection",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{(drs + suffix).lower()}"
            },
            {
            "rel": "root",
            "type": "application/json",
            "href": STAC_API
            },
            {
            "rel": "license",
            "type": "application/pdf",
            "href": url
            }
        ],
        "assets": {
            eo_bands: {
                "href": f"https://dap.ceda.ac.uk{location}/{fname}",
                "roles": [
                    "data"
                ]
            }
        }
    }

    # Remove platform until STAC standards have been updated to allow lists of platforms. 
    # Until then, the platform list is entered as 'platforms' instead.
    if file_ext=='.nc':
        stac_dict['properties'].pop('platform')

    return stac_dict, file_ext

def combine_records(recordA, recordB):
    
    for k, v in recordB['assets'].items():
        recordA['assets'][k] = v
    return recordA

# Parse command line arguments using click
@click.command()
@click.argument('cci_dirs', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--output_drs', 'output_drs', required=False,
              help='DRS to apply to all items for the CCI dirs specified')
@click.option('--exclusion',  'exclusion', required=False,
              help='Exclude opensearch records with this string present in their name')
@click.option('--start_time', 'start_time', required=False,
              help='Manually specify %Y%m%dT%H%M%SZ format start_time') # "%Y%m%dT%H%M%SZ"
@click.option('--end_time',   'end_time', required=False,
              help='Manually specify %Y%m%dT%H%M%SZ format end_time')
@click.option('--interval',   'interval', required=False,
              help='Interval for where start_time is defined by the filename')
@click.option('--global',   'assume_global', required=False, is_flag=True,
              help='Assume global coverage where other data is not present')

def main(cci_dirs, output_dir, output_drs, exclusion=None, start_time=None, end_time=None,
         assume_global=False, interval=None):
    '''
    Reads in OpenSearch records for CCI NetCDF and geotiff data.

    For NetCDF files, information is extracted from the OpenSearch record only.

    For GeoTIFF files, only partial information is available within the OpenSearch record, so additional metadata is extracted from the GeoTIFF file itself.
    '''

    exclusion = exclusion or 'uf8awhjidaisdf8sd'
    splitter={}

    STAC_API = 'https://api.stac.164.30.69.113.nip.io'
    suffix = '.openeo'

    # Setup client and query elasticsearch
    with open('API_CREDENTIALS') as f:
        api_creds = json.load(f)
        API_KEY = api_creds["secret"]

    client = Elasticsearch(hosts=['https://elasticsearch.ceda.ac.uk'],
                        headers={'x-api-key':API_KEY}
                        )
    
    drs = output_drs

    if os.path.isfile(cci_dirs):
        with open(cci_dirs) as f:
            cci_configurations = [r.strip().split(',') for r in f.readlines()]
    else:
        cci_configurations = [[cci_dirs]]

    for cfg in cci_configurations:

        cci_dir = cfg[0]
        if len(cfg) > 1:
            drs = cfg[1]
        if len(cfg) > 2:
            if os.path.isfile(cfg[2]):
                with open(cfg[2]) as f:
                    splitter = json.load(f)
            else:
                splitter=cfg[2]

        print(f"Input CCI directory: {cci_dir}")
        print(f"Output STAC record directory: {output_dir}")

        if drs == '':
            drs = output_drs

        body=get_query(cci_dir)
        response = client.search(index='opensearch-files', body=body)
        
        # Loop over OpenSearch records, converting each to STAC format
        is_last = False
        while len(response['hits']['hits']) == 10 or not is_last:

            if len(response['hits']['hits']) != 10:
                is_last = True

            for record in response['hits']['hits']:

                if exclusion in record['_source']['info']['name']:
                    print('Skipping due to exclusion')
                    continue
                elif '.tif' not in record['_source']['info']['name']:
                    print('Skipping non geotiff')
                    continue


                # Process OpenSearch record
                stac_dict, file_ext = process_record(
                    record['_source'], 
                    STAC_API, 
                    suffix=suffix, 
                    DRS=drs, 
                    splitter=splitter,
                    start_time=start_time,
                    end_time=end_time,
                    assume_global=assume_global,
                    interval=interval)

                if isinstance(stac_dict, dict) == False:
                    print(f"Unable to create STAC record.")
                    continue

                # Create directory for each CCI ECV/Project
                ecv_dir=stac_dict["collection"]
                cci_stac_dir=f"{output_dir}/{ecv_dir}/"

                if os.path.isdir(cci_stac_dir)==False:
                    try:
                        os.mkdir(cci_stac_dir)
                        print(f"Created directory '{cci_stac_dir}' successfully")
                    except PermissionError:
                        print(f"Permission denied: Unable to make '{cci_stac_dir}'")
                    except Exception as e:
                        print(f"An error occured '{e}'")
            
                # Write 'pretty print' STAC json file
                id=stac_dict["id"]
                stac_file=f"{cci_stac_dir}stac_{id}-{file_ext[1:]}.json"

                # Merge assets with existing stac record if present.

                if splitter is not None:
                    # Mapping applied so asset merging is allowed
                    if os.path.isfile(stac_file):
                        with open(stac_file) as f:
                            stac_combine = json.load(f)
                        stac_dict = combine_records(stac_dict, stac_combine)

                with open(stac_file, 'w', encoding='utf-8') as file:
                    json.dump(stac_dict, file, ensure_ascii=False, indent=2)

            searchAfter = response['hits']['hits'][-1]["sort"]
            body['search_after'] = searchAfter
            response = client.search(index='opensearch-files', body=body)

if __name__ == "__main__":
    main()
