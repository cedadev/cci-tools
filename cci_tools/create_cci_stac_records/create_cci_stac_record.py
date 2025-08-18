#!/usr/bin/env python
"""
This script performs an elasticsearch query for the directory defined in variable cci_dir 
and then converts the opensearch record output to STAC format.

Currently run from the command line using:
python create_cci_stac_record.py cci_dir

e.g. python create_cci_stac_record.py /neodc/esacci/biomass/data/agb/maps/v6.0/netcdf /gws/nopw/j04/esacci_portal/stac/stac_records
"""
from elasticsearch import Elasticsearch
from datetime import datetime
import elasticsearch.helpers
import json 
import requests
import os
import click
import pdb
import rasterio

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

def read_geotiff(geotiff_file:str):
    # Read in releavent info from GeoTIFF file itself
    
    # Values to feed into proj:transform, proj:epsg, and proj:shape
    with rasterio.open(geotiff_file) as src:

        metadata = src.tags()
        try:
            start_dt=metadata['time_coverage_start']
            dt_object=datetime.strptime(start_dt,"%Y%m%dT%H%M%SZ")
            start_datetime=dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_dt=metadata['time_coverage_end']
            dt_object=datetime.strptime(end_dt,"%Y%m%dT%H%M%SZ")
            end_datetime=dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            print(f"An error occured '{e}'")
            return None, None

        version = metadata['product_version']
        platforms = metadata['platform']
        drs = "TEMP"

        bbox_w = float(metadata['geospatial_lon_min']) # west
        bbox_n = float(metadata['geospatial_lat_max']) # north
        bbox_e = float(metadata['geospatial_lon_max']) # east
        bbox_s = float(metadata['geospatial_lat_min']) # south
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

def process_record(es_all_dict:dict, STAC_API:str)->tuple:
    # Extract filename, file id, and file extension
    fname, file_id, file_ext = extract_id(es_all_dict)

    # Extract file path
    location = es_all_dict['info'].get('directory')

    # Extract collection (ECV/project)
    ecv = extract_collection(es_all_dict)

    # Construct url to license on the CEDA archive assets server
    url = get_licence(ecv)
    
    # Extract dataset ID (UUID)
    uuid = es_all_dict['projects']['opensearch'].get('datasetId')

    if file_ext=='.nc':
        # === NetCDF ===
        # All information can be extracted from OpenSearch record

        stac_info, properties = extract_opensearch(es_all_dict)

        if isinstance(stac_info, dict) == False:
            print("Error: OpenSearch record does not contain the required information to create a STAC record.")
            return None, None

    elif file_ext=='.tif':
        # === GeoTIFF ===
        # Information will be extracted from the OpenSearch record and the GeoTIFF file itself

        stac_info, properties = read_geotiff(location+"/"+fname)

        if isinstance(stac_info, dict) == False:
            print(f"Error: GeoTIFF file does not contain the required information to create a STAC record: {location}/{fname}")
            return None, None

    else:
        print("Error: File format not recognised!")
        return

    # Create a dictionary of the required STAC output
    stac_dict = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
            ],
        "id": file_id,
        "collection": stac_info["drs"],
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
            "collections":[ecv, uuid, stac_info["drs"]],
            **properties
        },
        "links": [
            {
            "rel": "self",
            "type": "application/geo+json",
            "href": f"{STAC_API}/collections/{stac_info['drs']}/items/{file_id}"
            },
            {
            "rel": "parent",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{stac_info['drs']}" 
            },
            {
            "rel": "collection",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{stac_info['drs']}"
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
            stac_info["format"]: {
                "href": f"{location}/{fname}",
                "role": [
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

# Parse command line arguments using click
@click.command()
@click.argument('cci_dir', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(exists=True))

def main(cci_dir, output_dir):
    '''
    Reads in OpenSearch records for CCI NetCDF and geotiff data.

    For NetCDF files, information is extracted from the OpenSearch record only.

    For GeoTIFF files, only partial information is available within the OpenSearch record, so additional metadata is extracted from the GeoTIFF file itself.
    '''

    print(f"Input CCI directory: {cci_dir}")
    print(f"Output STAC record directory: {output_dir}")
    STAC_API = 'https://api.stac.ceda.ac.uk'

    # Setup client and query elasticsearch
    with open('API_CREDENTIALS') as f:
        api_creds = json.load(f)
        API_KEY = api_creds["secret"]

    client = Elasticsearch(hosts=['https://elasticsearch.ceda.ac.uk'],
                           headers={'x-api-key':API_KEY}
                           )

    body=get_query(cci_dir)
    response = client.search(index='opensearch-files', body=body)
    
    # Loop over OpenSearch records, converting each to STAC format
    is_last = False
    while len(response['hits']['hits']) == 10 or not is_last:

        if len(response['hits']['hits']) != 10:
            is_last = True

        for record in response['hits']['hits']:
            # Process OpenSearch record
            stac_dict, file_ext = process_record(record['_source'], STAC_API)

            if isinstance(stac_dict, dict) == False:
                print(f"Unable to create STAC record.")
                continue

            # Create directory for each CCI ECV/Project
            ecv_dir=stac_dict["properties"]["collections"][0]
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

            with open(stac_file, 'w', encoding='utf-8') as file:
                json.dump(stac_dict, file, ensure_ascii=False, indent=2)
        
            print(f"Created STAC record: {stac_file}")

        searchAfter = response['hits']['hits'][-1]["sort"]
        body['search_after'] = searchAfter
        response = client.search(index='opensearch-files', body=body)

if __name__ == "__main__":
    main()
