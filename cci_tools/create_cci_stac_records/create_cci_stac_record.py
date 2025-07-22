#!/usr/bin/env python
"""
This script performs an elasticsearch query for the directory defined in variable cci_dir 
and then converts the opensearch record output to STAC format.

Currently run from the command line using:
python create_cci_stac_record.py cci_dir

e.g. python create_cci_stac_record.py /neodc/esacci/biomass/data/agb/maps/v6.0/netcdf
"""
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import pandas as pd
import json 
import requests
import os
import click

def get_query(directory):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase_prefix": {
                            "info.directory.analyzed": directory
                        }
                    },
                    {
                        "exists": {
                            "field": "projects.opensearch"
                        }
                    }
                ]
            }
        }
    }
    return query

def process_record(es_all_dict:dict, output_dir:str):
    #=== id ===
    fname = str(es_all_dict['info'].get('name'))
    file_id, file_ext = os.path.splitext(fname)
    #id = fname.partition('.nc')[0]

    #=== Collection ===
    ecv = es_all_dict['projects']['opensearch'].get('ecv')

    if type(ecv) is list:
        if len(ecv) == 1:
            ecv = str(ecv[0]).lower()
        else:
            print('ECV is a list!!')
            
    #=== bbox ===
    # W, N, E, S
    coords = es_all_dict['info']['spatial']['coordinates'].get('coordinates')
    bbox_w = coords[0][0] # west
    bbox_n = coords[0][1] # north
    bbox_e = coords[1][0] # east
    bbox_s = coords[1][1] # south
    bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

    #=== Geometry ===
    geo_type = 'Polygon'
    coordinates = [[bbox_w, bbox_s], [bbox_e, bbox_s], [bbox_e, bbox_n], [bbox_w, bbox_n], [bbox_w, bbox_s]]

    #=== Properties ===
    start_datetime = str(es_all_dict['info']['temporal'].get('start_time'))
    end_datetime = str(es_all_dict['info']['temporal'].get('end_time'))
    version = es_all_dict['projects']['opensearch'].get('productVersion')
    platforms = es_all_dict['projects']['opensearch'].get('platform')
    drs = es_all_dict['projects']['opensearch'].get('drsId')
    uuid = es_all_dict['projects']['opensearch'].get('datasetId')

    properties = dict()
    for property,value in es_all_dict['projects']['opensearch'].items():
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        properties[property] = value

    # License    
    for license in ['_terms_and_conditions_v2.pdf','_terms_and_conditions.pdf','.pdf']:
        r = requests.get(f'https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}')
        if r.status_code == 200:
            break
    url = f'https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}'

    format = es_all_dict['info'].get('format')
    location = es_all_dict['info'].get('directory')

    # Create a dictionary of the required STAC output
    stac_dict = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
            ],
        "id": file_id,
        "collection": drs,
        "geometry": {
            "type": geo_type,
            "coordinates": [coordinates]
        },
        "bbox": bbox,
        "properties": {
            "datetime": None,
            "start_datetime": start_datetime.partition('+')[0]+'Z',
            "end_datetime": end_datetime.partition('+')[0]+'Z',
            "license": "other",
            "version": version,
            "aggregation": False,
            "platforms": platforms,
            "collections":[ecv, uuid, drs],
            **properties
        },
        "links": [
            {
            "rel": "self",
            "type": "application/geo+json",
            "href": "https://api.stac.ceda.ac.uk/collections/"+drs+"/items/"+file_id
            },
            {
            "rel": "parent",
            "type": "application/json",
            "href": "https://api.stac.ceda.ac.uk/collections/"+drs  
            },
            {
            "rel": "collection",
            "type": "application/json",
            "href": "https://api.stac.ceda.ac.uk/collections/"+drs  
            },
            {
            "rel": "root",
            "type": "application/json",
            "href": "https://api.stac.ceda.ac.uk/"
            },
            {
            "rel": "license",
            "type": "application/pdf",
            "href": url
            }
        ],
        "assets": {
            format: {
                "href": location+"/"+fname,
                "role": [
                    "data"
                ]
            }
        }
    }

    # Remove platform until STAC standards have been updated to allow lists of platforms. 
    # Until then, the platform list is entered as 'platforms' instead.
    stac_dict['properties'].pop('platform')

    # Create directory for each CCI ECV/Project
    cci_stac_dir=output_dir+ecv+'/'

    if os.path.isdir(cci_stac_dir)==False:
        try:
            os.mkdir(cci_stac_dir)
            print(f"Created directory '{cci_stac_dir}' successfully")
        except PermissionError:
            print(f"Permission denied: Unable to make '{cci_stac_dir}'")
        except Exception as e:
            print(f"An error occured '{e}'")
    
    stac_file=cci_stac_dir+'stac_'+file_id+'-'+file_ext[1:]+'.json'

    # Write 'pretty print' STAC json file
    with open(stac_file, 'w', encoding='utf-8') as file:
        json.dump(stac_dict, file, ensure_ascii=False, indent=2)
    
    print('Created STAC record: '+stac_file)


# Parse command line arguments using click
@click.command()
@click.argument('cci_dir', type=click.Path(exists=True))

##### MAIN PROGRAM #####

def main(cci_dir):

    # Define path for output stac directory/records
    output_dir = '/gws/nopw/j04/esacci_portal/stac/stac_records/'

    # Setup client and query elasticsearch
    client = Elasticsearch(hosts=['https://elasticsearch.ceda.ac.uk'])
    response = elasticsearch.helpers.scan(client, 
                                          query=get_query(cci_dir), 
                                          index="opensearch-files")
    
    # Loop over OpenSearch records, converting each to STAC format
    for record in response:
        process_record(record['_source'], output_dir)


if __name__ == "__main__":
    main()
