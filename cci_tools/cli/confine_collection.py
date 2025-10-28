__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

from cci_tools.core.utils import client, auth, STAC_API, es_client
import click
import json
from datetime import datetime
from elasticsearch import Elasticsearch

import os

def get_query():
    return {
        "query": {
            "match_all": {}
        }, "sort": [{"id": {"order": "asc"}}], "size": 10
    }

def confine_by_items(
        collection_name: str,
        start_datetime: str, 
        end_datetime: str, 
        bbox: list,
    ):
    # Define elasticsearch client
    # Get collection 10 items at a time
    # Continue checking each item using confine components

    response=es_client.search(index=f'items_{collection_name}', query={"match_all": {}},sort=[{"id": {"order": "asc"}}], size=10)

    is_last = False
    while len(response['hits']['hits']) == 10 or not is_last:

        if len(response['hits']['hits']) != 10:
            is_last = True

        for record in response['hits']['hits']:
            print(record['_source']['id'], record['_source']['bbox'])

            extent = {
                'temporal':{
                    'interval':[[
                        record['_source']['properties']['start_datetime'],
                        record['_source']['properties']['end_datetime']
                    ]]
                },
                "spatial":{
                    "bbox":[record['_source']['bbox']]
                }
            }


            start_datetime, end_datetime, bbox = confine_components(
                extent, start_datetime, end_datetime, bbox)

        searchAfter = response['hits']['hits'][-1]["sort"]
        response = es_client.search(index=f'items_{collection_name}', query={"match_all": {}},sort=[{"id": {"order": "asc"}}], size=10, search_after=searchAfter)

        if len(response["hits"]["hits"]) == 0:
            is_last=True
        
    return start_datetime, end_datetime, bbox

def confine_components(extent, start_datetime, end_datetime, bbox):

    start_datetime = sorted([extent['temporal']['interval'][0][0], start_datetime])[0]
    end_datetime = sorted([extent['temporal']['interval'][0][1], end_datetime])[1]
    
    bbox_w = min(bbox[0][0], extent['spatial']['bbox'][0][0])
    bbox_e = max(bbox[0][2], extent['spatial']['bbox'][0][2])

    bbox_n = max(bbox[0][3], extent['spatial']['bbox'][0][3])
    bbox_s = min(bbox[0][1], extent['spatial']['bbox'][0][1])

    if bbox_w < -180 or bbox_e > 180 or bbox_n > 90 or bbox_s < -90:
        print(extent)
        x=input()

    return start_datetime, end_datetime, [[
        float(f'{bbox_w:.2f}'),
        float(f'{bbox_s:.2f}'),
        float(f'{bbox_e:.2f}'),
        float(f'{bbox_n:.2f}')
    ]]

def confine_collection(
        collection_data: dict, 
        start_datetime: str, 
        end_datetime: str, 
        bbox: list, 
        child_based: bool = False):

    if not child_based:
        start_datetime, end_datetime, bbox = confine_by_items(
            collection_data['id'],
            start_datetime, end_datetime, bbox
        )

    for link in collection_data.get('links',[]):
        if link['rel'] == 'child':
            print(f'> C: {link["href"]}')

            child = client.get(link['href']).json()

            start_datetime, end_datetime, bbox = confine_components(
                child['extent'], 
                start_datetime, end_datetime, bbox
            )
    return start_datetime, end_datetime, bbox

# Parse command line arguments using click
@click.command()
@click.argument("collection")
@click.option('--child-based', 'child_based', required=False, is_flag=True,
              help='Confine using child collection data')

def main(collection: str, child_based=False):

    collection=collection.lower()
    print(collection)

    if client.get(f"{STAC_API}/collections/{collection}").status_code == 404:
        raise ValueError(f'Cannot confine {collection} - not found')

    # Get all items/sub-collections
    start_datetime = '9999-01-01T00:00:00Z'
    end_datetime = '0000-01-01T00:00:00Z'
    bbox = [[180,90,-180,-90]] # Reversed bbox

    coll_data = client.get(f"{STAC_API}/collections/{collection}").json()

    start_datetime, end_datetime, bbox = confine_collection(
        coll_data, start_datetime, end_datetime, bbox, 
        child_based=child_based) # Recursive function to find items and sub collections

    print(start_datetime, end_datetime, bbox)
    if input('Apply changes? (Y/N) ') != 'Y':
        return
    
    coll_data['extent']['spatial']['bbox'] = bbox
    coll_data['extent']['temporal']['interval'] = [[start_datetime, end_datetime]]
    resp = client.put(f"{STAC_API}/collections/{collection}", json=coll_data, auth=auth)
    if str(resp.status_code)[0] != '2':
        print(resp.content, resp.status_code)

if __name__ == '__main__':
    main()