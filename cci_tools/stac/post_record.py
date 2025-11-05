#!/usr/bin/env python
__author__    = "Diane Knappett"
__contact__   = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import httpx
from httpx_auth import OAuth2ClientCredentials
import click
import glob

from cci_tools.core.utils import STAC_API, client, auth

def post_record(stac_record, summaries):

    with open(stac_record, 'r') as file:
        # Load STAC record
        stac_data=json.load(file)

        # Ensure lower-case collections
        stac_data['collection'] = stac_data['collection'].lower()
        
        # Extract 'drsId' for collection name and 'id' for item name
        dataset_id = stac_data["collection"]
        item_id    = stac_data["id"]

        parent_href = f'{STAC_API}/collections/{stac_data["collection"]}'
        if parent_href not in summaries:
            summaries[parent_href] = {}

        for asset in stac_data['assets'].keys():
            if asset not in summaries[parent_href]:
                summaries[parent_href][asset] = {
                    'name': asset,
                    'common_name': asset,
                    'description':'None'
                }
    
        # Construct paths for STAC collection STAC item 
        stac_collection=STAC_API+"/collections/"+dataset_id+"/items"
        stac_item=stac_collection+"/"+item_id

        # Post a new STAC record
        response = client.post(
            stac_collection,
            json=stac_data,
            auth=auth
        )
        
        # If the STAC record already exists, just update it
        if response.status_code == 409:
            response = client.put(
                stac_item,
                json=stac_data,
                auth=auth
            )
        
    print('Item:',item_id, response)
    #print('Item:',item_id, response.content)
    return summaries
