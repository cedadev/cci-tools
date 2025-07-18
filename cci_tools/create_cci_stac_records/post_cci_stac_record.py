#!/usr/bin/env python
"""
This script posts a single STAC record to the STAC catalogue

Usage: 
"""
import json
import httpx
from httpx_auth import OAuth2ClientCredentials

stac_record='/gws/nopw/j04/esacci_portal/stac/stac_records/biomass/stac_ESACCI-BIOMASS-L4-AGB-MERGED-100m-2015-fv6.0-nc.json'

client = httpx.Client(
    verify=False,
    timeout=180,
)

with open('AUTH_CREDENTIALS') as f:
    creds = json.load(f)

auth = OAuth2ClientCredentials(
    "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
    client_id=creds["id"],
    client_secret=creds["secret"]
)

with open(stac_record, 'r') as file:
    # Load STAC record
    stac_data=json.load(file)
    
    # Extract 'drsId' for collection name and 'id' for item name
    dataset_id=stac_data["properties"]["drsId"]
    item_id=stac_data["id"]

    # Construct paths for STAC collection STAC item 
    stac_collection="https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/"+dataset_id+"/items"
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
    
print(response)
