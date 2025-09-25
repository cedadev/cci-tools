#!/usr/bin/env python
__author__    = "Diane Knappett"
__contact__   = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

"""
This script posts a single STAC record to the STAC catalogue:
https://radiantearth.github.io/stac-browser/#/external/api.stac-master.rancher2.130.246.130.221.nip.io/collections/cci

Usage: python post_cci_stac_record.py stac_record
e.g. python post_cci_stac_record.py /gws/nopw/j04/esacci_portal/stac/stac_records/biomass/stac_ESACCI-BIOMASS-L4-AGB-MERGED-100m-2007-fv6.0-nc.json
"""
import json
import httpx
from httpx_auth import OAuth2ClientCredentials
import click
import glob

#@click.command()
#@click.argument('stac_record', type=click.Path(exists=True))

STAC_API='https://api.stac.164.30.69.113.nip.io'

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

@click.command()
@click.argument('post_directory')
@click.option('--openeo', help='Flag for enabling openEO-specific posting rules')
def main(post_directory, openeo: bool = False):

    if post_directory.isnumeric():
        path_file='/gws/nopw/j04/esacci_portal/stac/stac_records/post_stac/stac_record_dirs_to_post.txt'
        with open(path_file) as f:
            post_directory=[r.strip() for r in f.readlines()][int(post_directory)]

    summaries = {}
    for record in glob.glob(f'{post_directory}/stac*.json'):
        summaries = post_record(record, summaries)

    if not openeo:
        return

    for href, summary in summaries.items():
        parent = client.get(href).json()

        summaries = parent.get('summaries',None)
        if summaries is None:
            summaries = {}
            summary_names = []
        else:
            summary_names = [i['name'] for i in summaries.get('eo:bands',{}) if 'name' in i]

        repost_summaries = False
        summaries_set = summaries.get('eo:bands',[])
        for name, band in summary.items():
            if name not in summary_names:
                summaries_set.append(band)
                repost_summaries=True

            # Need to be able to update the summaries.

        if parent['summaries'] is None and repost_summaries:
            parent['summaries'] = {'eo:bands':[]}

        parent['summaries']['eo:bands'] = summaries_set
        if repost_summaries:
            print('Parent:',href.split('/')[-1], client.put(href, json=parent, auth=auth))


if __name__ == "__main__":
    main()
