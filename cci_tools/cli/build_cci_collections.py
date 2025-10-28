__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import copy
import requests

# Top Level CCI

## Projects (ECVs) + reccap2 + Sea Level Budget Closure
## from slug

### Features (Moles Datasets)
### 

from httpx import Client
from httpx_auth import OAuth2ClientCredentials
from elasticsearch import Elasticsearch

from cci_tools.collection.main import create_project_collection, remove_duplicate_links
from cci_tools.core.utils import client, auth, STAC_API

import os
import click

@click.command()
@click.option('--dryrun', 'dryrun', is_flag=True, required=False)

def main(dryrun):

    with open('config/cci_ecv_config.json') as f:
        config = json.load(f)


    exists = False
    current = client.get(f"{STAC_API}/collections/cci")
    if str(current.status_code)[0] == '2':
        exists = True
        cci = current.json()
    else:
        with open('stac_collections/cci.json') as f:
            ccio = ''.join(f.readlines()).replace('STAC_API',STAC_API)
        cci = json.loads(ccio)

    top_ignore = ["facet_config","ecv_labels","ecv_title_ids","full_search_results"]
    keys = [c for c in config.keys() if c not in top_ignore]

    for project in (keys + ['reccap2','sea-level-budget-closure']):
        
        cci = create_project_collection(
            project,
            cci,
            project_reference=config,
            dryrun=dryrun
        )

    cci['links'] = remove_duplicate_links(cci['links'])

    if not dryrun:
        if exists:
            response = client.put(
                f"{STAC_API}/collections/cci",
                json=cci,
                auth=auth,
            )
        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=cci,
                auth=auth,
            )
        print(f'CCI: {response}')
    else:
        print('CCI: Skipped')
        with open('stac_collections/gen/cci.json','w') as f:
            f.write(json.dumps(cci))
    
if __name__ == '__main__':
    main()