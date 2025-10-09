import os
import json
import hashlib
import xarray as xr
from elasticsearch import Elasticsearch
from datetime import datetime

from cci_tools.collection.utils import STAC_API, client, auth, dryrun

def recursive_child_search(collection, depth):
    resp = client.get(f'{STAC_API}/collections/{collection}')

    if str(resp.status_code)[0] != '2':
        print(''.join(['>' for i in range(depth)]),collection, resp,)
        return

    for link in resp.json()['links']:
        if link['rel'] == 'child':
            recursive_child_search(link['href'].split('/')[-1], depth+1)

if __name__ == '__main__':
    recursive_child_search('cci',0)