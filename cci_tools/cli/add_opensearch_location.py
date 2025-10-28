__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import os, sys
from elasticsearch import Elasticsearch
from cci_tools.core.utils import es_client

if len(sys.argv) < 4:
    raise ValueError(
        'Please provide parameters "fileset_path", "format" and "location"'
    )

fileset  = sys.argv[-3] # Path to the list of files covered by the Kerchunk/Zarr dataset
format   = sys.argv[-2] # Format i.e kerchunk/zarr
location = sys.argv[-1] # Path to remote kerchunk/zarr dataset.

def path_based_query(path, filename):
    """
    Returns an Elasticsearch search based on the path
    and filename specified.
    """
    return {
        "query":{
            "bool":{
                "must":[{
                    "term": {
                        "info.directory":path 
                        }
                },{
                    "term": {
                        "info.name":filename
                        }
                }]
            }
        }
    }

def update_opensearch(filelist, format, location):
    """
    Update the opensearch records corresponding to a given filelist.
    
    """
    for filepath in filelist:

        filename = filepath.split('/')[-1]
        path = filepath.replace(filename,'')
        try:
            refs = es_client.search(
                index='opensearch-files',
                query=path_based_query(path, filename)
            )['hits']['hits'][0]
        except Exception as _:
            refs = es_client.search(
                index='opensearch-files',
                query=path_based_query(f'{path}/',filename)
            )['hits']['hits'][0]

        refs['_source']['info'][f'{format}_location'] = location

        es_client.update(
            index='opensearch-files',
            id=refs['_id'],
            body={'doc':refs['_source'],'doc_as_upsert':True}
        )


if __name__ == '__main__':

    if not os.path.isfile(fileset):
        raise FileNotFoundError(f'{fileset} could not be located.')

    with open(fileset) as f:
        lines = [r.strip() for r in f.readlines()]
    update_opensearch(lines, format, location)