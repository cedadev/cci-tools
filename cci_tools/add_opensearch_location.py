import json
import os, sys
from elasticsearch import Elasticsearch

ES_API_KEY=os.environ.get("ES_API_KEY") or None

cli = Elasticsearch(
    hosts=['https://elasticsearch.ceda.ac.uk'],
    headers={'x-api-key':ES_API_KEY}
)

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
            refs = cli.search(
                index='opensearch-files',
                query=path_based_query(path, filename)
            )['hits']['hits'][0]
        except Exception as _:
            refs = cli.search(
                index='opensearch-files',
                query=path_based_query(f'{path}/',filename)
            )['hits']['hits'][0]

        refs['_source']['info'][f'{format}_location'] = location

        cli.update(
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