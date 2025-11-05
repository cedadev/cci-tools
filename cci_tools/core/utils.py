__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import httpx
import json
from elasticsearch import Elasticsearch

dryrun = True

from httpx_auth import OAuth2ClientCredentials

def open_json(file):
    with open(file) as f:
        return json.load(f)

creds = open_json('AUTH_CREDENTIALS')

auth = OAuth2ClientCredentials(
    "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
    client_id=creds["id"],
    client_secret=creds["secret"]
)

def es_connection_kwargs(hosts, api_key, **kwargs):
    """
    Determine Elasticsearch connection kwargs
    """
    if isinstance(hosts, list):
        hosts = hosts[0]

    if hosts == 'https://elasticsearch.ceda.ac.uk':
        return {
            'hosts': [hosts],
            'headers':{'x-api-key':api_key},
            **kwargs
        }
    else:
        return {
            'hosts':[hosts],
            'api_key':api_key,
            **kwargs
        }

STAC_API = 'https://api.stac.164.30.69.113.nip.io'

client = httpx.Client(
    verify=False,
    timeout=180,
)

ES_API_KEY = open_json('API_CREDENTIALS')['secret']
ES_HOST = 'https://elasticsearch.164.30.69.113.nip.io'

es_client = Elasticsearch(
    **es_connection_kwargs(
        hosts=ES_HOST,
        api_key=ES_API_KEY
    )
)

ALLOWED_OPENSEARCH_EXTS = [
    '.cpg', '.csv', '.dat', '.dbf', '.dsr', '.geojson',
    '.gz' , '.jpg', '.kml', '.lyr', '.nc' , '.png', 
    '.prj', '.qpf', '.qml', '.qpj', '.sbn', '.sbx',
    '.shp', '.shx', '.tar', '.xml', '.zip'
]

COLLECTION_TEMPLATE = {
  "id": "test",
  "description": None,
  "stac_version": "1.1.0",
  "stac_extensions": [],
  "title": None,
  "type": "Collection",
  "license": "other",
  "links": [
    {
      "rel": "root",
      "type": "application/json",
      "href": "STAC_API"
    }
  ],
  "assets": {
    "thumbnail": {
      "roles": [
        "thumbnail"
      ],
      "href": "https://brand.esa.int/files/2020/05/ESA_logo_2020_Deep-1024x643.jpg",
      "type": "image/jpg"
    }
  },
  "extent": {
    "spatial": {
      "bbox": [-180,-90,180,90]
    },
    "temporal": {
      "interval": [
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:00:01Z"
      ]
    }
  },
  "keywords": None,
  "providers": [],
  "summaries": None
}

def get_dir_query(directory):
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

def get_file_query(file):
    file = file.split('/')[-1]
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "prefix": {
                            "info.name": file
                        }
                    },
                    {
                        "exists": {
                            "field": "projects.opensearch"
                        }
                    }
                ]
            }
        }, "sort": [{"info.name": {"order": "asc"}}, {"info.directory": {"order": "asc"}}], "size": 10
    }
    return query

def get_item_query(count_aggregations=True):

    query = {"term": {
      "properties.aggregation": {
        "value": False
      }
    }}
    if count_aggregations:
        query = {"match_all": {}}

    body = {
        "query": query,
        "size": 10
    }
    return body

def get_opensearch_record(moles_id, drs_id):

    url = f"https://archive.opensearch.ceda.ac.uk/opensearch/request?parentIdentifier={moles_id}&drsId={drs_id}&httpAccept=application/geo%2Bjson&maximumRecords=20&startPage=1"

    try:
        return client.get(url).json()
    except:
        # Known issue with moles uuid/collection duplicates:
        # A DRS can be allocated to multiple moles uuids if the `path` matches multiple moles uuids.
        print(url, 'ERROR')
        return None
    
def uuids_per_project(project, api_key):
    esc = Elasticsearch(
        hosts=['https://elasticsearch.ceda.ac.uk'],
        api_key=api_key
    )

    return [i['_source']['collection_id'] for i in esc.search(index='opensearch-collections', body={
        "query": {
            "match": {
                "project": project.lower(),
            }
        }
        }
    )['hits']['hits']]
    
def es_collection(uuid, api_key):
    esc = Elasticsearch(
        hosts=['https://elasticsearch.ceda.ac.uk'],
        api_key=api_key
    )

    return esc.search(index='opensearch-collections', body={
        "query": {
            "match": {
                "collection_id": uuid.lower(),
            }
        }
        }
    )['hits']['hits'][0]

def count_items(collection, item_aggregations=False, quick_check=False):
    """
    Remove all items for a specific collection."""

    body = get_item_query(count_aggregations=item_aggregations)
    response = es_client.search(index=f'items_{collection}', body=body)
    items = response['hits']['hits']

    if quick_check and len(items) > 0:
        return True
    
    return response['hits']['total']['value']

def recursive_find(
        collection, 
        collection_summary, 
        item_aggregations=False,
        depth=0,
        current_depth=1,
        quick_check=False
    ):
    """
    Remove collections recursively so no collections are left orphaned.
    
    This is less of an issue with collections vs items, but still with the large
    range of CCI collections this is important as orphaned collections may easily
    be 'lost'."""

    resp = client.get(collection)

    if resp.status_code == 404:
        return False, collection_summary
    
    coll_data = resp.json()

    collection_name = collection.split('/')[-1]

    try:
        item_count = count_items(
            collection_name, item_aggregations=item_aggregations, 
            quick_check=quick_check)
    except:
        item_count = 'N/A'

    if item_count == 10000:
        item_count = '>10000'

    if depth == current_depth or depth == 0:
        print(f"{collection.split('/')[-1]}: {item_count}")

    child_count = 0
    missing = 0
    for link in coll_data['links']:
        if link['rel'] == 'child':
            exists,collection_summary  = recursive_find(link['href'], 
                                                        collection_summary,item_aggregations=item_aggregations,
                                                        depth=depth, current_depth=current_depth+1, 
                                                        quick_check=quick_check)
            if exists:
                child_count += 1
            else:
                missing += 1
    if current_depth == depth:
        collection_summary.append((collection.split("/")[-1],item_count))

    if missing > 0:
        print(f' > {collection.split("/")[-1]} Missing: {missing}')
    return True, collection_summary
