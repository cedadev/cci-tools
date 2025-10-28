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