# Inputs:
# - DRS/ID for openeo collection name
# - Path for locating opensearch record OR moles uuid

import json
import click
import requests
from elasticsearch import Elasticsearch

def get_uuid(path):

    cli = Elasticsearch(hosts=['https://elasticsearch.ceda.ac.uk'])
    opensearch_resp = cli.search(
        index='opensearch-files',
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "prefix": {
                                "info.directory": path
                            }
                        },
                        {
                            "exists": {
                                "field": "projects.opensearch.datasetId"
                            }
                        }
                    ]
                }
            }
        }
    )
    return opensearch_resp['hits']['hits'][0]['_source']['projects']['opensearch'].get('datasetId')

def get_moles(uuid):

    resp = requests.get(f'https://catalogue.ceda.ac.uk/api/v2/observations.json?uuid={uuid}')
    try:
        moles_resp = resp.json()['results'][0]
    except IndexError:
        moles_resp = {}

    return moles_resp

def get_opensearch_hit(uuid) -> dict:
    """
    Query elasticsearch for the opensearch collections metadata
    """
    cli = Elasticsearch(hosts=['https://elasticsearch.ceda.ac.uk'])

    opensearch_resp = cli.search(
        index='opensearch-collections',
        body={
            "query":{
                "bool": {
                "must":[
                    {
                    "match":{
                        "collection_id":uuid
                    }
                    }
                ]
                }
            }
        })
    
    try:
        opensearch_hit = opensearch_resp['hits']['hits'][0]
    except IndexError or KeyError:
        opensearch_hit = {}
    
    return opensearch_hit.get('_source',{})

# Parse command line arguments using click
@click.command()
@click.argument('drs')
@click.option('--path', required=False)

@click.option('--uuid', required=False)
@click.option('--formats', required=False)
def main(drs, path=None, uuid=None, formats=None):

    STAC_API = 'https://api.stac.164.30.69.113.nip.io'
    DEFAULTS = ['openeo']

    if path is not None:
        uuid = get_uuid(path)

    title = drs
    drs = drs.lower()
    if '.openeo' not in drs:
        drs = drs + '.openeo'

    metadata = get_moles(uuid)
    opensearch_hit = get_opensearch_hit(uuid)

    with open('stac_collections/openeo_collection_template.json') as f:
        template = ''.join([r.strip() for r in f.readlines()])

    desc = str(metadata['abstract'])
    with open('desc.txt','w') as f:
        f.write(repr(desc)) 

    template = template.replace('STAC_API',STAC_API)
    template = template.replace('SELF',drs)
    template = template.replace('TITLE',title)
    template = template.replace('UUID',uuid)
    try:
        template_json = json.loads(template)
    except:
        raise ValueError('Failed JSON serialisation')

    defaults = DEFAULTS
    if formats is not None:
        defaults += formats.split(',')
    
    keywords = defaults + drs.split('.') + [k.strip() for k in metadata.get('keywords',None).split(',')]
    template_json['keywords'] = list(set(keywords)) + [uuid]
    template_json['summaries'] = None
    template_json['description'] = desc
    template_json['providers'] = [
        {
            'roles': ["host"],
            'name': 'Centre for Environmental Data Analysis (CEDA)',
            'url': 'https://catalogue.ceda.ac.uk'
        }
    ]

    start_datetime = opensearch_hit.get('start_date','2025-09-19T00:00:00Z')
    end_datetime   = opensearch_hit.get('end_date','2025-09-19T00:00:00Z')

    bbox = [[-180, -90, 180, 90]]

    template_json['extent'] = {
        "spatial": {
            "bbox": bbox,
        },
        "temporal": {
            "interval": [
                [
                start_datetime,
                end_datetime
                ]
            ]
        }
    }

    # Fill in template

    with open(f'openeo/collections/{drs}.json','w') as f:
        f.write(json.dumps(template_json))

if __name__ == '__main__':
    main()