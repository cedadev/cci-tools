from elasticsearch import Elasticsearch
import os

def uuids_per_project(project, api_key, hosts: list = None):
    """
    Get all collection uuids for a project from Elasticsearch.
    """
    if hosts is None:
        hosts = [os.environ.get('ES_HOST', 'https://elasticsearch.164.30.69.113.nip.io')]

    esc = Elasticsearch(
        hosts=hosts,
        api_key=api_key
    )

    hits = esc.search(index='opensearch-collections', body={
        "query": {
            "bool":{
                "must":[
                    {
                        "match": {
                            "project": project.lower().replace('_',' '),
                        }
                    }
                ],
                "must_not":[
                    {
                        "term":{
                            "versionStatus": "superseded"
                        }
                    }
                ]}
            }
        }
    )['hits']['hits']

    return [i['_source']['collection_id'] for i in hits]
    
def es_collection(uuid, api_key, hosts: list = None):

    if hosts is None:
        hosts = [os.environ.get('ES_HOST', 'https://elasticsearch.164.30.69.113.nip.io')]

    esc = Elasticsearch(
        hosts=hosts,
        api_key=api_key
    )

    return esc.search(index='opensearch-collections', body={
        "query": {
            "bool": {
                "must":[
                    {
                        "match": {
                            "collection_id": uuid.lower(),
                        }
                    }
                ],
                "must_not":[
                    {
                        "term":{
                            "versionStatus": "superseded"
                        }
                    }
                ]}
            }
        }
    )['hits']['hits'][0]['_source']