from elasticsearch import Elasticsearch
import os

import logging
from cci_tools.core.utils import logstream, ES_HOST

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def uuids_per_project(project, api_key, hosts: list = None):
    """
    Get all collection uuids for a project from Elasticsearch.
    """
    if hosts is None:
        hosts = [
            os.environ.get("ES_HOST", ES_HOST)
        ]

    esc = Elasticsearch(hosts=hosts, api_key=api_key)

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "project": project.lower().replace("_", " "),
                        }
                    }
                ],
                "must_not": [{"term": {"versionStatus": "superseded"}}],
            }
        }
    }
    logger.debug(f'Searching collections with query: {query}')

    hits = esc.search(
        index="opensearch-collections",
        body=query
    )["hits"]["hits"]

    logger.debug(f'Found hits: {len(hits)}')
    return [i["_source"]["collection_id"] for i in hits]


def es_collection(uuid, api_key, hosts: list = None):

    if hosts is None:
        hosts = [
            os.environ.get("ES_HOST", ES_HOST)
        ]

    esc = Elasticsearch(hosts=hosts, api_key=api_key)
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "collection_id": uuid.lower(),
                        }
                    }
                ],
                "must_not": [{"term": {"versionStatus": "superseded"}}],
            }
        }
    }
    logger.debug(f'Searching for collection with query: {query}')

    hits = esc.search(
        index="opensearch-collections",
        body=query
    )["hits"]["hits"]

    if not hits:
        return None

    return hits[0]["_source"]
