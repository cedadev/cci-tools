from elasticsearch import Elasticsearch

def get_query():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "prefix": {
                            "info.directory": "/neodc/esacci/biomass/data/agb/maps/v6.0/geotiff"
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

client = Elasticsearch(
    hosts=['https://elasticsearch.ceda.ac.uk'],
    headers={'x-api-key':'U1NJNWVaY0JIaXQwWVVYMnJNZkw6bmhMQ3VlT2hyMjBMZEt6akxXRkQ4Zw=='}
)


body = get_query()
count_items = 0
response = client.search(index='opensearch-files', body=body)
is_last = False
while len(response['hits']['hits']) == 10 or not is_last:

    if len(response['hits']['hits']) != 10:
        is_last = True

    for hit in response['hits']['hits']:
        # do stuff (in a function)
        print(count_items, hit['_source']['info']['name'])
    searchAfter = response['hits']['hits'][-1]["sort"]
    body['search_after'] = searchAfter
    response = client.search(index='opensearch-files', body=body)
    count_items += 1