from elasticsearch import Elasticsearch
import xarray as xr
import numpy as np
import os

api_key = os.environ.get("ES_API_KEY")

cli = Elasticsearch(
    hosts=['https://elasticsearch.ceda.ac.uk'],
    headers={'x-api-key':api_key}
)

query = {
  "query":{
    "term": {
      "info.temporal.start_time":"1970-01-01T00:00:00+00:00" 
      }
  }
}

files_affected = cli.search(index='opensearch-files',body=query)

for hit in files_affected['hits']['hits']:
    file = f'{hit["_source"]["info"]["directory"]}/{hit["_source"]["info"]["name"]}'

    ds = xr.open_dataset(file)
    print(str(np.array(ds.time[0].compute())))

    nt = str(np.array(ds.time[1].compute())).split('T')[0]
    ntime = f'{nt}T00:00:00'
    print(ntime)

    hit["_source"]["info"]["temporal"]["start_time"] = ntime
    hit["_source"]["info"]["temporal"]["time_range"]["gte"] = ntime

    cli.update(
        index='opensearch-files',
        id=hit["_id"],
        body={'doc': hit["_source"], 'doc_as_upsert': True}
    )