# Initialise by creating opensearch-style kerchunk record (osr_kch)
# Process opensearch record into STAC
# Export as normal

import os
import json
import hashlib
import xarray as xr
from elasticsearch import Elasticsearch
from datetime import datetime
import glob
import copy

from cci_tools.core.utils import STAC_API, client, auth, dryrun
from cci_tools.stac.openeo import process_record

# Setup client and query elasticsearch
with open('API_CREDENTIALS') as f:
    api_creds = json.load(f)
    API_KEY = api_creds["secret"]

escli = Elasticsearch(
    hosts=['https://elasticsearch.164.30.69.113.nip.io'],
    api_key=API_KEY)
failed = []

KERCHUNK_TEMPLATE={
  "type": "Feature",
  "stac_version": "1.1.0",
  "stac_extensions": [
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
  ],
  "id": "198201-201812-ESACCI-L4_FIRE-BA-AVHRR-LTDR-fv1.1_kr1.0",
  "collection": "62866635ab074e07b93f17fbf87a2c1a-main",
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [-180, -90],
        [180, -90],
        [180, 90],
        [-180, 90],
        [-180, -90]
      ]
    ]
  },
  "bbox": [-180, -90, 180, 90],
  "properties": {
    "datetime": None,
    "created": "2025-09-16T13:27:15.946018Z",
    "updated": "2025-09-17T15:03:28.166031Z",
    "start_datetime": "1982-01-01T00:00:00Z",
    "end_datetime": "2018-12-31T23:59:59Z",
    "license": "other",
    "version": [
      "1.1"
    ],
    "aggregation": True,
    "platforms": [
      "NOAA-19",
      "NOAA-9",
      "NOAA-11",
      "NOAA-14",
      "NOAA-16",
      "NOAA-18",
      "NOAA-7"
    ],
    "collections": [
      "fire",
      "62866635ab074e07b93f17fbf87a2c1a",
      "62866635ab074e07b93f17fbf87a2c1a-main"
    ],
    "opensearch_url": "https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier=62866635ab074e07b93f17fbf87a2c1a",
    "esa_url": "https://climate.esa.int/en/catalogue/62866635ab074e07b93f17fbf87a2c1a/",
    "platformGroup": "NOAA",
    "productVersion": "1.1",
    "processingLevel": "L4",
    "dataType": "BA",
    "ecv": "FIRE",
    "datasetId": "62866635ab074e07b93f17fbf87a2c1a",
    "project": "FIRE",
    "institute": "University of Alcala",
    "sensor": [
      "AVHRR-3",
      "AVHRR-2"
    ],
    "frequency": "month"
  },
  "links": [
    {
      "rel": "self",
      "type": "application/geo+json",
      "href": "https://api.stac.164.30.69.113.nip.io/collections/62866635ab074e07b93f17fbf87a2c1a-main/items/198201-201812-ESACCI-L4_FIRE-BA-AVHRR-LTDR-fv1.1_kr1.0"
    },
    {
      "rel": "parent",
      "type": "application/json",
      "href": "https://api.stac.164.30.69.113.nip.io/collections/62866635ab074e07b93f17fbf87a2c1a-main"
    },
    {
      "rel": "collection",
      "type": "application/json",
      "href": "https://api.stac.164.30.69.113.nip.io/collections/62866635ab074e07b93f17fbf87a2c1a-main"
    },
    {
      "rel": "root",
      "type": "application/json",
      "href": "https://api.stac.164.30.69.113.nip.io/"
    },
    {
      "href": "https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_fire_terms_and_conditions.pdf",
      "rel": "license",
      "type": "application/pdf"
    }
  ],
  "assets": {
    "reference_file": {
      "href": "https://dap.ceda.ac.uk/neodc/esacci/fire/metadata/kerchunk/burned_area/AVHRR-LTDR/grid/v1.1/198201-201812-ESACCI-L4_FIRE-BA-AVHRR-LTDR-fv1.1_kr1.0.json",
      "roles": [
        "data"
      ],
      "cloud_format": "kerchunk"
    }
  }
}

def process_opensearch(line, kdir, kfile, ndir, nfile, tcs, tce):

    hits = escli.search(
        index='opensearch-files',
        body = {
            "query":{
                "bool": {
                    "must":[
                        {
                        "match":{
                            "info.directory":ndir,
                        }
                        },
                        {
                        "match":{
                            "info.name":nfile,
                        }
                    }
                ]
            }
        }
    })

    try:
        record = hits['hits']['hits'][0]
    except:
        failed.append((nfile,'no_hits'))
        raise ValueError('no hits')

    # Access the record here

    record['_source']['info']['name_auto'] = kfile
    record['_source']['info']['format'] = 'Kerchunk'
    record['_source']['info']['type'] = '.json'
    record['_source']['info']['directory'] = kdir
    record['_source']['info']['size'] = int(os.path.getsize(line))
    record['_source']['info']['name'] = kfile
    record['_source']['info']['temporal'] = {
        'start_time': tcs,
        'end_time': tce,
        'time_range':{
            'gte':tcs,
            'lte':tce
        }
    }
    record['_id'] = hashlib.sha1(line.encode(errors="ignore")).hexdigest()

    return record

def process_file(line, count, total_files):
    print(f'{count+1}/{total_files}')
    kfile = line.split('/')[-1]
    kdir  = line.replace(f'/{kfile}','')

    if int(os.path.getsize(line)) > 500000000:
        failed.append((line,'too_large'))
        raise ValueError('Too large')
        
    with open(line) as f:
        dsr = json.load(f)

    addn = False
    id = hashlib.sha1(line.encode(errors="ignore")).hexdigest()

    if os.path.isfile(f"osr_cache/{id}"):
        print(' > skipped')
        return

    print(' > Localising data retrieval')
    for ref in dsr['refs'].keys():
        if len(dsr['refs'][ref]) == 3:
            dsr['refs'][ref][0] = dsr['refs'][ref][0].replace('https://dap.ceda.ac.uk/','/')
            sourcefile = dsr['refs'][ref][0]

    ds = xr.open_dataset(dsr, engine='kerchunk')

    tcs, tce = None, None

    for fmt in [
        "%Y%m%dT%H%M%SZ",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y%m%d",
        "%Y%m%dT%H%M%S",
        "%Y%m%d%H%M%SZ",
        "%Y%m%d%H%M%S"]:
        try:
            tcs = datetime.strftime(
                datetime.strptime(ds.time_coverage_start, fmt),
                "%Y-%m-%dT%H:%M:%S"
            )
            tce = datetime.strftime(
                datetime.strptime(ds.time_coverage_end, fmt),
                "%Y-%m-%dT%H:%M:%S"
            )
        except:
            pass

    if tcs is None:
        try:
            print(ds.time_coverage_end)
            failed.append((line,ds.time_coverage_end))
        except:
            print('No time option at all - skipping')
            failed.append((line,'temporal_unsp'))
            return
        
    ds.close()

    nfile = sourcefile.split('/')[-1]
    ndir  = sourcefile.replace(f'/{nfile}','').replace('https://dap.ceda.ac.uk/','/')

    hits = escli.search(
        index='opensearch-files',
        body = {
            "query":{
                "bool": {
                    "must":[
                        {
                        "match":{
                            "info.directory":ndir,
                        }
                        },
                        {
                        "match":{
                            "info.name":nfile,
                        }
                    }
                ]
            }
        }
    })

    try:
        record = hits['hits']['hits'][0]
    except:
        failed.append((line,'no_hits'))
        raise ValueError('no hits')
    
    drsId = record['_source']['projects']['opensearch'].get('drsId',None)
    if isinstance(drsId, list):
        drsId = drsId[0]
    if drsId is None:
        drsId = record['_source']['projects']['opensearch']['datasetId'] + '-main'

    # Get STAC representation

    collection_link = f'{STAC_API}/collections/{drsId.lower()}'
    print(collection_link, drsId)
    try:
        stac_record = client.get(f'{collection_link}/items').json()['features'][0]
    except:
        print(' > No items - creating from opensearch')
        opensearch_record = process_opensearch(line, kdir, kfile, ndir, nfile, tcs, tce)
        stac_record, ext  = process_record(opensearch_record['_source'], STAC_API)

        stac_record['collection'] = stac_record['collection'].lower()
        stac_record['properties']['platforms'] = stac_record['properties'].pop('platform')
        print('Successful record creation')

    stac_record['properties']['start_datetime'] = tcs + 'Z'
    stac_record['properties']['end_datetime'] = tce + 'Z'
    stac_record['properties']['aggregation'] = True
    # Adjust self link
    stac_record['assets'] = {
        'reference_file':{
            'href':line.replace('/neodc','https://dap.ceda.ac.uk/neodc'),
            'roles':['data'],
            'cloud_format': 'kerchunk'
        }
    }

    for link in stac_record['links']:
        if link['rel'] == 'self':
            link['href'] = link['href'].replace(stac_record['id'], kfile.rstrip('.json'))
            selflink = link['href']

    stac_record['id'] = kfile.rstrip('.json')
    
    resp = client.put(selflink, json=stac_record, auth=auth)
    if str(resp.status_code)[0] != '2':
        resp = client.post(f'{collection_link}/items', json=stac_record, auth=auth)
    print(kfile, resp)
    if resp.status_code == 400:
        print(resp.content)
        x=input()


if __name__ == '__main__':

    fileset = glob.glob('/neodc/esacci/*/metadata/kerchunk/**/*kr*.json', recursive=True)
    for x, kfile in enumerate(fileset):
        try:
            process_file(
                kfile,
                x,
                len(fileset)
            )
        except KeyboardInterrupt:
            break
        except KeyError:
            print('Failed to find spatial')
            failed.append((kfile,'spatial_missing'))
        except:
            failed.append((kfile,'general_error'))

    with open('kerchunk_remaining.txt','w') as f:
        f.write('\n'.join([','.join([i for i in f]) for f in failed]))