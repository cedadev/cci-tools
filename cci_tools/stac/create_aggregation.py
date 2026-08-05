# Initialise by creating opensearch-style kerchunk record (osr_kch)
# Process opensearch record into STAC
# Export as normal

import os
import hashlib
import xarray as xr
import requests
from datetime import datetime
import glob

from cci_tools.core.utils import STAC_API, client, auth
from cci_tools.collection.main import confine_components, remove_duplicate_links

ACCEPT_LONS = ['lon', 'longitude','Lon', 'Longitude']
ACCEPT_LATS = ['lat', 'latitude', 'Lat', 'Latitude']


def process_opensearch(line, kdir, kfile, ndir, nfile, tcs, tce):

    hits = es_client.search(
        index="opensearch-files",
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "info.directory": ndir,
                            }
                        },
                        {
                            "match": {
                                "info.name": nfile,
                            }
                        },
                    ]
                }
            }
        },
    )

    try:
        record = hits["hits"]["hits"][0]
    except:
        failed.append((nfile, "no_hits"))
        raise ValueError("no hits")

    # Access the record here

    record["_source"]["info"]["name_auto"] = kfile
    record["_source"]["info"]["format"] = "Kerchunk"
    record["_source"]["info"]["type"] = ".json"
    record["_source"]["info"]["directory"] = kdir
    record["_source"]["info"]["size"] = int(os.path.getsize(line))
    record["_source"]["info"]["name"] = kfile
    record["_source"]["info"]["temporal"] = {
        "start_time": tcs,
        "end_time": tce,
        "time_range": {"gte": tcs, "lte": tce},
    }
    record["_id"] = hashlib.sha1(line.encode(errors="ignore")).hexdigest()

    return record


def process_endpoint(endpoint: str, item: dict, engine: str = 'kerchunk') -> tuple:
    """
    Template the Aggregated STAC item using an existing template

    - Extend the item spatiotemporal bbox based on the aggregated dataset
    - Add 'aggregation=True' to the item
    """
    
    ds = xr.open_dataset(endpoint, engine=engine)

    tcs, tce = None, None

    FORMATS = [
        "%Y%m%dT%H%M%SZ",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y%m%d",
        "%Y%m%dT%H%M%S",
        "%Y%m%d%H%M%SZ",
        "%Y%m%d%H%M%S",
    ]

    for fmt in FORMATS:
        try:
            tcs = datetime.strftime(
                datetime.strptime(ds.time_coverage_start, fmt), "%Y-%m-%dT%H:%M:%SZ"
            )
            tce = datetime.strftime(
                datetime.strptime(ds.time_coverage_end, fmt), "%Y-%m-%dT%H:%M:%SZ"
            )
            break
        except Exception as _:
            try:
                tcs = datetime.strftime(
                    datetime.strptime(ds.time[0], fmt), "%Y-%m-%dT%H:%M:%SZ"
                )
                tce = datetime.strftime(
                    datetime.strptime(ds.time[-1], fmt), "%Y-%m-%dT%H:%M:%SZ"
                )
            except Exception as _:
                pass

    if tcs is None:
        try:
            print(ds.time_coverage_end)
            return None, None
        except Exception as _:
            print("No time option at all - skipping")
            return None, None
        
    for lon in ACCEPT_LONS:
        if lon in ds.dims:
            break
    for lat in ACCEPT_LATS:
        if lat in ds.dims:
            break
        
    try:
        bbox_w = float(ds[lon].min())
        bbox_n = float(ds[lat].max())
        bbox_s = float(ds[lat].min())
        bbox_e = float(ds[lon].max())
    except Exception as _:
        bbox_w = -180
        bbox_e = 180
        bbox_n = 90
        bbox_s = -90

    ds.close()

    bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

    if bbox_w == bbox_e and bbox_n == bbox_s:
        geometry = {
            'type': "Point",
            'coordinates': [bbox_w, bbox_s]
        }
    else:
        geometry = {
            'type': "Polygon",
            'coordinates' : [
                [
                    [bbox_w, bbox_s],
                    [bbox_e, bbox_s],
                    [bbox_e, bbox_n],
                    [bbox_w, bbox_n],
                    [bbox_w, bbox_s],
                ]
            ]}

    item['properties']['start_datetime'] = tcs
    item['properties']['end_datetime'] = tce
    item['properties']['file_type'] = engine.title()
    item['properties']['aggregation'] = True
    item['geometry'] = geometry
    item['bbox'] = bbox
    item['id'] = item['id'].replace('NetCDF',engine.title())

    spatiotemporal = {
        'start_datetime': tcs,
        'end_datetime': tce,
        'bbox': [bbox],
    }

    asset_id = 'zarr'
    asset_type = 'application/vnd.zarr'
    if engine == 'kerchunk':
        asset_id = 'reference_file'
        asset_type = 'application/vnd.zarr+kerchunk'

    item['assets'] = {
        asset_id: {
            'href': 'https://dap.ceda.ac.uk' + endpoint,
            'roles': ['data'],
            'type': asset_type
        }
    }

    item['links'] = [link for link in item['links'] if link['rel'] != 'self']

    return item, spatiotemporal


def get_item_template(uuid, drs_id):

    if drs_id is None:
        drs_id = f'{uuid}-main'

    items = requests.get(f'{STAC_API}/collections/{drs_id}/items').json()

    for item in items['features']:
        if item['properties']['file_type'] == 'NetCDF':
            return item
        
    raise ValueError(f'Unable to locate NetCDF template for {STAC_API}/collections/{drs_id}/items')


def get_parent_collection(uuid, drs_id):

    if drs_id is None:
        drs_id = f'{uuid}-main'

    collection = requests.get(f'{STAC_API}/collections/{drs_id}').json()

    return collection


def handle_collection_aggregation(endpoint: str, moles_uuid: str, drs_id: str | None, engine: str = 'kerchunk'):
    """
    Handle updating the parent collection properties and post the item
    """

    template = get_item_template(moles_uuid, drs_id)

    item, spatiotemporal = process_endpoint(endpoint, template, engine=engine)

    parent = get_parent_collection(moles_uuid, drs_id)

    parent['keywords'] = list(set(parent['keywords'] + ['aggregated',engine]))
    extent = confine_components(
        parent['extent'],
        **spatiotemporal)
    
    parent['extent'] = {
        'spatial':{
            'bbox': [extent[2][0]]
        },
        'temporal':{
            'interval': [[extent[0],extent[1]]]
        }
    }

    parent['links'] = remove_duplicate_links(parent['links'])

    print(f'Pushing to: {STAC_API}/collections/{parent["id"]}/items')
    
    # Post Item
    response = client.post(f'{STAC_API}/collections/{parent["id"]}/items', json=item, auth=auth)

    # If the STAC record already exists, just update it
    if response.status_code == 409:
        response = client.put(
            f'{STAC_API}/collections/{parent["id"]}/items/{item["id"]}', 
            json=item, auth=auth)

    # Put Parent Collection
    response = client.put(f'{STAC_API}/collections/{parent["id"]}', json=parent, auth=auth)
    print(response.content)

if __name__ == "__main__":

    fileset = glob.glob(
        "/neodc/esacci/*/metadata/kerchunk/**/*kr*.json", recursive=True
    )
    for x, kfile in enumerate(fileset):
        try:
            process_file(kfile, x, len(fileset))
        except KeyboardInterrupt:
            break
        except KeyError:
            print("Failed to find spatial")
            failed.append((kfile, "spatial_missing"))
        except:
            failed.append((kfile, "general_error"))

    with open("kerchunk_remaining.txt", "w") as f:
        f.write("\n".join([",".join([i for i in f]) for f in failed]))
