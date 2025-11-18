#!/usr/bin/env python
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import click
import xarray as xr
import json

from cci_tools.stac.create_record import process_record
from cci_tools.collection.openeo import openeo_collection
from cci_tools.core.utils import STAC_API, client, auth

# Take a kerchunk/zarr endpoint and build an openeo collection with one item/asset
# Takes the name of the kerchunk file, or a manual input for the name

KNOWN_PROPERTIES = [
    'product_version',
    'project',
    'sensor'
]

def apply_openeo_reqs_for_item(endpoint, did, ecv, moles_uuid, engine):

    # Add parameters
    min_dict_info = {
        'info':{
            'directory': '/'.join(endpoint.split('/')[:-1]),
            'name': endpoint.split('/')[-1],
        },
        'projects':{'opensearch':{
            'ecv':ecv,
            'datasetId':moles_uuid}}
    }

    item_record,_,_ = process_record(
        min_dict_info,
        drs=did,
        splitter='aggregation',
        openeo=True,
        fmt_override=f'xarray|{engine}',
        collections=['cci_openeo',did]
    )

    item_record['properties']['license'] = license
    item_record['properties']['aggregation'] = True

    item_record['properties']['cube:dimensions'] = {
      "lat": {
        "reference_system": "EPSG:4326"
      },
      "lon": {
        "reference_system": "EPSG:4326"
      }
    }
    item_record['properties']['proj:epsg'] = 4326
    item_record['assets']['aggregation']['type'] = 'application/vnd+zarr'

    return item_record
    

@click.command()
@click.argument('endpoint')
@click.option('--did', 'did', required=False)
@click.option('--uuid', 'moles_uuid', required=False)
@click.option('--ecv', 'ecv', required=False)
@click.option('-d','dryrun', is_flag=True, required=False)

def main(endpoint: str, did: str, moles_uuid: str, ecv: str, dryrun: bool = False):

    # Build the item/asset
    # Build the collection (summaries)

    did = did or '.'.join(endpoint.split('/')[-1].split('.')[:-1])
    if '.json' in endpoint:
        engine = 'kerchunk'
    elif '.nca' in endpoint:
        engine = 'CFA'
    elif '.zarr' in endpoint:
        engine = 'zarr'
    else:
        raise ValueError('Aggregation extension is not known')

    item_record = apply_openeo_reqs_for_item(endpoint, did, ecv, moles_uuid, engine)

    ds = xr.open_dataset(endpoint, engine=engine)
    summary_bands = {}
    alt_shape = 0
    for v in ds.variables:
        if len(ds.variables[v].shape) == 1:
            pass
        else:
            if alt_shape == 0:
                alt_shape = len(ds.variables[v].shape)

            if len(ds.variables[v].shape) != alt_shape:
                # Not applicable for openeo
                raise ValueError(
                    f'Non-conforming dimensions for openeo with {v}: {alt_shape}, {len(ds.variables[v].shape)}'
                )
            
            summary_bands[v] = {
                'long_name': ds.variables[v].attrs['long_name'],
                'description': ds.variables[v].attrs['long_name'],
            }

    for prop in KNOWN_PROPERTIES:
        if prop in ds.attrs:
            item_record['properties'][prop] = ds.attrs.get(prop)

    license='other' # xarray license not valid stac

    collection_record = openeo_collection(
        did.lower() + '.openeo', 
        ds.attrs['summary'],
        [item_record['bbox']],
        item_record['properties']['start_datetime'],
        item_record['properties']['end_datetime'],
        ds.title,
        moles_uuid=moles_uuid,
        keywords=did.split('-') + [k.strip() for k in ds.keywords.split('>')],
        summary_bands=summary_bands,
        license=license
    )

    item_record['properties']['license'] = license
    item_record['properties']['aggregation'] = True

    item_record['properties']['cube:dimensions'] = {
      "lat": {
        "reference_system": "EPSG:4326"
      },
      "lon": {
        "reference_system": "EPSG:4326"
      }
    }
    item_record['properties']['proj:epsg'] = 4326
    item_record['assets']['aggregation']['type'] = 'application/vnd+zarr'

    if dryrun:
        with open('item.json','w') as f:
            f.write(json.dumps(item_record))
        print('> Output to file: item')

        with open('collection.json','w') as f:
            f.write(json.dumps(collection_record))
        print('> Output to file: collection')

    else:
        print('collection:',collection_record["id"])
        # Post the collection, then the item
        resp = client.post(f'{STAC_API}/collections', json=collection_record, auth=auth)
        if str(resp.status_code) == '409':
            resp = client.put(f'{STAC_API}/collections/{collection_record["id"]}', json=collection_record, auth=auth)
        print(resp)

        resp = client.post(f'{STAC_API}/collections/{did.lower()}.openeo/items', json=item_record, auth=auth)
        if str(resp.status_code) == '409':
            resp = client.put(f'{STAC_API}/collections/{did.lower()}.openeo/items/{item_record["id"]}', json=item_record, auth=auth)
        print(resp)

if __name__ == '__main__':
    main()