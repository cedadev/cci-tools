#!/usr/bin/env python
__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import click
import xarray as xr
import json
from typing import Union

from cci_tools.stac.create_record import process_record
from cci_tools.collection.openeo import openeo_collection
from cci_tools.core.utils import STAC_API, client, auth
import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

# Take a kerchunk/zarr endpoint and build an openeo collection with one item/asset
# Takes the name of the kerchunk file, or a manual input for the name

KNOWN_PROPERTIES = ["product_version", "project", "sensor"]


def apply_openeo_reqs_for_item(
    endpoint, did, ecv, moles_uuid, engine, license: Union[str, None] = None
):

    # Add parameters
    min_dict_info = {
        "info": {
            "directory": "/".join(endpoint.split("/")[:-1]),
            "name": endpoint.split("/")[-1],
        },
        "projects": {"opensearch": {"ecv": ecv, "datasetId": moles_uuid}},
    }

    item_record, _, _ = process_record(
        min_dict_info,
        drs=did,
        splitter="aggregation",
        openeo=True,
        fmt_override=f"xarray|{engine}",
        collections=["cci_openeo", did],
    )

    item_record["properties"]["license"] = license
    item_record["properties"]["aggregation"] = True

    item_record["properties"]["cube:dimensions"] = {
        "lat": {"reference_system": "EPSG:4326"},
        "lon": {"reference_system": "EPSG:4326"},
    }

    item_record["properties"]["proj:epsg"] = 4326
    item_record["assets"]["aggregation"]["type"] = "application/vnd+zarr"
    item_record["assets"]["aggregation"]["xarray:open_kwargs"] = {
        "engine": engine,
        "chunks": {},
    }

    return item_record


@click.command()
@click.argument("endpoint")
@click.option("--did", "did", required=False)
@click.option("--uuid", "moles_uuid", required=False)
@click.option("--ecv", "ecv", required=False)
@click.option("-d", "dryrun", is_flag=True, required=False)
@click.option("-v", "--verbose", count=True)
def main(
    endpoint: str,
    did: str,
    moles_uuid: str,
    ecv: str,
    dryrun: bool = False,
    verbose: int = 0,
):
    """
    Create OpenEO collection/item records for an existing kerchunk/zarr endpoint.
    """

    set_verbose(verbose)

    # Build the item/asset
    # Build the collection (summaries)

    did = did or ".".join(endpoint.split("/")[-1].split(".")[:-1])
    if ".json" in endpoint:
        engine = "kerchunk"
    elif ".nca" in endpoint:
        engine = "CFA"
    elif ".zarr" in endpoint:
        engine = "zarr"
    else:
        raise ValueError("Aggregation extension is not known")

    license = "other"  # xarray license not valid stac

    item_record = apply_openeo_reqs_for_item(
        endpoint, did, ecv, moles_uuid, engine, license=license
    )

    ds = xr.open_dataset(endpoint, engine=engine)
    summary_bands = {}
    alt_shape = 0
    cube_variables = []
    for v in ds.variables:
        if len(ds.variables[v].shape) == 1:
            pass
        else:
            if alt_shape == 0:
                alt_shape = len(ds.variables[v].shape)

            if not (len(ds.variables[v].shape) != alt_shape or "bnds" in v):
                summary_bands[v] = {
                    "long_name": ds.variables[v].attrs.get("long_name", v),
                    "description": ds.variables[v].attrs.get("long_name", v),
                }
                cube_variables.append(v)

    if len(cube_variables) == 0:
        raise ValueError("No variables available for OpenEO dataset")
    logger.info(f"Available data variables: {cube_variables}")
    item_record["properties"]["cube:variables"] = {v: {} for v in cube_variables}

    for prop in KNOWN_PROPERTIES:
        if prop in ds.attrs:
            item_record["properties"][prop] = ds.attrs.get(prop)

    keywords = []
    if hasattr(ds, "keywords"):
        keywords = [k.strip() for k in ds.keywords.split(">")]

    collection_record = openeo_collection(
        did.lower() + ".openeo",
        ds.attrs.get("summary", None),
        [item_record["bbox"]],
        item_record["properties"]["start_datetime"],
        item_record["properties"]["end_datetime"],
        ds.title,
        moles_uuid=moles_uuid,
        keywords=did.split("-") + keywords,
        summary_bands=summary_bands,
        license=license,
    )

    if dryrun:
        try:
            with open("item.json", "w") as f:
                f.write(json.dumps(dict(item_record)))
        except TypeError as e:
            raise e
            logger.error("Error: Unserializable")
        logger.info("> Output to file: item")

        with open("collection.json", "w") as f:
            f.write(json.dumps(collection_record))
        logger.info("> Output to file: collection")

    else:
        logger.info(f"collection: {collection_record['id']}")
        # Post the collection, then the item
        resp = client.post(f"{STAC_API}/collections", json=collection_record, auth=auth)
        if str(resp.status_code) == "409":
            resp = client.put(
                f'{STAC_API}/collections/{collection_record["id"]}',
                json=collection_record,
                auth=auth,
            )
        logger.info(f"Collection response: {resp}")

        resp = client.post(
            f"{STAC_API}/collections/{did.lower()}.openeo/items",
            json=item_record,
            auth=auth,
        )
        if str(resp.status_code) == "409":
            resp = client.put(
                f'{STAC_API}/collections/{did.lower()}.openeo/items/{item_record["id"]}',
                json=item_record,
                auth=auth,
            )
        logger.info(f"Item response: {resp}")


if __name__ == "__main__":
    main()
