#!/usr/bin/env python
__author__ = "Diane Knappett"
__contact__ = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import httpx
from httpx_auth import OAuth2ClientCredentials
import click
import glob

from cci_tools.core.utils import STAC_API, client, auth
import logging
from cci_tools.core.utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


def post_records(post_directory: str | None, post_records: list | None, openeo: bool = False):

    summaries = {}

    if post_directory is not None:
        for record in glob.glob(f"{post_directory}/**/stac*.json", recursive=True):
            summaries = post_record(record, summaries)
    elif post_records is not None:
        for record in post_records:
            summaries = post_record(record, summaries)

    if not openeo:
        return

    for href, summary in summaries.items():
        parent = client.get(href).json()

        summaries = parent.get("summaries", None)
        if summaries is None:
            summaries = {}
            summary_names = []
        else:
            summary_names = [
                i["name"] for i in summaries.get("eo:bands", {}) if "name" in i
            ]

        repost_summaries = False
        summaries_set = summaries.get("eo:bands", [])
        for name, band in summary.items():
            if name not in summary_names:
                summaries_set.append(band)
                repost_summaries = True

            # Need to be able to update the summaries.

        if parent["summaries"] is None and repost_summaries:
            parent["summaries"] = {"eo:bands": []}

        parent["summaries"]["eo:bands"] = summaries_set
        if repost_summaries:
            logger.info(
                f"Parent: {href.split('/')[-1]}, Updated: {client.put(href, json=parent, auth=auth)}"
            )


def post_record(stac_record, summaries):

    if isinstance(stac_record, str):
        with open(stac_record, "r") as file:
            # Load STAC record
            stac_data = json.load(file)
    
    # Ensure lower-case collections
    stac_data["collection"] = stac_data["collection"].lower()

    # Extract 'drsId' for collection name and 'id' for item name
    dataset_id = stac_data["collection"]
    item_id = stac_data["id"]

    parent_href = f'{STAC_API}/collections/{stac_data["collection"]}'
    if parent_href not in summaries:
        summaries[parent_href] = {}

    for asset in stac_data["assets"].keys():
        if asset not in summaries[parent_href]:
            summaries[parent_href][asset] = {
                "name": asset,
                "common_name": asset,
                "description": "None",
            }

    # Construct paths for STAC collection STAC item
    stac_collection = STAC_API + "/collections/" + dataset_id + "/items"
    stac_item = stac_collection + "/" + item_id

    # Post a new STAC record
    response = client.post(stac_collection, json=stac_data, auth=auth)

    # If the STAC record already exists, just update it
    if response.status_code == 409:
        response = client.put(stac_item, json=stac_data, auth=auth)

    logger.info(f"Item:{item_id} {response}")
    # logger.info('Item:',item_id, response.content)
    return summaries
