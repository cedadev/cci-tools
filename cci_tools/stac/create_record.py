#!/usr/bin/env python
__author__ = "Diane Knappett"
__contact__ = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import requests
import os

from cci_tools.readers.geotiff import read_geotiff
from cci_tools.readers.xarray import scrape_xarray
from cci_tools.core.utils import ALLOWED_OPENSEARCH_EXTS, STAC_API

import logging
from cci_tools.core.utils import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


def extract_id(es_all_dict: dict):
    """
    Extract filename from OpenSearch record
    """
    fname = str(es_all_dict["info"].get("name"))
    file_id, file_ext = os.path.splitext(fname)

    return fname, file_id, file_ext


def extract_collection(es_all_dict: dict):
    """
    Extract collection from OpenSearch record
    """
    ecv = es_all_dict["projects"]["opensearch"].get("ecv")

    if type(ecv) is list:
        if len(ecv) == 1:
            ecv = str(ecv[0]).lower()
        else:
            raise ValueError("Handling of multi-ecv record not supported")

    return ecv


def get_licence(ecv: str):
    """
    Construct URL to the relevant data license on the CEDA artefacts server
    """
    for license in [
        "_terms_and_conditions_v2.pdf",
        "_terms_and_conditions.pdf",
        ".pdf",
    ]:
        r = requests.get(
            f"https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}"
        )
        if r.status_code == 200:
            break
    url = (
        f"https://artefacts.ceda.ac.uk/licences/specific_licences/esacci_{ecv}{license}"
    )

    return url


def extract_opensearch(es_all_dict: dict):
    incomplete = False

    # Extract geospatial bounding box (W, N, E, S)
    try:
        coords = es_all_dict["info"]["spatial"]["coordinates"].get("coordinates")
        bbox_w = coords[0][0]  # west
        bbox_n = coords[0][1]  # north
        bbox_e = coords[1][0]  # east
        bbox_s = coords[1][1]  # south
        bbox = [bbox_w, bbox_s, bbox_e, bbox_n]

        if bbox_w == bbox_e and bbox_n == bbox_s:
            geo_type = "Point"
            coordinates = [bbox_w, bbox_s]
        else:
            geo_type = "Polygon"
            coordinates = [
                [
                    [bbox_w, bbox_s],
                    [bbox_e, bbox_s],
                    [bbox_e, bbox_n],
                    [bbox_w, bbox_n],
                    [bbox_w, bbox_s],
                ]
            ]
    except Exception as err:
        logger.info(f"Exception extracting opensearch geo-information: {err}")
        logger.info(" > Using Global Defaults")
        incomplete = True
        bbox = [-180, -90, 180, 90]
        geo_type = "Polygon"
        coordinates = [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]]

    # Extract specific properties required
    try:
        sdatetime = str(es_all_dict["info"]["temporal"].get("start_time"))
        start_datetime = sdatetime.partition("+")[0] + "Z"
        edatetime = str(es_all_dict["info"]["temporal"].get("end_time"))
        end_datetime = edatetime.partition("+")[0] + "Z"
    except Exception as err:
        logger.info(f"Exception extracting opensearch temporal information: {err}")
        logger.info(" > Using Global Defaults")
        incomplete = True
        start_datetime = "0001-01-01T00:00:00Z"
        end_datetime = "0001-01-01T00:00:00Z"

    try:
        version = es_all_dict["projects"]["opensearch"].get("productVersion")
        platforms = es_all_dict["projects"]["opensearch"].get("platform")
        drs = es_all_dict["projects"]["opensearch"].get("drsId")
    except Exception as err:
        logger.info(f"Exception extracting opensearch facet information: {err}")
        logger.info(" > Using Global Defaults")
        incomplete = True
        version = "Unknown"
        platforms = "Unknown"
        drs = None

    # Extract all other properties
    properties = dict()
    try:
        for property, value in es_all_dict["projects"]["opensearch"].items():
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            properties[property] = value
    except Exception as err:
        logger.info(f"Exception when fetching properties: {err}")

    # Extract format
    format = es_all_dict.get("info", {}).get("format")

    if incomplete:
        properties["incomplete"] = True

    stac_info = {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "version": version,
        "platforms": platforms,
        "drs": drs,
        "bbox": bbox,
        "geo_type": geo_type,
        "coordinates": coordinates,
        "properties": properties,
        "format": format,
    }

    return stac_info, properties


def handle_process_record(
    record: dict,
    output_dir: str,
    exclusion: str = None,
    stac_api: str = STAC_API,
    drs: str = None,
    splitter: list = None,
    start_time: str = None,
    end_time: str = None,
    halt: bool = False,
    **kwargs,
) -> str:

    incomplete = False
    if exclusion in record["_source"]["info"]["name"]:
        logger.info(
            f"Skipping {record['_source']['info']['name']} due to exclusion: {exclusion}"
        )
        return "Excluded"

    try:
        # Process OpenSearch record
        stac_dict, incomplete = process_record(
            record["_source"],
            stac_api,
            drs=drs,
            splitter=splitter,
            start_time=start_time,
            end_time=end_time,
            **kwargs,
        )

        if stac_dict.get("error"):
            return stac_dict["error"]

    except Exception as err:
        if halt:
            raise err
        logger.info(f"Failed to create STAC record: {err}")
        return str(err)

    # Create directory for each CCI ECV/Project
    ecv_dir = stac_dict["collection"]
    cci_stac_dir = f"{output_dir}/{ecv_dir}/"

    if not os.path.isdir(cci_stac_dir):
        try:
            os.mkdir(cci_stac_dir)
            logger.info(f"Created directory '{cci_stac_dir}' successfully")
        except PermissionError as err:
            raise err
        except Exception as err:
            if halt:
                raise err
            logger.error(f"An error occured '{err}'")
            return "Failed:" + str(err)

    # Write 'pretty print' STAC json file
    id = stac_dict["id"]
    stac_file = f"{cci_stac_dir}stac_{id}.json"

    with open(stac_file, "w", encoding="utf-8") as file:
        json.dump(stac_dict, file, ensure_ascii=False, indent=2)

    if incomplete:
        return "Incomplete"

    return "OK"


def process_record(
    es_all_dict: dict,
    drs: str = "",
    splitter: dict = None,
    start_time: str = None,
    end_time: str = None,
    openeo: bool = False,
    fmt_override: str = None,
    collections: list = None,
    interval: str = None,
    **kwargs,
) -> tuple:

    incomplete = False
    # Extract filename, file id, and file extension
    fname, file_id, file_ext = extract_id(es_all_dict)

    # Extract file path
    location = es_all_dict["info"].get("directory")

    # Extract collection (ECV/project)
    ecv = extract_collection(es_all_dict)

    # Construct url to license on the CEDA archive assets server
    url = get_licence(ecv)

    # Extract dataset ID (UUID)
    uuid = es_all_dict["projects"]["opensearch"].get("datasetId")

    fmt_override = fmt_override or ""
    if "xarray" in fmt_override:
        stac_info = scrape_xarray(location, fname, fmt_override, drs, collections)
        properties = stac_info["properties"]

    elif file_ext in ALLOWED_OPENSEARCH_EXTS:
        # Information can only be extracted from OpenSearch record

        stac_info, properties = extract_opensearch(es_all_dict)
        incomplete = stac_info["properties"].get("incomplete", False)

        if stac_info["format"] == None:
            stac_info["format"] = (file_ext[1:]).upper()

        stac_info["format"] = stac_info["format"].replace(" ", "_")

        if not isinstance(stac_info, dict):
            logger.error(
                "OpenSearch record does not contain the required information to create a STAC record."
            )
            return {"error": "InsufficientInformation"}, incomplete

    elif file_ext == ".tif" or file_ext == ".TIF":
        # === GeoTIFF ===
        # Information will be extracted from the OpenSearch record and the GeoTIFF file itself

        stac_info = read_geotiff(
            location + "/" + fname,
            start_time=start_time,
            end_time=end_time,
            fill_incomplete=True,
            openeo=openeo,
            interval=interval,
        )

        incomplete = stac_info["properties"].get("incomplete", False)

        if not isinstance(stac_info, dict):
            logger.error(
                f"GeoTIFF file does not contain the required information to create a STAC record: {location}/{fname}"
            )
            return {"error": "InsufficientInformation"}, incomplete

    else:
        logger.error(f"File format {file_ext} not recognised!")
        return {"error": "FormatUnrecognised"}, False

    exts = [
        "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        "https://stac-extensions.github.io/classification/v1.0.0/schema.json",
    ]

    asset_id = stac_info["format"]
    drs = drs or stac_info.get("drs", None) or f"{uuid}-main"
    stac_id = file_id + f"-{stac_info['format']}"

    if openeo:
        drs = drs + ".openeo"
        stac_id = file_id + ".openeo"
        exts.append("https://stac-extensions.github.io/eo/v1.1.0/schema.json")

        if isinstance(splitter, str):
            asset_id = splitter
        else:
            asset_id = None
            for split, mapping in splitter.items():
                if split in file_id:
                    file_id = file_id.replace(split, mapping[0])
                    asset_id = mapping[1]  # Asset label.
            if asset_id is None:
                logger.warning("No splitting identified for this item")

    remote_location = location
    if "https://" not in remote_location:
        remote_location = "https://dap.ceda.ac.uk" + remote_location

    if uuid is not None:
        properties["opensearch_url"] = (
            f"https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={uuid}",
        )
        properties["esa_url"] = (f"https://climate.esa.int/en/catalogue/{uuid}/",)

    vals = ["start_datetime", "end_datetime", "version", "file_type", "platforms"]

    for v in vals:
        if not properties.get(v, None):
            if v == "file_type":
                properties[v] = stac_info["format"]
            else:
                properties[v] = stac_info[v]

    top_properties = {}
    for v in vals:
        temp_dict = properties.pop(v, None)
        if temp_dict is not None:
            top_properties[v] = temp_dict

    # Create a dictionary of the required STAC output
    stac_dict = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": exts,
        "id": stac_id,
        "collection": drs.lower(),
        "geometry": {
            "type": stac_info["geo_type"],
            "coordinates": stac_info["coordinates"],
        },
        "bbox": stac_info["bbox"],
        "properties": {
            "datetime": None,
            **top_properties,
            "license": "other",
            "aggregation": False,
            "collections": [ecv, uuid, drs],
            **properties,
        },
        "links": [
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"{STAC_API}/collections/{drs}/items/{stac_id}",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{drs}",
            },
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{drs}",
            },
            {"rel": "root", "type": "application/json", "href": STAC_API},
            {"rel": "license", "type": "application/pdf", "href": url},
        ],
        "assets": {asset_id: {"href": f"{remote_location}/{fname}", "roles": ["data"]}},
    }

    # Remove platform until STAC standards have been updated to allow lists of platforms.
    # Until then, the platform list is entered as 'platforms' instead.
    if "platform" in stac_dict["properties"] or file_ext == ".nc":
        stac_dict["properties"].pop("platform")
    if "platformGroup" in stac_dict["properties"] or file_ext == ".nc":
        stac_dict["properties"].pop("platformGroup")

    return stac_dict, incomplete


def combine_records(recordA: dict, recordB: dict) -> dict:
    """
    Merge the assets from record B into record A"""

    for k, v in recordB["assets"].items():
        recordA["assets"][k] = v
    return recordA
