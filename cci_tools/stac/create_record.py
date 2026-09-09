#!/usr/bin/env python
__author__ = "Diane Knappett"
__contact__ = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import requests
import os

from cci_tools.readers.geotiff import read_geotiff
from cci_tools.readers.xarray import scrape_xarray
from cci_tools.stac.post_record import post_record
from cci_tools.core.utils import ALLOWED_OPENSEARCH_EXTS, STAC_API, \
    es_client, get_file_query, get_moles_data, \
    get_licence, get_providers, order_properties

ACCEPTABLE_RESPONSES = ["OK", "Excluded"]

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


def single_opensearch_record(
        file,
        output_dir,
        **kwargs) -> bool:
    
    body = get_file_query(file)
    hits = es_client.search(index="opensearch-files", body=body)["hits"][
        "hits"
    ]

    if len(hits) == 0:
        return f"{file}: Not found in Opensearch"
        

    record = hits[0]
    response = handle_process_record(
        record,
        output_dir,
        **kwargs,
    )

    if response not in ACCEPTABLE_RESPONSES:
        return f"{file}:{response}"
    else:
        return


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
            drs,
            stac_api=stac_api,
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
    id = stac_dict["id"]
    if output_dir != 'UPLOAD': # UPLOAD directly to the STAC API
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
    
    else:
        _ = post_record(stac_dict, {})

    if incomplete:
        return "Incomplete"

    return "OK"


def process_record(
    es_all_dict: dict,
    drs: str | None,
    stac_api: str = STAC_API,
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
    remote_location = location
    if "https://" not in remote_location:
        remote_location = "https://dap.ceda.ac.uk" + remote_location

    # Extract collection (ECV/project)
    ecv = extract_collection(es_all_dict)

    # Construct url to licence on the CEDA archive assets server
    licence_url = get_licence(ecv)

    # Extract dataset ID (UUID)
    uuid = es_all_dict["projects"]["opensearch"].get("datasetId")

    # Now using the processing schema only - may also add OpenEO as needed
    exts = [
        "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
    ]

    ## File Metadata Extraction

    fmt_override = fmt_override or ""
    if "xarray" in fmt_override:
        stac_info = scrape_xarray(location, fname, fmt_override, drs, collections)
        properties = stac_info["properties"]

        # Xarray scraping includes projection information
        exts.append("https://stac-extensions.github.io/projection/v1.1.0/schema.json")

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

        # Geotiff scraping includes projection information
        exts.append("https://stac-extensions.github.io/projection/v1.1.0/schema.json")

        incomplete = stac_info["properties"].get("incomplete", False)

        if not isinstance(stac_info, dict):
            logger.error(
                f"GeoTIFF file does not contain the required information to create a STAC record: {location}/{fname}"
            )
            return {"error": "InsufficientInformation"}, incomplete

    else:
        logger.error(f"File format {file_ext} not recognised!")
        return {"error": "FormatUnrecognised"}, False

    ## Asset/ID Extraction
    asset_id = stac_info["format"]
    drs      = drs or stac_info.get("drs", None) or f"{uuid}-main"
    stac_id  = file_id + f"-{stac_info['format']}"

    ## Handling OpenEO Differences
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

    ## Core Properties Extraction
    vals = ["start_datetime", "end_datetime", "file_type"]
    core_properties = {}
    for v in vals:
        if not properties.get(v, None):
            if v == "file_type":
                core_properties[v] = stac_info["format"]
            else:
                core_properties[v] = stac_info[v]

    ## Version Extraction
    version = stac_info.get('version',None)
    numericVersion = version
    if version.startswith('v'):
        numericVersion = version[1:]

    ## Handling Different Properties
    cci_properties = {
        'cci:project':     es_all_dict["projects"]["opensearch"].get("project",[None])[0], # Always take the string value for this
        'cci:collections': [ecv, uuid, drs],
        'cci:drsId': drs,
        'cci:ecv': ecv,
        'cci:esa_url':     f"https://climate.esa.int/en/catalogue/{uuid}/",
        'cci:dataType':    es_all_dict["projects"]["opensearch"].get('dataType',None),
        'cci:sensor':      es_all_dict["projects"]["opensearch"].get('sensor',None),
        'cci:platform':    stac_info.get('platforms',None),
        'cci:frequency':   es_all_dict["projects"]["opensearch"].get("frequency",None),
        'cci:product':     es_all_dict["projects"]["opensearch"].get("product",None),
        'cci:productVersion': version,
        'cci:institute':   es_all_dict["projects"]["opensearch"].get('institute',None)
    }
    ceda_properties = {
        'ceda:uuid': uuid,
        'ceda:aggregation': False,
        'ceda:opensearch_url': f"https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={uuid}"
    }
    processing_properties = {
        'processing:version': numericVersion, # minus 'v'
        'processing:level': es_all_dict["projects"]["opensearch"].get('processingLevel')
    }

    moles_data = get_moles_data(uuid)

    all_properties = {
        "datetime": None,
        "title": moles_data['title'],
        "description": moles_data['abstract'] + f'\r\n\n\n See CEDA Catalogue Record for citation details: https://catalogue.ceda.ac.uk/uuid/{uuid}'
        **core_properties,
        "licence": "other", # "CC-BY-4.0" not allowed
        **cci_properties,
        **ceda_properties,
        **processing_properties,
        'providers': get_providers(es_all_dict["projects"]["opensearch"].get('institute')),
        **properties, # Any other properties from alternative sources
    }

    ## Create STAC Dictionary
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
        "properties": order_properties(all_properties),
        "links": [
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"{stac_api}/collections/{drs}/items/{stac_id}",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{stac_api}/collections/{drs}",
            },
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"{stac_api}/collections/{drs}",
            },
            {
                "rel": "via",
                "type": "text/html",
                "href": f"https://catalogue.ceda.ac.uk/uuid/{uuid}"
            },
            {"rel": "root", "type": "application/json", "href": stac_api},
            {"rel": "licence", "type": "application/pdf", "href": licence_url}
        ],
        "assets": {"asset_id": {"href": f"{remote_location}/{fname}", "roles": ["data"]}},
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
