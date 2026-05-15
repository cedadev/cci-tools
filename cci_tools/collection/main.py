__author__ = "Daniel Westwood"
__contact__ = "daniel.westwood@stmoles_stac.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import copy
import requests
from cci_tools.core.utils import client, auth, STAC_API, COLLECTION_TEMPLATE, logstream

from cci_tools.elasticsearch import (
    uuids_per_project,
    es_collection,
)

from xml.dom import minidom

import logging

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


def get_project_kwargs():
    """
    Get kwargs for project construction."""

    accepted = False
    while not accepted:
        logger.info("Start of project temporal range: ")
        temporal_start = input("(format: YYYY-MM-DDTHH:MM:SSZ) : ")

        logger.info("End of project temporal range: ")
        temporal_end = input("(format: YYYY-MM-DDTHH:MM:SSZ) : ")

        temporal = [temporal_start, temporal_end]

        description = input("Project-level Abstract: ")

        logger.info(f"Temporal: {temporal}")
        logger.info(f"Description: {description}")

        if input("Accept these values? (Y/N) ") == "Y":
            accepted = True

    return {"temporal": temporal, "abstract": description}


def get_project_labels_from_opensearch():

    content = minidom.parseString(
        requests.get(
            "https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier=cci"
        ).content
    )

    logger.info("Getting project labels from CEDA Catalogue OpenSearch description.xml")
    project_values, ecv_values = [], []
    for param in content.getElementsByTagName("param:Parameter"):
        if param.getAttribute("name") == "project":
            options = param.getElementsByTagName("param:Option")
            project_values += [
                o.getAttribute("value").lower().replace(" ", "_") for o in options
            ]
        if param.getAttribute("name") == "ecv":
            options = param.getElementsByTagName("param:Option")
            ecv_values += [
                o.getAttribute("value").lower().replace(" ", "_") for o in options
            ]

    logger.info(f"Checking existing collections for project labels: {project_values}")
    exists = []
    for project in list(set(project_values)):
        if (
            requests.get(
                f"{STAC_API}/collections/" + project
            ).status_code
            == 200
        ):
            exists.append(project)
    for ecv in list(set(ecv_values)):
        if (
            requests.get(
                f"{STAC_API}/collections/" + ecv
            ).status_code
            == 200
        ):
            exists.append(ecv)

    return sorted(list(set(exists)))


def set_field(default_or_existing_value, new_value, exists: bool = False):
    if exists and default_or_existing_value:
        # If exists and has a value already
        return default_or_existing_value
    else:
        # If exists and does not have a value, or does not exist
        return new_value


def remove_duplicate_links(old_links: list, allow_capitals: bool = True):
    """
    Remove duplicated aggregation/queryable links that are added repeatedly by the STAC API.

    Will also remove duplicate self/root/parent links if present.
    """
    remove_duplicates = {
        "items": False,
        "parent": False,
        "root": False,
        "self": False,
        "aggregate": True,
        "aggregations": True,
        "queryables": True,
    }

    logger.info('Removing duplicate links.')
    new_links = []
    children = []
    for link in old_links:

        remove = remove_duplicates.get(link["rel"], False)
        if not remove:

            if link["rel"] == "child":
                if link["href"] not in children and (
                    allow_capitals or link["href"] == link["href"].lower()
                ):
                    children.append(link["href"])
                    new_links.append(link)
                continue

            new_links.append(link)
            if link["rel"] in remove_duplicates:
                remove_duplicates[link["rel"]] = True

    return new_links


def add_drs_collection(
    parent: dict,
    drs_reference: dict,
    overwrite: bool = False,
    dryrun: bool = False,
    uuid: str = None,
) -> tuple:
    """
    Add a DRS collection to a parent MOLES UUID-based collection

    ODP Aggregation - CEDA DRS

    parent - id, title, keywords, links
    """
    id = drs_reference["id"]

    exists = False
    current = client.get(f"{STAC_API}/collections/{id.lower()}")
    if current.status_code == 200:
        if not overwrite:
            if dryrun:
                logger.info(
                    f' > > DRS Collection "{id}" no changes detected (use --overwrite to update)'
                )
            return parent, False

        exists = True
        drs_stac = current.json()
    else:
        logger.info(f' > > NEW DRS Collection: "{id}"')
        drs_stac = copy.deepcopy(COLLECTION_TEMPLATE)

    # Get extent from parent or opensearch
    extent = copy.deepcopy(parent["extent"])

    if id == "":
        id = parent["id"] + "-main"  # -main of parent
        title = "(NonDRS) " + parent["title"]
    else:
        title = id

    drs_stac["id"] = set_field(drs_stac.get("id"), id.lower(), exists=exists)
    drs_stac["description"] = set_field(
        drs_stac.get("description"), drs_reference["description_url"], exists=exists
    )
    drs_stac["title"] = set_field(drs_stac.get("title"), title, exists=exists)

    drs_stac["extent"] = set_field(drs_stac.get("extent"), extent, exists=exists)

    drs_stac["keywords"] = list(
        set(drs_stac.get("keywords", []) + parent.get("keywords", []) + id.split("."))
    )

    drs_stac["links"].append(
        {
            "rel": "ceda_catalogue",
            "type": "text/html",
            "href": f"https://catalogue.ceda.ac.uk/uuid/{uuid or parent['id']}",
        },
    )

    drs_stac["links"] = remove_duplicate_links(drs_stac["links"])

    parent["links"].append(
        {
            "rel": "child",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id.lower()}",
        }
    )
    drs_stac["providers"] = [
        {
            "roles": ["host"],
            "name": "Centre for Environmental Data Analysis (CEDA)",
            "url": "https://catalogue.ceda.ac.uk",
        },
        {
            "roles": ["host"],
            "name": "ESA Open Data Poral (ODP)",
            "url": "https://climate.esa.int/data",
        },
    ]

    if dryrun:
        with open(f"stac_collections/gen/{id}.json", "w") as f:
            f.write(json.dumps(drs_stac))
        logger.info(f"{id}: Local")
    else:
        if exists:
            response = "Skipped"
            if overwrite:
                response = client.put(
                    f"{STAC_API}/collections/{id.lower()}",
                    json=drs_stac,
                    auth=auth,
                )
        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=drs_stac,
                auth=auth,
            )

        if isinstance(response, str):
            pass
        elif response.status_code not in [200, 201]:
            raise ValueError(response.content)

        logger.info(f" > > {id}: {response}")

    return parent, not exists


def get_drs_set_for_uuid(collection_id: str, drs_ids: list):
    """
    Get the set of DRS IDs associated with a given UUID, by querying the CEDA Catalogue API.
    """
    drs_list = []
    for drs in drs_ids:

        description_url = f"https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={collection_id}"
        if drs != "":
            description_url += f"&drsId={drs}"
        drs_list.append({"id": drs, "description_url": description_url})
    return drs_list


def add_uuid_collection(
    project_coll: dict,
    uuid: str,
    overwrite: bool = False,
    dryrun: bool = False,
    api_key: str = None,
) -> dict:
    """
    ODP Subcollection/Feature - CEDA Moles Identfier
    """

    es_coll_data = es_collection(uuid, api_key=api_key)
    if es_coll_data is None:
        logger.info(' > MOLES Collection "{uuid}" skipped - version superseded')
    collection_id = es_coll_data["collection_id"]

    exists = False
    current = client.get(f"{STAC_API}/collections/{collection_id}")
    if current.status_code == 200:
        exists = True
        moles_stac = current.json()
    else:
        moles_stac = copy.deepcopy(COLLECTION_TEMPLATE)

    moles_parent = {
        "id": set_field(moles_stac.get("id"), collection_id, exists=exists),
        "links": [],
        "title": set_field(
            moles_stac.get("title"), es_coll_data.get("title"), exists=exists
        ),
        "extent": moles_stac.get("extent"),
    }

    new_drss = False
    for drs_ref in get_drs_set_for_uuid(
        collection_id, drs_ids=es_coll_data.get("drsId", [])
    ):
        moles_parent, added = add_drs_collection(
            moles_parent, drs_ref, overwrite=overwrite, dryrun=dryrun, uuid=uuid
        )

        new_drss = new_drss or added

    if exists:
        if not overwrite and not new_drss:
            if dryrun:
                logger.info(
                    f' > MOLES Collection "{collection_id}" no changes detected (use --overwrite to update)'
                )
            return project_coll, False
    else:
        logger.info(f' > NEW MOLES Collection: "{collection_id}"')

    moles_stac.update(moles_parent)

    moles_info = requests.get(
        f"https://catalogue.ceda.ac.uk/api/v3/observations/?discoveryKeywords__name=ESACCI&uuid={collection_id}"
    ).json()

    abstract = moles_info["results"][0]["abstract"]

    moles_stac["description"] = set_field(
        moles_stac.get("description"),
        abstract
        + "\n\n"
        + f'https://catalogue.ceda.ac.uk/uuid/{es_coll_data["collection_id"]}',
        exists=exists,
    )

    start = (es_coll_data.get("start_date", None) or "1970-01-01").split("T")[0]
    end = (es_coll_data.get("end_date", None) or "2025-12-31").split("T")[0]

    temporal_coverage = [
        f"{start}T00:00:00Z",
        f"{end}T23:59:59Z",
    ]

    # Only use the new value if there's no current extent.
    # Prevents resetting values for extent that have been set outside this mechanism.
    moles_stac["extent"] = set_field(
        moles_stac.get("extent"),
        {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [temporal_coverage]},
        },
        exists=exists,
    )

    moles_stac["links"].append(
        {
            "rel": "items",
            "type": "application/geo+json",
            "href": f"{STAC_API}/collections/{collection_id}/items",
        }
    )
    moles_stac["links"].append(
        {
            "rel": "parent",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{project_coll.get('id')}",
        }
    )
    moles_stac["links"].append(
        {
            "rel": "self",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{collection_id}",
        }
    )

    moles_stac["keywords"] = list(
        set(
            project_coll.get("keywords", []) + moles_stac.get("keywords", [])
            or [] + collection_id.split(".")
        )
    )

    moles_stac["links"] = remove_duplicate_links(
        moles_stac["links"], allow_capitals=False
    )

    project_coll["links"].append(
        {
            "rel": "child",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{collection_id}",
        }
    )

    if dryrun:
        with open(f"stac_collections/gen/{collection_id}.json", "w") as f:
            f.write(json.dumps(moles_stac))
        logger.info(f"{collection_id}: Local")

    else:
        if exists:
            response = "Skipped"
            if overwrite:
                response = client.put(
                    f"{STAC_API}/collections/{id}",
                    json=moles_stac,
                    auth=auth,
                )
        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=moles_stac,
                auth=auth,
            )
        logger.info(f" > {id}: {response}")
        if response.status_code == 400:
            logger.info(moles_stac["extent"])
            logger.info(response.content)

    return project_coll, not exists


def create_project_collection(
    project: str,
    parent: dict,
    dataset_collection: str = None,
    overwrite: bool = False,
    api_key: str = None,
    dryrun: bool = False,
):
    """
    ODP/CEDA Project
    """

    # Method to get the following project-level information
    # - project id
    # - project description
    # - project temporal coverage

    # Get description and temporal coverage from moles?

    id = project.replace("-", "_")

    child_coll = {"id": id, "links": []}

    new_uuids = False
    # Find all moles UUIDs (from opensearch-collections) for this project type.
    for uuid in uuids_per_project(project, api_key=api_key):
        if uuid == "cci":
            continue
        child_coll, add_uuid = add_uuid_collection(
            child_coll, uuid, overwrite=overwrite, api_key=api_key, dryrun=dryrun
        )
        new_uuids = new_uuids or add_uuid

    exists = False
    current = client.get(f"{STAC_API}/collections/{id}")
    if current.status_code == 200:
        if not overwrite and not new_uuids:
            if dryrun:
                logger.info(
                    f'Project collection "{id}" no changes detected (use --overwrite to update)'
                )
            return parent, False

        exists = True
        project_coll = current.json()
    else:
        logger.info(f'NEW Project Collection: "{id}"')
        project_coll = copy.deepcopy(COLLECTION_TEMPLATE)

    # Dataset Collections
    if dataset_collection:
        moles_reference = requests.get(
            f"https://catalogue.ceda.ac.uk/api/v3/observationcollections/?uuid={dataset_collection}"
        ).json()["results"][0]
    elif exists:
        moles_reference = {}
        pass
    else:
        moles_reference = get_project_kwargs()

    project_coll["links"] = project_coll.get("links", []) + child_coll.get("links", [])

    project_coll["id"] = set_field(project_coll.get("id"), id, exists=exists)
    project_coll["title"] = set_field(project_coll.get("title"), project, exists=exists)

    project_coll["description"] = set_field(
        project_coll.get("description"), moles_reference.get("abstract"), exists=exists
    )

    # Temporal coverage of a whole ecv?
    temporal_coverage = set_field(
        project_coll.get("extent", {}).get("temporal_coverage"),
        moles_reference.get("temporal"),
        exists=exists,
    )

    ## Other metadata (summaries, keywords)
    project_coll["summaries"] = set_field(
        project_coll.get("summaries"), {"project": [project]}, exists=exists
    )

    project_coll["keywords"] = list(
        set(
            ["ESACCI", project, id]
            + id.split(".")
            + moles_reference.get("keywords", [])
        )
    )

    project_coll["links"].append(
        {
            "rel": "parent",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{parent['id']}",
        }
    )
    project_coll["links"].append(
        {
            "rel": "self",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}",
        }
    )
    project_coll["links"].append(
        {"rel": "root", "type": "application/json", "href": f"{STAC_API}"}
    )

    project_coll["links"] = remove_duplicate_links(project_coll["links"])

    parent["links"].append(
        {
            "rel": "child",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}",
        }
    )

    project_coll["extent"] = project_coll.get("extent", None) or {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [temporal_coverage]},
    }

    if dryrun:
        with open(f"stac_collections/gen/{id}.json", "w") as f:
            f.write(json.dumps(project_coll))
        logger.info(f"{id}: Local")
    else:
        if exists:
            response = "Skipped"
            if overwrite:
                response = client.put(
                    f"{STAC_API}/collections/{id}",
                    json=project_coll,
                    auth=auth,
                )

        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=project_coll,
                auth=auth,
            )
        logger.info(f"{id}: {response}")

    return parent, not exists
