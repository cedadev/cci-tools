__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stmoles_stac.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import copy
import requests
from cci_tools.core.utils import (
    client, 
    auth, 
    STAC_API,
    COLLECTION_TEMPLATE,
    get_opensearch_record,
    uuids_per_project,
    es_collection
)

def get_project_labels_from_vocabs():

    project_labels = []
    ontology = requests.get('https://vocab.ceda.ac.uk/ontology/cci/cci-content/cci-ontology.json').json()
    for ont in ontology:
        
        if 'cci/project' in ont["@id"]:
            project_labels.append(ont["http://www.w3.org/2004/02/skos/core#prefLabel"][0]["@value"].replace(' ','_'))

    return project_labels

def remove_duplicate_links(old_links: list, allow_capitals: bool = True):
    """
    Remove duplicated aggregation/queryable links that are added repeatedly by the STAC API.
    
    Will also remove duplicate self/root/parent links if present.
    """
    remove_duplicates = {'items':False, 'parent':False, 'root':False, 'self':False, 'aggregate':True, 'aggregations': True, 'queryables': True}

    new_links = []
    children = []
    for link in old_links:

        remove = remove_duplicates.get(link['rel'],False)
        if not remove:

            if link['rel'] == 'child':
                if link['href'] not in children and (allow_capitals or link['href'] == link['href'].lower()):
                    children.append(link['href'])
                    new_links.append(link)
                continue

            new_links.append(link)
            if link['rel'] in remove_duplicates:
                remove_duplicates[link['rel']] = True

    return new_links

def add_drs_collection(
        parent: dict,
        drs_reference: dict,  
        overwrite: bool = False, 
        dryrun: bool = False,
        uuid: str = None) -> dict:
    """
    Add a DRS collection to a parent MOLES UUID-based collection

    ODP Aggregation - CEDA DRS

    parent - id, title, keywords, links
    """
    id = drs_reference['id']

    exists = False
    current = client.get(f"{STAC_API}/collections/{id.lower()}")
    if current.status_code == 200:
        if not overwrite:
            print(f'DRS collection {id} already exists, skipping (use --overwrite to update)')
            return parent
        exists = True
        drs_stac = current.json()
    else:
        drs_stac = copy.deepcopy(COLLECTION_TEMPLATE)
    

    extent = copy.deepcopy(parent['extent'])
    if drs_reference['id']:
        opensearch_record = get_opensearch_record(
            parent['id'], 
            drs_reference['id']
        )
        
        if opensearch_record is None:
            return parent
        
        dates = []
        for i in opensearch_record['features']:
            if 'date' not in i['properties']:
                continue
            dates += i['properties'].get('date','').split('/')

        if len(dates) == 0:
            dates = [
                '1970-01-01T00:00:00Z',
                '2025-09-17T00:00:00Z'
            ]

        dates = sorted(dates)
        for d in range(len(dates)):
            dates[d] = dates[d].split('+')[0]

            if dates[d][-1] != 'Z':
                dates[d] += 'Z'

        extent['temporal']['interval'][0] = [dates[0], dates[-1]]
    
    if id == "":
        id = parent['id'] + '-main' # -main of parent
        title = '(NonDRS) ' + parent['title']
    else:
        title = id

    drs_stac['id'] = id.lower()
    drs_stac['description'] = drs_reference['description_url']
    drs_stac['title'] = title

    drs_stac['extent'] = extent

    drs_stac['keywords'] = drs_stac['keywords'] or []
    drs_stac['keywords'] = list(
        set(
            drs_stac.get('keywords',[]) + \
            parent.get('keywords',[]) + \
            id.split('.')
        )
    )

    drs_stac['links'].append({
      "rel": "ceda_catalogue",
      "type": "text/html",
      "href": f"https://catalogue.ceda.ac.uk/uuid/{uuid or parent['id']}"
    },)

    drs_stac['links'] = remove_duplicate_links(drs_stac['links'])

    parent['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id.lower()}"
    })
    drs_stac['providers'] = [
        {
            'roles': ["host"],
            'name': 'Centre for Environmental Data Analysis (CEDA)',
            'url': 'https://catalogue.ceda.ac.uk'
        },
        {
            'roles':["host"],
            'name': "ESA Open Data Poral (ODP)",
            'url': 'https://climate.esa.int/data'
        }
    ]

    if dryrun:
        with open(f'stac_collections/gen/{id}.json','w') as f:
            f.write(json.dumps(drs_stac))
        print(f'{id}: Local')
    else:
        if exists:
            response = 'Skipped'
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
        elif response.status_code not in [200,201]:
            raise ValueError(response.content)

        print(f' > > {id}: {response}')

    return parent

def get_drs_set_for_uuid(collection_id: str, drs_ids: list):
    """
    Get the set of DRS IDs associated with a given UUID, by querying the CEDA Catalogue API.
    """
    drs_list = []
    for drs in drs_ids:

        description_url = f'https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={collection_id}'
        if drs != "":
            description_url += f'&drsId={drs}'
        drs_list.append({
            'id': drs,
            'description_url': description_url
        })
    return drs_list

def add_uuid_collection(
        project_coll: dict, 
        uuid: str, 
        overwrite: bool = False, 
        dryrun: bool = False,
        api_key: str = None
    ) -> dict:
    """
    ODP Subcollection/Feature - CEDA Moles Identfier
    """

    es_coll_data  = es_collection(uuid, api_key=api_key)
    collection_id = es_coll_data['collection_id']

    for drs_ref in get_drs_set_for_uuid(collection_id, drs_ids=es_coll_data.get('drs_ids',[])):
        moles_stac = add_drs_collection(drs_ref, moles_stac, 
            overwrite=overwrite,
            uuid=uuid)

    exists = False
    current = client.get(f"{STAC_API}/collections/{id}")
    if current.status_code == 200:
        if not overwrite:
            print(f'MOLES collection {collection_id} already exists, skipping (use --overwrite to update)')
            return project_coll
        exists = True
        moles_stac = current.json()
    else:
        moles_stac = copy.deepcopy(COLLECTION_TEMPLATE)

    abstract = requests.get(
        f'https://catalogue.ceda.ac.uk/api/v2/observations.json?discoveryKeywords__name=ESACCI&uuid={collection_id}').json()['results'][0]['abstract']

    moles_stac['id'] = collection_id
    moles_stac['title'] = es_coll_data['title']
    moles_stac['description'] = abstract + '\n\n' + f'https://catalogue.ceda.ac.uk/uuid/{es_coll_data["collection_id"]}'
    
    moles_stac['keywords'] = project_coll['keywords']

    start = (es_coll_data.get('start_date',None) or '1970-01-01').split('T')[0]
    end = (es_coll_data.get('end_date',None) or '2025-12-31').split('T')[0]

    temporal_coverage = [
        f"{start}T00:00:00Z",
        f"{end}T23:59:59Z",
    ]

    moles_stac['extent'] = {
        "spatial": {
            "bbox": [
                [
                    -180,
                    -90,
                    180,
                    90
                ]
            ]
            },
        "temporal": {
            "interval": [
                temporal_coverage
            ]
        }
    }

    moles_stac['links'].append({
        "rel": "items",
        "type": "application/geo+json",
        "href": f"{STAC_API}/collections/{id}/items"
    })
    moles_stac['links'].append({
        "rel": "parent",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{uuid}"
    })
    moles_stac['links'].append({
        "rel": "self",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    moles_stac['keywords'] = list(
        set(
            project_coll.get('keywords',[]) + \
            moles_stac.get('keywords',[]) + \
            id.split('.')
        )
    )

    moles_stac['links'] = remove_duplicate_links(moles_stac['links'], allow_capitals=False)

    project_coll['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    if dryrun:
        with open(f'stac_collections/gen/{id}.json','w') as f:
            f.write(json.dumps(project_coll))
        print(f'{id}: Local')

    else:
        if exists:
            response = 'Skipped'
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
        print(f' > {id}: {response}')
        if response.status_code == 400:
            print(moles_stac['extent'])
            print(response.content)

    return project_coll

def create_project_collection(
        project: str, 
        parent: dict, 
        dataset_collection: str,
        overwrite: bool = False,
        api_key: str = None,
        dryrun: bool = False):
    """
    ODP/CEDA Project
    """

    # Method to get the following project-level information
    # - project id
    # - project description
    # - project temporal coverage

    # Get description and temporal coverage from moles?

    # Dataset Collections
    moles_reference = requests.get(f'https://catalogue.ceda.ac.uk/api/v3/observationcollections/?uuid={dataset_collection}').json()['results'][0]

    id = project.replace('-','_')

    # Find all moles UUIDs (from opensearch-collections) for this project type.
    for uuid in uuids_per_project(project, api_key=api_key):
        project_coll = add_uuid_collection(project, project_coll, uuid, 
                                           overwrite=overwrite,
                                           api_key=api_key, dryrun=dryrun)

    exists = False
    current = client.get(f"{STAC_API}/collections/{id}")
    if current.status_code == 200:
        if not overwrite:
            print(f'Project collection {id} already exists, skipping (use --overwrite to update)')
            return parent 
        
        exists = True
        project_coll = current.json()
    else:
        project_coll = copy.deepcopy(COLLECTION_TEMPLATE)

    project_coll['id'] = id
    project_coll['title'] = project

    project_coll['description'] = moles_reference['abstract']

    # Temporal coverage of a whole ecv?
    temporal_coverage = None

    ## Other metadata (summaries, keywords)
    project_coll['summaries'] = {'project': [project]}

    project_coll['keywords'] = list(
        set(['ESACCI', project, id] + id.split('.') + moles_reference.get('keywords',[]))
    )

    project_coll['links'].append({
        "rel": "parent",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{parent['id']}"
    })
    project_coll['links'].append({
        "rel": "self",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })
    project_coll['links'].append({
        "rel": "root",
        "type": "application/json",
        "href": f"{STAC_API}"
    })

    project_coll['links'] = remove_duplicate_links(project_coll['links'])

    parent['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    project_coll['extent'] = project_coll.get('extent',None) or {
        "spatial": {
            "bbox": [
                [
                    -180,
                    -90,
                    180,
                    90
                ]
            ]
            },
        "temporal": {
            "interval": [
                temporal_coverage
            ]
        }
    }

    if dryrun:
        with open(f'stac_collections/gen/{id}.json','w') as f:
            f.write(json.dumps(project_coll))
        print(f'{id}: Local')
    else:
        if exists:
            response = 'Skipped'
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
        print(f'{id}: {response}')

    return parent

    