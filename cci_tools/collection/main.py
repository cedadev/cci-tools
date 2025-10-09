__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import json
import copy
import requests
from cci_tools.collection.utils import (
    client, 
    auth, 
    STAC_API,
    COLLECTION_TEMPLATE,
    get_opensearch_record,
    uuids_per_project,
    es_collection
)

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
        suffix: str = None,
        overwrite: bool = False, 
        dryrun: bool = False,
        uuid: str = None):
    """
    Add a DRS collection to a parent MOLES UUID-based collection

    ODP Aggregation - CEDA DRS

    parent - id, title, keywords, links
    """
    suffix = suffix or ''

    extent = copy.deepcopy(parent['extent'])
    if drs_reference['id'] is not None and drs_reference['id'] != '':
        opensearch_record = get_opensearch_record(
            parent['id'].replace(suffix,''), 
            drs_reference['id'].replace(suffix,''))
        
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
    

    id = drs_reference['id']
    if id == "":
        id = parent['id'] + '-main' # -main of parent
        title = '(NonDRS) ' + parent['title']
    else:
        id = id + suffix
        title = id

    exists = False
    current = client.get(f"{STAC_API}/collections/{id.lower()}")
    if current.status_code != 404:
        exists = True
        drs_stac = current.json()
    else:
        drs_stac = copy.deepcopy(COLLECTION_TEMPLATE)

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

    if not dryrun:

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
    else:
        print(f' > > {id}: Skipped')

    return parent

def add_uuid_collection(
        project_coll: dict, 
        uuid: str, 
        suffix: str = None,
        overwrite: bool = False, 
        dryrun: bool = False,
        api_key: str = None
    ):
    """
    ODP Subcollection/Feature - CEDA Moles Identfier
    """

    suffix = suffix or ''

    moles_ids = []
    for moles_record in es_collection(uuid, api_key=api_key):

        fc = {}
        fc['id']    = moles_record['collection_id']
        if fc['id'] == 'cci':
            continue
        fc['abstract'] = requests.get(
            f'https://catalogue.ceda.ac.uk/api/v2/observations.json?discoveryKeywords__name=ESACCI&uuid={fc["id"]}').json()['results'][0]['abstract']
        fc['feature_title'] = moles_record['title']
        fc['url']   = f'https://catalogue.ceda.ac.uk/uuid/{moles_record["collection_id"]}'
        fc['start'] = (moles_record.get('start_date',None) or '1970-01-01').split('T')[0]
        fc['end']   = (moles_record.get('end_date',None) or '2025-12-31').split('T')[0]
        fc['aggregations'] = []
        for drs in moles_record.get('drsId',[""]):

            description_url = f'https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={fc["id"]}'
            if drs != "":
                description_url += f'&drsId={drs}'
            fc['aggregations'].append({
                'id': drs,
                'description_url': description_url
            })

        try:
            id = fc['id'] + suffix

            exists = False
            current = client.get(f"{STAC_API}/collections/{id}")
            if current.status_code != 404:
                exists = True
                moles_stac = current.json()
            else:
                moles_stac = copy.deepcopy(COLLECTION_TEMPLATE)

            id = fc['id'] + suffix
            moles_stac['id'] = id
            moles_stac['description'] = fc['abstract'] + '\n\n' + fc['url']
            moles_stac['title'] = fc['feature_title']

            moles_stac['keywords'] = project_coll['keywords']

            temporal_coverage = [
                f"{fc['start']}T00:00:00Z",
                f"{fc['end']}T23:59:59Z",
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

            if 'openeo' in suffix:
                # Determine summaries to be applied to each record.
                pass
            for drs in fc['aggregations']:

                # Agg - id, description_url
                try:
                    moles_stac = add_drs_collection(drs, moles_stac, 
                                                    suffix=suffix, overwrite=overwrite,
                                                    uuid=uuid)
                except Exception as err:
                    raise err

            moles_stac['links'] = remove_duplicate_links(moles_stac['links'], allow_capitals=False)

            project_coll['links'].append({
                "rel" : "child",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{id}"
            })

            if not dryrun or fc['id'] in moles_ids:
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
                    x=input()

            else:
                print(f' > {id}: Skipped')

            moles_ids.append(fc['id'])
        except Exception as err:
            raise err

    return project_coll

def create_project_collection(
        project: str, 
        parent: dict, 
        suffix: str = None,
        project_reference: dict = None,
        project_kwargs: dict = None,
        overwrite: bool = False,
        api_key: str = None,
        dryrun: bool = False):
    """
    ODP/CEDA Project
    """

    project_reference = project_reference or {}
    project_kwargs = project_kwargs or {}

    suffix = suffix or ''

    if project in project_reference:
        id = project_reference[project]['ecv']['slug'].replace('-','_')
    else:
        id = project.replace('-','_')

    id += suffix

    exists = False
    current = client.get(f"{STAC_API}/collections/{id}")
    if current.status_code != 404:
        exists = True
        project_coll = current.json()
    else:
        project_coll = copy.deepcopy(COLLECTION_TEMPLATE)

    project_coll['id'] = id
    project_coll['title'] = project
    temporal_coverage = None
    if project in project_reference:
        project_coll['description'] = project_reference[project]['abstract']

        try:
            temporal_coverage = [
                f"{project_reference[project]['ecv']['temporal_coverage']['min_date']}T00:00:00Z",
                f"{project_reference[project]['ecv']['temporal_coverage']['max_date']}T00:00:00Z"
            ]
        except KeyError:
            try:
                temporal_coverage = [
                    f"{project_reference[project]['ecv']['min_date']}T00:00:00Z",
                    f"{project_reference[project]['ecv']['max_date']}T00:00:00Z"
                ]
            except KeyError:
                print(f'No temporal_coverage for {project}')
                return
    else:
        temporal_coverage = project_kwargs['temporal']
        project_coll['description'] = project_kwargs['description']

    project_coll['keywords'] = ['ESACCI', project, id]
    project_coll['summaries'] = {
        'project': [project]
    }

    project_coll['keywords'] = list(
        set(
            project_coll.get('keywords',[]) + id.split('.')
        )
    )
    for uuid in uuids_per_project(project, api_key=api_key):
        project_coll = add_uuid_collection(project, project_coll, uuid, 
                                           suffix=suffix, overwrite=overwrite,
                                           api_key=api_key, dryrun=dryrun)

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

    