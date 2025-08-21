import json
import copy

# Top Level CCI

## Projects (ECVs) + reccap2 + Sea Level Budget Closure
## from slug

### Features (Moles Datasets)
### 

from httpx import Client
from httpx_auth import OAuth2ClientCredentials

client = Client(
    verify=False,
    timeout=180,
)

STAC_API = 'https://api.stac.164.30.69.113.nip.io'

with open('AUTH_CREDENTIALS') as f:
    creds = json.load(f)


auth = OAuth2ClientCredentials(
    "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
    client_id=creds["id"],
    client_secret=creds["secret"]
)


def get_opensearch_record(moles_id, drs_id):

    url = f"https://archive.opensearch.ceda.ac.uk/opensearch/request?parentIdentifier={moles_id}&drsId={drs_id}&httpAccept=application/geo%2Bjson&maximumRecords=20&startPage=1"

    print(url, client.get(url))

    return client.get(url).json()

def create_aggregation_collection(agg, feature, parent_suffix='cci'):
    """
    ODP Aggregation - CEDA DRS
    """

    if parent_suffix == 'cci':
        parent_suffix = ''

    aggregation = copy.deepcopy(template)

    extent = feature['extent']
    if agg['id'] is not None and agg['id'] != '':
        opensearch_record = get_opensearch_record(
            feature['id'].replace(parent_suffix,''), 
            agg['id'].replace(parent_suffix,''))
        
        dates = []
        for i in opensearch_record['features']:
            dates += i['properties']['date'].split('/')

        dates = sorted(dates)

        extent['temporal']['interval'][0] = [dates[0], dates[-1]]
    

    id = agg['id']
    if id == "":
        id = feature['id'] + '-main' # -main of parent
    else:
        id = id + parent_suffix

    aggregation['id'] = id
    aggregation['description'] = agg['description_url']
    aggregation['title'] = id

    aggregation['extent'] = extent
    aggregation['keywords'] = feature['keywords']

    feature['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    if not dryrun:
        response = client.put(
            f"{STAC_API}/collections/{id}",
            json=aggregation,
            auth=auth,
        )
        if response.status_code not in [200,201]:
            response = client.post(
                f"{STAC_API}/collections",
                json=aggregation,
                auth=auth,
            )

        print(f' > > {id}: {response}')
    else:
        if id == 'esacci.BIOMASS.yr.L4.AGB.multi-sensor.multi-platform.MERGED.6-0.100m.openeo':
            with open('example_drs.json','w') as f:
                f.write(json.dumps(aggregation))
        print(f' > > {id}: Skipped')

    return feature

def create_subcollections(project, tmpl, pid, parent_suffix='cci'):
    """
    ODP Subcollection/Feature - CEDA Moles Identfier
    """


    if parent_suffix == 'cci':
        parent_suffix = ''

    if project not in content:
        print(f" > Cannot create subcollections for {project} at this time")
        return tmpl
    
    for fc in content[project]['feature_collection']:
        feature = copy.deepcopy(template)

        id = fc['id'] + parent_suffix
        feature['id'] = id
        feature['description'] = fc['abstract'] + '\n\n' + fc['url']
        feature['title'] = fc['feature_title']

        feature['keywords'] = tmpl['keywords']

        c3s_coverage = [
            f"{fc['start']}T00:00:00Z",
            f"{fc['end']}T00:00:00Z",
        ]

        feature['extent'] = {
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
                    c3s_coverage
                ]
            }
        }

        feature['links'].append({
            "rel": "aggregate",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}/aggregate"
        })
        feature['links'].append({
            "rel": "aggregations",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}/aggregations"
        })
        feature['links'].append({
            "rel": "items",
            "type": "application/geo+json",
            "href": f"{STAC_API}/collections/{id}/items"
        })
        feature['links'].append({
            "rel": "parent",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{pid}"
        })
        feature['links'].append({
            "rel": "queryables",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}/queryables"
        })
        feature['links'].append({
            "rel": "self",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}"
        })

        if project == 'Biomass':
            if 'openeo' in parent_suffix:
                # Determine summaries to be applied to each record.
                pass
            for agg in fc['aggregations']:
                feature = create_aggregation_collection(agg, feature, parent_suffix=parent_suffix)

        tmpl['links'].append({
            "rel" : "child",
            "type": "application/json",
            "href": f"{STAC_API}/collections/{id}"
        })

        if not dryrun:
            response = client.put(
                f"{STAC_API}/collections/{id}",
                json=feature,
                auth=auth,
            )
            print(response)
            if response.status_code not in [200,201]:
                response = client.post(
                    f"{STAC_API}/collections",
                    json=feature,
                    auth=auth,
                )
            print(f' > {id}: {response}')

        else:
            print(f' > {id}: Skipped')

    return tmpl

def create_project_collection(project, parent, parent_suffix='cci'):
    """
    ODP/CEDA Project
    """

    if parent_suffix == 'cci':
        parent_suffix = ''

    tmpl = copy.deepcopy(template)

    if project in content:
        id = content[project]['ecv']['slug'].replace('-','_')
    else:
        id = project.replace('-','_')

    id += parent_suffix

    tmpl['id'] = id
    c3s_coverage = None
    if project in projects:
        tmpl['description'] = projects[project]['abstract']
        tmpl['title'] = project

        try:
            c3s_coverage = [
                f"{content[project]['ecv']['c3s_coverage']['min_date']}T00:00:00Z",
                f"{content[project]['ecv']['c3s_coverage']['max_date']}T00:00:00Z"
            ]
        except KeyError:
            try:
                c3s_coverage = [
                    f"{content[project]['ecv']['min_date']}T00:00:00Z",
                    f"{content[project]['ecv']['max_date']}T00:00:00Z"
                ]
            except KeyError:
                print(f'No c3s_coverage for {project}')
                return

    tmpl['keywords'] = ['ESACCI', project, id]
    tmpl['summaries'] = {
        'project': [project]
    }

    if id == 'greenland_ice_sheet' or id == 'biomass' or id == 'biomass.openeo':
        tmpl = create_subcollections(project, tmpl, id, parent_suffix=parent_suffix)

    tmpl['links'].append({
        "rel": "aggregate",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}/aggregate"
    })
    tmpl['links'].append({
        "rel": "aggregations",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}/aggregations"
    })
    tmpl['links'].append({
        "rel": "parent",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{parent['id']}"
    })
    tmpl['links'].append({
        "rel": "queryables",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}/queryables"
    })
    tmpl['links'].append({
        "rel": "self",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    parent['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    tmpl['extent'] = {
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
                c3s_coverage
            ]
        }
    }

    if dryrun:
        with open(f'stac_collections/gen/{id}.json','w') as f:
            f.write(json.dumps(tmpl))
        print(f'{id}: Local')
    else:
        response = client.put(
            f"{STAC_API}/collections/{id}",
            json=tmpl,
            auth=auth,
        )
        if response.status_code not in [200,201]:
            response = client.post(
                f"{STAC_API}/collections",
                json=tmpl,
                auth=auth,
            )
        print(f'{id}: {response}')


    tmpl = {}
    return parent

if __name__ == '__main__':

    dryrun = True

    with open('config/cci_ecv_config.json') as f:
        content = json.load(f)

    with open('config/projects.json') as f:
        projects = json.load(f)

    with open('stac_collections/template.json') as f:
        temp = ''.join(f.readlines()).replace('STAC_API',STAC_API)
        
    template = json.loads(temp)

    with open('stac_collections/openeo.json') as f:
        tempo = ''.join(f.readlines()).replace('STAC_API',STAC_API)

    openeo = json.loads(tempo)

    with open('stac_collections/cci.json') as f:
        ccio = ''.join(f.readlines()).replace('STAC_API',STAC_API)

    cci = json.loads(ccio)

    top_ignore = ["facet_config","ecv_labels","ecv_title_ids","full_search_results"]
    keys = [c for c in content.keys() if c not in top_ignore]

    for project in (keys + ['reccap2','sea-level-budget-closure']):
        
        cci    = create_project_collection(project, cci)
        #if project == 'Biomass':
        #    openeo = create_project_collection(project, openeo, parent_suffix='.openeo')

    if not dryrun:
        response = client.put(
            f"{STAC_API}/collections/cci",
            json=cci,
            auth=auth,
        )
        if response.status_code not in [200,201]:
            response = client.post(
                f"{STAC_API}/collections",
                json=cci,
                auth=auth,
            )
        print(f'CCI: {response}')
    else:
        print('CCI: Skipped')
        with open('stac_collections/gen/cci.json','w') as f:
            f.write(json.dumps(cci))

    if not dryrun:
        response = client.put(
            f"{STAC_API}/collections/cci_openeo",
            json=openeo,
            auth=auth,
        )
        if response.status_code not in [200,201]:
            response = client.post(
                f"{STAC_API}/collections",
                json=openeo,
                auth=auth,
            )
        print(f'OpenEO: {response}')
    else:
        print('OpenEO: Skipped')
    