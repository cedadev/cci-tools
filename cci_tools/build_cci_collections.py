__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

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

def remove_duplicate_links(old_links, allow_capitals=True):

    # Remove duplicate links
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


def create_aggregation_collection(agg, feature, skipset, parent_suffix='cci'):
    """
    ODP Aggregation - CEDA DRS
    """

    if parent_suffix == 'cci':
        parent_suffix = ''

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
        title = '(NonDRS) ' + feature['title']
    else:
        id = id + parent_suffix
        title = id

    exists = False
    current = client.get(f"{STAC_API}/collections/{id.lower()}")
    if current.status_code != 404:
        exists = True
        aggregation = current.json()
    else:
        aggregation = copy.deepcopy(template)

    aggregation['id'] = id.lower()
    aggregation['description'] = agg['description_url']
    aggregation['title'] = title

    aggregation['extent'] = extent

    aggregation['keywords'] = aggregation['keywords'] or []
    aggregation['keywords'] = list(
        set(
            aggregation.get('keywords',[]) + \
            feature.get('keywords',[]) + \
            id.split('.')
        )
    )

    aggregation['links'] = remove_duplicate_links(aggregation['links'])

    feature['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id.lower()}"
    })

    if not dryrun:

        if exists:
            response = client.put(
                f"{STAC_API}/collections/{id.lower()}",
                json=aggregation,
                auth=auth,
            )
        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=aggregation,
                auth=auth,
            )

        if response.status_code not in [200,201]:
            raise ValueError(response.status_code)

        print(f' > > {id}: {response}')
    else:
        print(f' > > {id}: Skipped')

    return feature, skipset

def create_subcollections(project, tmpl, pid, skipcol, parent_suffix='cci',):
    """
    ODP Subcollection/Feature - CEDA Moles Identfier
    """

    if parent_suffix == 'cci':
        parent_suffix = ''

    if project not in content:
        print(f" > Cannot create subcollections for {project} at this time")
        return tmpl, skipcol
    
    for fc in content[project]['feature_collection']:

        skipset = []
        try:
            id = fc['id'] + parent_suffix

            exists = False
            current = client.get(f"{STAC_API}/collections/{id}")
            if current.status_code != 404:
                exists = True
                feature = current.json()
            else:
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
                "rel": "self",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{id}"
            })

            feature['keywords'] = list(
                set(
                    tmpl.get('keywords',[]) + \
                    feature.get('keywords',[]) + \
                    id.split('.')
                )
            )

            if 'openeo' in parent_suffix:
                # Determine summaries to be applied to each record.
                pass
            for agg in fc['aggregations']:
                try:
                    feature, skipset = create_aggregation_collection(agg, feature, skipset, parent_suffix=parent_suffix)
                except:
                    skipset.append((agg['id'],feature['id']))

            feature['links'] = remove_duplicate_links(feature['links'], allow_capitals=False)

            tmpl['links'].append({
                "rel" : "child",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{id}"
            })

            if not dryrun:
                if exists:
                    response = client.put(
                        f"{STAC_API}/collections/{id}",
                        json=feature,
                        auth=auth,
                    )
                else:
                    response = client.post(
                        f"{STAC_API}/collections",
                        json=feature,
                        auth=auth,
                    )
                print(f' > {id}: {response}')

            else:
                print(f' > {id}: Skipped')

            skipcol += skipset

        except:
            skipcol.append(tmpl['id'])

    return tmpl, skipcol

def create_project_collection(project, parent, skipcol, parent_suffix='cci'):
    """
    ODP/CEDA Project
    """

    if parent_suffix == 'cci':
        parent_suffix = ''

    if project in content:
        id = content[project]['ecv']['slug'].replace('-','_')
    else:
        id = project.replace('-','_')

    id += parent_suffix

    exists = False
    current = client.get(f"{STAC_API}/collections/{id}")
    if current.status_code != 404:
        exists = True
        tmpl = current.json()
    else:
        tmpl = copy.deepcopy(template)

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

    tmpl['keywords'] = list(
        set(
            tmpl.get('keywords',[]) + id.split('.')
        )
    )

    tmpl, skipcol = create_subcollections(project, tmpl, id, skipcol, parent_suffix=parent_suffix)

    tmpl['links'].append({
        "rel": "parent",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{parent['id']}"
    })
    tmpl['links'].append({
        "rel": "self",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })
    tmpl['links'].append({
        "rel": "root",
        "type": "application/json",
        "href": f"{STAC_API}"
    })

    tmpl['links'] = remove_duplicate_links(tmpl['links'])

    parent['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"{STAC_API}/collections/{id}"
    })

    tmpl['extent'] = tmpl.get('extent',None) or {
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
        if exists:
            response = client.put(
                f"{STAC_API}/collections/{id}",
                json=tmpl,
                auth=auth,
            )
        else:
            response = client.post(
                f"{STAC_API}/collections",
                json=tmpl,
                auth=auth,
            )
        print(f'{id}: {response}')


    tmpl = {}
    return parent, skipcol

if __name__ == '__main__':

    dryrun = False

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

    exists = False
    current = client.get(f"{STAC_API}/collections/cci")
    if current.status_code != 404:
        exists = True
        cci = current.json()
    else:
        with open('stac_collections/cci.json') as f:
            ccio = ''.join(f.readlines()).replace('STAC_API',STAC_API)
        cci = json.loads(ccio)

    top_ignore = ["facet_config","ecv_labels","ecv_title_ids","full_search_results"]
    keys = [c for c in content.keys() if c not in top_ignore]

    skipcol = []

    for project in (keys + ['reccap2','sea-level-budget-closure']):
        
        cci, skipcol    = create_project_collection(project, cci, skipcol)
        #if project == 'Biomass':
        #    openeo = create_project_collection(project, openeo, parent_suffix='.openeo')

    cci['links'] = remove_duplicate_links(cci['links'])

    print(len(skipcol), 'Skipped')

    with open('skipped.csv','w') as f:
        f.write('\n'.join([','.join(s) for s in skipcol]))

    if not dryrun:
        if exists:
            response = client.put(
                f"{STAC_API}/collections/cci",
                json=cci,
                auth=auth,
            )
        else:
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
    