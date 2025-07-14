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

with open('AUTH_CREDENTIALS') as f:
    creds = json.load(f)


auth = OAuth2ClientCredentials(
    "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
    client_id=creds["id"],
    client_secret=creds["secret"]
)

with open('config/cci_ecv_config.json') as f:
    content = json.load(f)

with open('config/projects.json') as f:
    projects = json.load(f)

with open('stac_collections/cci.json') as f:
    cci = json.load(f)

with open('stac_collections/template.json') as f:
    template = json.load(f)

def create_aggregation_collection(agg, feature):

    aggregation = copy.deepcopy(template)

    id = agg['id']
    if id == "":
        id = 'main'

    aggregation['id'] = id
    aggregation['description'] = agg['description_url']
    aggregation['title'] = id

    aggregation['extent'] = feature['extent']

    feature['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}"
    })

    response = client.post(
        "https://api.stac-master.rancher2.130.246.130.221.nip.io/collections",
        json=aggregation,
        auth=auth,
    )
    print(f'{id}:{response}')

    return feature

def create_subcollections(project, tmpl, pid):

    if project not in content:
        print(f"Cannot create subcollections for {project} at this time")
        return tmpl
    
    for fc in content[project]['feature_collection']:
        feature = copy.deepcopy(template)

        id = fc['id']
        feature['id'] = fc['id']
        feature['description'] = fc['abstract'] + '\n\n' + fc['url']
        feature['title'] = fc['feature_title']

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
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/aggregate"
        })
        feature['links'].append({
            "rel": "aggregations",
            "type": "application/json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/aggregations"
        })
        feature['links'].append({
            "rel": "items",
            "type": "application/geo+json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/items"
        })
        feature['links'].append({
            "rel": "parent",
            "type": "application/json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{pid}"
        })
        feature['links'].append({
            "rel": "queryables",
            "type": "application/json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/queryables"
        })
        feature['links'].append({
            "rel": "self",
            "type": "application/json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}"
        })

        if project == 'Biomass':
            for agg in fc['aggregations']:
                feature = create_aggregation_collection(agg, feature)

        tmpl['links'].append({
            "rel" : "child",
            "type": "application/json",
            "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}"
        })

        response = client.put(
            f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}",
            json=feature,
            auth=auth,
        )
        print(f'{id}: {response}')

    return tmpl


def create_project_collection(project, cci):

    tmpl = copy.deepcopy(template)

    if project in content:
        id = content[project]['ecv']['slug'].replace('-','_')
    else:
        id = project.replace('-','_')

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

    if id == 'greenland_ice_sheet' or id == 'biomass':
        tmpl = create_subcollections(project, tmpl, id)

    tmpl['links'].append({
        "rel": "aggregate",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/aggregate"
    })
    tmpl['links'].append({
        "rel": "aggregations",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/aggregations"
    })
    tmpl['links'].append({
        "rel": "parent",
        "type": "application/json",
        "href": "https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/cci"
    })
    tmpl['links'].append({
        "rel": "queryables",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}/queryables"
    })
    tmpl['links'].append({
        "rel": "self",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}"
    })

    cci['links'].append({
        "rel" : "child",
        "type": "application/json",
        "href": f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}"
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
    else:
        print(f"Posting {id}")
        response = client.put(
            f"https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/{id}",
            json=tmpl,
            auth=auth,
        )
        print(response)


    tmpl = {}
    return cci

if __name__ == '__main__':

    dryrun = False

    with open('config/cci_ecv_config.json') as f:
        content = json.load(f)

    with open('config/projects.json') as f:
        projects = json.load(f)

    with open('stac_collections/template.json') as f:
        template = json.load(f)

    top_ignore = ["facet_config","ecv_labels","ecv_title_ids","full_search_results"]
    keys = [c for c in content.keys() if c not in top_ignore]

    for project in (keys + ['reccap2','sea-level-budget-closure']):
        
        print(f'Creating {project}')
        cci = create_project_collection(project, cci)


    response = client.put(
        "https://api.stac-master.rancher2.130.246.130.221.nip.io/collections/cci",
        json=cci,
        auth=auth,
    )
    print(response)
    