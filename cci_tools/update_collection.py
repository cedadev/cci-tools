__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

from cci_tools.utils import client, auth, STAC_API
import click
import json

# Parse command line arguments using click
@click.command()
@click.argument("collection_file")
@click.argument("parent", required=False)

def main(collection_file: str, parent: str = None):

    collection = collection_file.split('/')[-1].replace('.json','')

    post = True
    if client.get(f"{STAC_API}/collections/{collection}").status_code != 404:
        post = False

    if parent:

        new_link = {
            "rel":"child",
            "type": "application/json",
            "href":f"{STAC_API}/collections/{collection}"
        }
        parent_data = client.get(f"{STAC_API}/collections/{parent}").json()

        exists=False
        for l in parent_data["links"]:
            exists = exists or (l['href'] == new_link['href'])
        if not exists:
            parent_data['links'].append(new_link)
        client.put(f"{STAC_API}/collections/{parent}", json=parent_data, auth=auth)

    with open(collection_file) as f:
        collection_data = json.load(f)

    if post:
        client.post(f"{STAC_API}/collections", json=collection_data, auth=auth)
    else:
        client.put(f"{STAC_API}/collections/{collection}", json=collection_data, auth=auth)

if __name__ == '__main__':
    main()