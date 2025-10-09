__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

# Update an existing collection

from cci_tools.collection.utils import client, auth, STAC_API
from cci_tools.collection.main import remove_duplicate_links
import click
import json
import glob

# Parse command line arguments using click
@click.command()
@click.argument("collection_file")
@click.argument("parent", required=False)

def main(collection_file: str, parent: str = None):

    if collection_file[-1] == '/':
        fset = glob.glob(f'{collection_file}/*')
    else:
        fset = [collection_file]

    for f in fset:
        collection_file = f
        collection = collection_file.split('/')[-1].replace('.json','')

        post = True
        if client.get(f"{STAC_API}/collections/{collection}").status_code != 404:
            post = False

        print(f'Post collection: {post}')

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

            parent_data['links'] = remove_duplicate_links(parent_data['links'])

            client.put(f"{STAC_API}/collections/{parent}", json=parent_data, auth=auth)

        with open(collection_file) as f:
            collection_data = json.loads(''.join([r.strip() for r in f.readlines()]).replace('STAC_API',STAC_API))

        if post:
            resp = client.post(f"{STAC_API}/collections", json=collection_data, auth=auth)
        else:
            resp = client.put(f"{STAC_API}/collections/{collection}", json=collection_data, auth=auth)
        print(collection, resp)


if __name__ == '__main__':
    main()