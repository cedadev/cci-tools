import click

from cci_tools.core.utils import STAC_API, client, auth
from cci_tools.collection.main import remove_duplicate_links

@click.command
@click.argument('collection_name')
@click.argument('parent')
@click.option('--new_parent', 'new_parent',required=False)

def main(collection_name: str, parent: str, new_parent: str = None):

    # Only remove where necessary
    if parent != 'root':
        parent_data = client.get(f'{STAC_API}/collections/{parent}').json()

        # Remove from parent
        new_links = []
        for coll in parent_data['links']:
            if coll['rel'] != 'child':
                new_links.append(coll)
                continue
            if collection_name not in coll['href']:
                new_links.append(coll)
                continue
            print('Removed:',coll['href'].split('/')[-1])

        parent_data['links'] = remove_duplicate_links(new_links)
        print('Old:',client.put(f'{STAC_API}/collections/{parent}', 
                                json=parent_data, 
                                auth=auth))

    # Add to migration location (if applicable)
    if new_parent is not None:
        new_parent_data = client.get(f'{STAC_API}/collections/{new_parent}').json()

        new_parent_data['links'].append({
            'rel':'child',
            'type':'application/json',
            'href':f'{STAC_API}/collections/{collection_name}'
        })

        new_parent_data['links'] = remove_duplicate_links(new_parent_data['links'])

        print('New: ',client.put(f'{STAC_API}/collections/{new_parent}', 
                                 json=new_parent_data, 
                                 auth=auth))

if __name__ == '__main__':
    main()
