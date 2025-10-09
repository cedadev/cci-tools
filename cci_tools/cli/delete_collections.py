__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import click
from time import sleep

from cci_tools.collection.utils import STAC_API, client, auth, dryrun

def remove_items(item_url, dryrun=True, item_aggregations=False):
    """
    Remove all items for a specific collection."""

    item_data = {'features':[None]}
    deleted_items = True
    while deleted_items:

        deleted_items = False
        resp = client.get(item_url)
        if resp.status_code == 404:
            return
        item_data = resp.json()

        for item in item_data['features']:
            if not item_aggregations and item["properties"].get('aggregation'):
                # If not deleting aggregations, skip aggregated items
                print(f'SKIP_A {item_url}/{item["id"]}')
                continue
            
            print(f'DELETE {item_url}/{item["id"]}')
            if not dryrun:
                deleted_items = True
                print(client.delete(f'{item_url}/{item["id"]}',auth=auth))
                sleep(0.1)

        if dryrun:
            return

def recursive_removal(collection, depth, top_only=False, lowest_only=False, 
                      keep_collections=False, dryrun=True, delete_depth=None, 
                      item_aggregations=False):
    """
    Remove collections recursively so no collections are left orphaned.
    
    This is less of an issue with collections vs items, but still with the large
    range of CCI collections this is important as orphaned collections may easily
    be 'lost'."""

    has_children = False
    resp = client.get(collection)
    if resp.status_code == 404:
        return
    
    coll_data = resp.json()

    remove_items(f'{collection}/items', dryrun=dryrun, item_aggregations=item_aggregations)

    if not top_only:
        for link in coll_data['links']:
            if link['rel'] == 'child':
                recursive_removal(link['href'], depth+1, lowest_only=lowest_only, 
                                keep_collections=keep_collections,
                                dryrun=dryrun, delete_depth=delete_depth, item_aggregations=item_aggregations)
                has_children = True

    if delete_depth is not None and delete_depth != depth:
        return

    if keep_collections:
        return
    
    # If lowest only and not has children, or not lowest only.
    if not lowest_only or (lowest_only and not has_children):
        print(f'DELETE {collection.split("/")[-1]}')
        if not dryrun:
            client.delete(collection, auth=auth)

DEPTHS_EXPLAINED = [
    'CCI',
    'Project',
    'Moles-Record',
    'DRS'
]

# Parse command line arguments using click
@click.command()
@click.argument('collection')
@click.argument('parent', required=False)
@click.option('--keep_collections','keep_collections',is_flag=True,
              help='Keep collections')
@click.option('--item_aggregations','item_aggregations',is_flag=True,
              help='Delete aggregated items')
@click.option('--top_only','top_only',is_flag=True,
              help='Delete the specified collection only')
@click.option('--lowest_only','lowest_only',is_flag=True,
              help='Delete the lowest level collections (with no children)')
@click.option('-r','realrun',is_flag=True,
              help='Actually delete content')
@click.option('--delete_depth','delete_depth', type=int,
              help='Delete collections at a certain depth in the nested collection set.')

def main(collection: str, parent: str = None, 
         keep_collections = False,
         item_aggregations = False,
         top_only = False, lowest_only=False, 
         realrun=False, delete_depth=None):

    dryrun = not realrun

    if parent and not lowest_only and keep_collections:
        parent_data = client.get(f'{STAC_API}/collections/{parent}').json()
        # Remove collection link from parent
        new_links = []
        for link in parent_data['links']:
            if not (link['rel'] == 'child' and collection in link['href']):
                new_links.append(link)
            else:
                print(f'Removing {collection} from {parent} (parent)')
        parent_data['links'] = new_links

        client.put(
            f'{STAC_API}/collections/{parent}',
            json=parent_data,
            auth=auth
        )

    # Generate warnings
    if not dryrun:
        if keep_collections:
            print(f'Are you sure you want to remove all items from the {collection} collection?')
        elif top_only:
            print(f'Are you sure you want to remove the {collection} collection?')
        elif delete_depth:
            if delete_depth > len(DEPTHS_EXPLAINED)-1:
                raise ValueError('Depth exceeds known collection levels')
            
            exp = DEPTHS_EXPLAINED[delete_depth]
            print(f'Are you sure you want to remove all collections at the {exp} level?')
        else:
            print(f'Are you sure you want to remove the {collection} collection and ALL collections/items below it?')
        ask = input('Y/N: ')
        if ask != 'Y':
            return
        
    recursive_removal(f'{STAC_API}/collections/{collection}', 
                    1, top_only=top_only, lowest_only=lowest_only, 
                    keep_collections=keep_collections,
                    dryrun=dryrun, delete_depth=delete_depth, item_aggregations=item_aggregations)


if __name__ == '__main__':
    main()
