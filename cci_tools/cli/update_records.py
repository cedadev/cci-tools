import click
import json
import glob
import os

from cci_tools.stac.upgrade_record import upgrade_record

@click.command()
@click.argument('directory')
def main(directory: str):
    """
    Provide a directory containing one or more item collections
    """

    collections = glob.glob(f"{directory}/*.json")

    print(f'Identified: {len(collections)} collections')
    for y, collection in enumerate(collections):
        if 'hold' in collection or 'test' in collection:
            continue

        print(f'Updating collection {y+1}/{len(collections)} ({collection.split("/")[-1][:10]})')
        per_item_collection(collection)

def per_item_collection(item_collection: str, overwrite: bool = False):
    """
    Update all items in a local collection

    item_collection should be a path to a single item or a path to a collection file
    """

    if os.path.isfile(item_collection.replace('.json','.cci1.0.json').replace('temp','archive')) and not overwrite:
        return

    with open(item_collection) as f:
        data = json.load(f)

    if not isinstance(data,list):
        data = [data]

    # Should be the same within a single collection
    uuid, moles_info, providers = None, None, None

    new_data = []
    for x, item in enumerate(data):
        print(f' > Updating {(x+1)}/{len(data)}')

        new_item, uuid, moles_info, providers = upgrade_record(item, uuid, moles_info, providers)
        new_data.append(new_item)

    if len(new_data) == 1:
        new_data = new_data[0]

    with open(item_collection.replace('.json','.cci1.0.json').replace('from_otc','archive'),'w') as f:
        f.write(json.dumps(new_data))

if __name__ == '__main__':
    main()