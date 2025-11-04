import click
from cci_tools.core.utils import recursive_find, STAC_API

@click.command()
@click.option('--collection', 'collection', default='cci',
              help='The CCI STAC collection within which to search for sub-collections or items e.g. aerosol.')

def main(collection):
    collection_item_count(collection)

def collection_item_count(collection):
    _, collection_summary = recursive_find(f'{STAC_API}/collections/{collection}',[], depth=1, quick_count=False)

if __name__ == "__main__":
    main()
