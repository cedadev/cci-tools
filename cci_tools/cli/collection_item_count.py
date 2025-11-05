import click
from cci_tools.core.utils import recursive_find, STAC_API

@click.command()
@click.argument('collection')
# Also count aggregated items
@click.option('--aggs','aggregations', required=False, is_flag=True)
# Just check if the collection contains any items
@click.option('--check','quick_check', required=False, is_flag=True)
# Just show the count for collections at X depth
@click.option('--depth','depth', required=False, type=int, default=0)

def main(collection, quick_count=False, quick_check=False, aggregations=False, depth=0):
    collection_item_count(collection, quick_count=quick_count, quick_check=quick_check, aggregations=aggregations, depth=depth)

def collection_item_count(collection,quick_count=False, quick_check=False, aggregations=False, depth=0):
    _, collection_summary = recursive_find(f'{STAC_API}/collections/{collection}',[], 
                                           item_aggregations=aggregations,
                                           depth=depth,
                                           quick_check=quick_check)

if __name__ == "__main__":
    main()
