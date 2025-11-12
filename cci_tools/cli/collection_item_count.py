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
@click.option('--count_all','count_all', required=False, is_flag=True)

def main(collection, quick_count=False, quick_check=False, aggregations=False, depth=0, count_all=False):
    collection_item_count(collection, quick_count=quick_count, quick_check=quick_check, aggregations=aggregations, depth=depth, count_all=count_all)

def collection_item_count(collection,quick_count=False, quick_check=False, aggregations=False, depth=0, count_all=False):
    total_count, collection_summary = recursive_find(f'{STAC_API}/collections/{collection}',[], 
                                           item_aggregations=aggregations,
                                           depth=depth,
                                           quick_check=quick_check, count_all=count_all)

if __name__ == "__main__":
    main()
