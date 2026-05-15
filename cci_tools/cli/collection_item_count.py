import click
from cci_tools.core.utils import recursive_find, STAC_API
import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


@click.command()
@click.argument("collection")
# Also count aggregated items
@click.option("--aggs", "aggregations", required=False, is_flag=True)
# Just check if the collection contains any items
@click.option("--check", "quick_check", required=False, is_flag=True)
# Just show the count for collections at X depth
@click.option("--depth", "depth", required=False, type=int, default=0)
@click.option("--count_all", "count_all", required=False, is_flag=True)
@click.option("-v", "verbose", count=True)
def main(
    collection: str,
    quick_count: bool = False,
    quick_check: bool = False,
    aggregations: bool = False,
    depth: int = 0,
    count_all: bool = False,
    verbose: int = 0,
):
    """
    Count the number of items in a collection, with options to include aggregated items,
    perform a quick check for the presence of items, and count items at a specific depth only.
    """

    set_verbose(verbose)
    collection_item_count(
        collection,
        quick_count=quick_count,
        quick_check=quick_check,
        aggregations=aggregations,
        depth=depth,
        count_all=count_all,
    )


def collection_item_count(
    collection,
    quick_count=False,
    quick_check=False,
    aggregations=False,
    depth=0,
    count_all=False,
):
    total_count, collection_summary = recursive_find(
        f"{STAC_API}/collections/{collection}",
        [],
        item_aggregations=aggregations,
        depth=depth,
        quick_check=quick_check,
        count_all=count_all,
    )


if __name__ == "__main__":
    main()
