import click

from cci_tools.core.utils import STAC_API, client, auth
from cci_tools.collection.main import remove_duplicate_links
import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


@click.command
@click.argument("collection_name")
@click.argument("parent")
@click.option("--new_parent", "new_parent", required=False)
@click.option("-v", "--verbose", count=True)
def main(collection_name: str, parent: str, new_parent: str = None, verbose: int = 0):
    """
    Migrate an existing collection from its current parent to a new parent collection.
    """

    set_verbose(verbose)

    # Only remove where necessary
    if parent != "root":
        parent_data = client.get(f"{STAC_API}/collections/{parent}").json()

        # Remove from parent
        new_links = []
        for coll in parent_data["links"]:
            if coll["rel"] != "child":
                new_links.append(coll)
                continue
            if collection_name not in coll["href"]:
                new_links.append(coll)
                continue
            logger.info(f"Removed: {coll['href'].split('/')[-1]}")

        parent_data["links"] = remove_duplicate_links(new_links)
        logger.info(
            f"Old: {client.put(f'{STAC_API}/collections/{parent}', json=parent_data, auth=auth)}"
        )

    # Add to migration location (if applicable)
    if new_parent is not None:
        new_parent_data = client.get(f"{STAC_API}/collections/{new_parent}").json()

        new_parent_data["links"].append(
            {
                "rel": "child",
                "type": "application/json",
                "href": f"{STAC_API}/collections/{collection_name}",
            }
        )

        new_parent_data["links"] = remove_duplicate_links(new_parent_data["links"])

        logger.info(
            f"New: {client.put(f'{STAC_API}/collections/{new_parent}', json=new_parent_data, auth=auth)}"
        )


if __name__ == "__main__":
    main()
