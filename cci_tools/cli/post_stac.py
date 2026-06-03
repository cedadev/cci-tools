import click
import glob

from cci_tools.stac.post_record import post_records
from cci_tools.core.utils import client, auth
import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False


@click.command()
@click.argument("post_directory", type=click.Path(exists=True))
@click.option(
    "--openeo", help="Flag for enabling openEO-specific posting rules", is_flag=True
)
@click.option("-v", "--verbose", count=True)
def main(post_directory, openeo: bool = False, verbose: int = 0):

    set_verbose(verbose)

    if post_directory.isnumeric():
        path_file = "/gws/nopw/j04/esacci_portal/stac/stac_records/post_stac/stac_record_dirs_to_post.txt"
        with open(path_file) as f:
            post_directory = [r.strip() for r in f.readlines()][int(post_directory)]

    post_records(post_directory, openeo)


if __name__ == "__main__":
    main()
