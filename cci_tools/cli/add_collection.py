__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

# Click-based script for interfacing with the cci_tools library
# to create new collections in the nested cci structure.
from cci_tools.core.utils import client, auth, STAC_API
from cci_tools.collection.main import (
    create_project_collection,
    add_drs_collection,
    add_uuid_collection,
    get_project_labels_from_opensearch
)

import click
import os

import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

def get_drs_reference(id):
    """
    Get kwargs for DRS creation"""
    accepted = False
    while not accepted:
        description = input('DRS description: ')

        print(f'Description: {description}')
        
        if input('Accept these values? (Y/N) ') == 'Y':
            accepted = True

    return {
        'id':id,
        'description_url': description
    }

# Parse command line arguments using click
@click.command()
@click.argument('parent')
@click.argument('child')
@click.option('--create', 'create', type=click.Choice(['project','moles','drs','all']),
              help='What type of nested collection to create', required=True)
@click.option('--overwrite', 'overwrite', is_flag=True, required=False)
@click.option('--dryrun', 'dryrun', is_flag=True, required=False)
@click.option('--ds_collection','dataset_collection', required=False)
@click.option('-v','verbose', count=True)

def main(parent: str, child: str, 
         create: str = None, overwrite: bool = False,
         dryrun: bool = False, dataset_collection: str = None, verbose: int = 1):
    # Add collection by DRS to a parent moles ID
    # Add/refresh moles collection

    # Parent
    # child
    # create [moles, drs, openeo]
    # Overwrite

    set_verbose(verbose)

    api_key = os.environ.get("ES_API_KEY")
    if not api_key:
        print('Warning: API Key not loaded, please set with "export ES_API_KEY=..."')

    presp = client.get(f'{STAC_API}/collections/{parent}')
    if str(presp.status_code)[0] != '2':
        raise ValueError(f'Parent could not be fetched: {presp.content}')
    
    pdata = presp.json()
    
    match create:
        case 'all':
            # Create ALL project collections - find all project labels

            project_labels = get_project_labels_from_opensearch()
            print(f'Checking existing project collections: {len(project_labels)}')

            for label in project_labels:
                pdata, added = create_project_collection(label, pdata,
                                               overwrite=overwrite,
                                               api_key=api_key,
                                               dryrun=dryrun)

        case 'project':
            pdata, added = create_project_collection(child, pdata,
                                               dataset_collection=dataset_collection,
                                               overwrite=overwrite,
                                               api_key=api_key,
                                               dryrun=dryrun)
        case 'moles':
            pdata, added = add_uuid_collection(pdata, child, 
                                         overwrite=overwrite, dryrun=dryrun,
                                         api_key=api_key)
        case 'drs':
            pdata, added = add_drs_collection(pdata, get_drs_reference(child),
                                        overwrite=overwrite, dryrun=dryrun,
                                        uuid=parent)

    if dryrun:
        print('Skipped updating parent - DRYRUN')
    elif not added:
        print('Skipped updating parent - No updates to children')
    else:
        print(parent, client.put(f'{STAC_API}/collections/{parent}', json=pdata, auth=auth))


if __name__ == '__main__':
    main()