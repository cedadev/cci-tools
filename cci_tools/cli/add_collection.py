__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

# Click-based script for interfacing with the cci_tools library
# to create new collections in the nested cci structure.
from cci_tools.collection.utils import client, auth, STAC_API
from cci_tools.collection.main import (
    create_project_collection,
    add_drs_collection,
    add_uuid_collection
)

import click
import os

def get_project_kwargs():
    """
    Get kwargs for project construction."""
    
    accepted = False
    while not accepted:
        print('Start of project temporal range: ')
        temporal_start = input('(format: YYYY-MM-DDTHH:MM:SSZ) : ')

        print('End of project temporal range: ')
        temporal_end = input('(format: YYYY-MM-DDTHH:MM:SSZ) : ')

        temporal = [temporal_start,temporal_end]

        description = input('Project Description')

        print(f'Temporal: {temporal}')
        print(f'Description: {description}')
        
        if input('Accept these values? (Y/N) ') == 'Y':
            accepted = True

    return {
        'temporal': temporal,
        'description': description
    }

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
@click.option('--create', 'create', type=click.Choice(['project','moles','drs','openeo']),
              help='What type of nested collection to create', required=True)
@click.option('--overwrite', 'overwrite', is_flag=True, required=False)
@click.option('--dryrun', 'dryrun', is_flag=True, required=False)
@click.option('--uuid', 'uuid', is_flag=True, required=False)


def main(parent: str, child: str, 
         create: str = None, overwrite: bool = False,
         dryrun: bool = False, uuid: str = None):
    # Add collection by DRS to a parent moles ID
    # Add/refresh moles collection

    # Parent
    # child
    # create [moles, drs, openeo]
    # Overwrite

    api_key = os.environ.get("ES_API_KEY")
    if not api_key:
        print('Warning: API Key not loaded, please set with "export ES_API_KEY=..."')

    presp = client.get(f'{STAC_API}/collections/{parent}')
    if str(presp.status_code)[0] != '2':
        raise ValueError(f'Parent could not be fetched: {presp.content}')
    
    pdata = presp.json()
    
    match create:
        case 'project':
            pdata = create_project_collection(child, pdata, 
                                               project_kwargs=get_project_kwargs(),
                                               overwrite=overwrite,
                                               api_key=api_key,
                                               dryrun=dryrun)
        case 'moles':
            pdata = add_uuid_collection(pdata, child, 
                                         overwrite=overwrite, dryrun=dryrun,
                                         api_key=api_key)
        case 'drs':
            pdata = add_drs_collection(pdata, get_drs_reference(child),
                                        overwrite=overwrite, dryrun=dryrun,
                                        uuid=parent)
        case 'openeo':
            if uuid is None:
                raise ValueError('Missing UUID for this OpenEO collection')
            
            pdata = add_drs_collection(pdata, get_drs_reference(child),
                                        suffix='.openeo', overwrite=overwrite,
                                        dryrun=dryrun, uuid=uuid)

    print(parent, client.put(f'{STAC_API}/collections/{parent}', json=pdata, auth=auth))


if __name__ == '__main__':
    main()