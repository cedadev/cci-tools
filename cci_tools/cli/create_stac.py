import click
import json
import os

from cci_tools.core.utils import (
    es_client, client, auth, STAC_API,
    get_file_query, get_dir_query
)
from cci_tools.stac.create_record import (
    handle_process_record
)

# Parse command line arguments using click
@click.command()
@click.argument('cci_dirs')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--output_drs', 'output_drs', required=False,
              help='DRS to apply to all items for the CCI dirs specified')
@click.option('--exclusion',  'exclusion', required=False,
              help='Exclude opensearch records with this string present in their name')
@click.option('--start_time', 'start_time', required=False,
              help='Manually specify %Y%m%dT%H%M%SZ format start_time') # "%Y%m%dT%H%M%SZ"
@click.option('--end_time',   'end_time', required=False,
              help='Manually specify %Y%m%dT%H%M%SZ format end_time')
@click.option('--interval',   'interval', required=False,
              help='Interval for where start_time is defined by the filename')
@click.option('--global',   'assume_global', required=False, is_flag=True,
              help='Assume global coverage where other data is not present')
@click.option('--openeo',   'openeo', required=False, is_flag=True,
              help='Enable OpenEO-specific STAC configurations')
@click.option('--halt',   'halt', required=False, is_flag=True,
              help='Halt on errors')

def main(
        cci_dirs, 
        output_dir, 
        output_drs, 
        **kwargs
    ):
    '''
    Reads in OpenSearch records for CCI NetCDF and geotiff data.

    For NetCDF files, information is extracted from the OpenSearch record only.

    For GeoTIFF files, only partial information is available within the OpenSearch record, so additional metadata is extracted from the GeoTIFF file itself.
    '''
    create_stac(cci_dirs, output_dir, output_drs, **kwargs)

def create_stac(
        cci_dirs, 
        output_dir, 
        output_drs, 
        exclusion=None, 
        start_time=None, 
        end_time=None,
        halt=False,
        **kwargs
    ):
    
    exclusion = exclusion or 'uf8awhjidaisdf8sd'
    splitter = None
    drs = output_drs

    if os.path.isfile(cci_dirs):
        with open(cci_dirs) as f:
            cci_configurations = [r.strip().split(',') for r in f.readlines()]
    else:
        cci_configurations = [[cci_dirs]]

    for cfg in cci_configurations:

        cci_dir = cfg[0]
        if len(cfg) > 1:
            drs = cfg[1]
        if len(cfg) > 2:
            if os.path.isfile(cfg[2]):
                with open(cfg[2]) as f:
                    splitter = json.load(f)
            else:
                splitter=cfg[2]
        

        print(f"Input CCI directory: {cci_dir}")
        print(f"Output STAC record directory: {output_dir}")

        if drs == '':
            drs = output_drs

        # Loop over OpenSearch records, converting each to STAC format
        failed_list=[]
        count_success=0
        count_fail=0
        is_last = False

        if os.path.isfile(cci_dir):
            if cci_dir.endswith('.txt'):
                with open(cci_dir) as f:
                    fileset = [r.strip() for r in f.readlines()]
            else:
                fileset = [cci_dir]

            for file in fileset:
                body = get_file_query(file)
                hits = es_client.search(index='opensearch-files', body=body)['hits']['hits']

                if len(hits) == 0:
                    print("")
                    print(f"{file}: Not found in Opensearch")
                    continue
                
                record = hits[0]
                success, status = handle_process_record(
                    record,
                    output_dir,
                    exclusion=exclusion,
                    drs=drs,
                    splitter=splitter,
                    start_time=start_time,
                    end_time=end_time,
                    halt=halt,
                    **kwargs
                )

                if not success:
                    failed_list.append(f'{file}:{status}')
                if not status:
                    failed_list.append(f'{file}:incomplete')
        else:
            body = get_dir_query(cci_dir)
            hits = es_client.search(index='opensearch-files', body=body)['hits']['hits']
            if len(hits) == 0:
                print("")
                print(f"{cci_dir}: No OpenSearch hits found!")
                continue
        
            while len(response['hits']['hits']) == 10 or not is_last:
                if len(response['hits']['hits']) != 10:
                    is_last = True
                for record in response['hits']['hits']:
                    success, status = handle_process_record(
                        record,
                        output_dir,
                        exclusion=exclusion,
                        drs=drs,
                        splitter=splitter,
                        start_time=start_time,
                        end_time=end_time,
                        halt=halt,
                        **kwargs
                    )
                    if not success:
                        failed_list.append(f'{file}:{status}')
                    if not status:
                        failed_list.append(f'{file}:incomplete')

                searchAfter = response['hits']['hits'][-1]["sort"]
                body['search_after'] = searchAfter
                response = client.search(index='opensearch-files', body=body)
                if len(response["hits"]["hits"]) == 0:
                    is_last=True
            
        if failed_list:
            try:
                output_failed_files=f"{output_dir}/failed_files_{record["_source"]["projects"]["opensearch"]["datasetId"]}.txt"
            except:
                output_failed_files=f"{output_dir}/failed_files-no_datasetID.txt"

            with open(output_failed_files, "w") as file:
                for item in failed_list:
                    file.write(item + "\n")
            print(f"The list of files for which STAC records could not be created, or that were created but are incomplete, have been written to the following file:")
            print(output_failed_files)
            print("")

        print("")
        print(f"No. of STAC records created successfully: {count_success}")
        print(f"No. of STAC records that failed: {count_fail}")
        print("")


if __name__ == "__main__":
    main()