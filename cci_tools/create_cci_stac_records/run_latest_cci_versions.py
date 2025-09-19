#!/usr/bin/env python
__author__    = "Diane Knappett"
__contact__   = "diane.knappett@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

"""
This script walks the directory structure to find the latest versions
of cci data and then creates stac records for it

e.g. python cci_tools/create_cci_stac_records/run_latest_cci_versions.py /neodc/esacci/aerosol/data
"""
import click
import os
import re
from packaging.version import Version
import create_cci_stac_records

def find_latest_versions(root_path):
    latest_versions = {}

    version_pattern = re.compile(r'^v(\d+(?:\.\d+)*)$')

    for dirpath, dirnames, filenames in os.walk(root_path):
        version_dirs = []
        for dirname in dirnames:
            match = version_pattern.match(dirname)
            if match:
                try:
                    version = Version(match.group(1))
                    padded_dirname=dirname.ljust(5,'0')  # Pads vX.X to be vX.XX. This ensures that v4.3 (i.e. v4.30) is interpretted as higher than v4.21
                    version_dirs.append((version, dirname, padded_dirname))
                except Exception:
                    continue  # Skip invalid version formats

        if version_dirs:
            # Sort and pick the highest version

            # Calculate the latest version numerically
            latest = max(version_dirs, key=lambda x: x[2])
            latest_ver_num = latest[1]
            # Detemine the latest version via a sorted string list
            ver_list=sorted([x[1] for x in version_dirs])
            latest_ver=ver_list[-1]

            if latest_ver == latest_ver_num:
                latest_versions[dirpath] = os.path.join(dirpath, latest_ver)
            else:
                print(f"WARNING: {latest_ver} and {latest_ver_num} don't match!")
                latest_versions[dirpath] = None

    return latest_versions

# Parse command line arguments using click
@click.command()
@click.argument('root_directory')

def main(root_directory):
    if root_directory.isnumeric():
        path_file='/home/users/dknappett/tools/cci-tools/cci_project_paths.txt'
        with open(path_file) as f:
            root_directory=[r.strip() for r in f.readlines()][int(root_directory)]

    # Example usage
    #root_directory = '/neodc/esacci/aerosol/data'
    latest_dirs = find_latest_versions(root_directory)

    #for parent, latest in latest_dirs.items():
    #    print(f"{parent} → {latest}")
    
    # Extract project/ECV (i.e. after /neodc/esacci/)
    dir_split=root_directory.split('/')
    project=dir_split[3]

    output_dir=f'/gws/nopw/j04/esacci_portal/stac/stac_records/lotus_jobs/{project}'
    if not os.path.exists(output_dir):
        try:
            os.mkdir(output_dir)
            print(f"Created directory '{output_dir}' successfully")
        except PermissionError:
            print(f"Permission denied: Unable to make '{output_dir}'")
        except Exception as e:
            print(f"An error occured '{e}'")

    output_file=f"{output_dir}/latest_version_list.txt"

    with open(output_file, "w") as file:
        for parent, latest in latest_dirs.items():
                file.write(latest + "\n")
                print(f"{parent} → {latest}")
    
    print("")    
    print(f"The latest version of CCI directories for the {project} project have been written to the following file:")
    print(output_file)
    print("")

    # Loop over creation of STAC records
    for parent, latest in latest_dirs.items():
        create_cci_stac_records.create_stac(latest, output_dir, None)

if __name__ == "__main__":
    main()