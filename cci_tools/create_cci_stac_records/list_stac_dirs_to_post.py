from pathlib import Path
import click

def list_subdirectories(path):
    """
    Lists subdirectories of the given path
    """
    path_list=[]

    for subdir in Path(path).rglob('*'):
        if subdir.is_dir():
            print(subdir)
            path_list.append(subdir)
    
    output_dir=f'/gws/nopw/j04/esacci_portal/stac/stac_records/post_stac'
    output_file=f"{output_dir}/stac_record_dirs_to_post.txt"
    print(f"{output_file}")

    for i in path_list:
        print(f"Post records from: {i}")

    with open(output_file, "w") as file:
        for i in path_list:
                file.write(f"{i} \n")
                print(f"Post records from: {i}")
    
    print("")    
    print(f"The directories of STAC records ready to post have been written to:")
    print(output_file)
    print("")

# Parse command line arguments using click
@click.command()
@click.argument('path', type=click.Path(exists=True))

def main(path):
    list_subdirectories(path)

if __name__ == "__main__":
    main()
