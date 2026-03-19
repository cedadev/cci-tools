import click
from cci_tools.readers.s3 import locate_content

@click.command()
@click.argument('prefix')

def main(prefix):

    bucket = 'ceda-backup-data'
    endpoint = 'https://ceda-backup-data.obs.eu-nl.otc.t-systems.com/'

    fileset = locate_content(bucket, prefix=prefix)

    for file in fileset:
        print(endpoint + file)

if __name__ == '__main__':
    main()