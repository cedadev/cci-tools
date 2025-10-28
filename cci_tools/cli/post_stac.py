import click
import glob

from cci_tools.stac.post_record import post_record
from cci_tools.core.utils import client, auth


@click.command()
@click.argument('post_directory', type=click.Path(exists=True))
@click.option('--openeo', help='Flag for enabling openEO-specific posting rules',is_flag=True)
def main(post_directory, openeo: bool = False):

    if post_directory.isnumeric():
        path_file='/gws/nopw/j04/esacci_portal/stac/stac_records/post_stac/stac_record_dirs_to_post.txt'
        with open(path_file) as f:
            post_directory=[r.strip() for r in f.readlines()][int(post_directory)]

    summaries = {}
    for record in glob.glob(f'{post_directory}/**/stac*.json', recursive=True):
        summaries = post_record(record, summaries)

    if not openeo:
        return

    for href, summary in summaries.items():
        parent = client.get(href).json()

        summaries = parent.get('summaries',None)
        if summaries is None:
            summaries = {}
            summary_names = []
        else:
            summary_names = [i['name'] for i in summaries.get('eo:bands',{}) if 'name' in i]

        repost_summaries = False
        summaries_set = summaries.get('eo:bands',[])
        for name, band in summary.items():
            if name not in summary_names:
                summaries_set.append(band)
                repost_summaries=True

            # Need to be able to update the summaries.

        if parent['summaries'] is None and repost_summaries:
            parent['summaries'] = {'eo:bands':[]}

        parent['summaries']['eo:bands'] = summaries_set
        if repost_summaries:
            print('Parent:',href.split('/')[-1], client.put(href, json=parent, auth=auth))


if __name__ == "__main__":
    main()
