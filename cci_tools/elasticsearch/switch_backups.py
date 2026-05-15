from cci_tools.core.utils import es_client
from elasticsearch import NotFoundError
import click
import os
import glob


def make_backup_changes(record, use_backup, use_alt_opendap):

    record["use_backup"] = use_backup
    record["use_alt_opendap"] = use_alt_opendap
    return record


def pull_from_fix_index(filepath):

    return es_client.get(index="opensearch-backup-check", id=filepath)["_source"]


def push_to_fix_index(filepath, doc):

    print(
        filepath,
        es_client.update(
            index="opensearch-backup-check",
            id=filepath,
            body={"doc": doc, "doc_as_upsert": True},
        )["_shards"]["successful"]
        > 0,
    )


@click.command
@click.argument("files")
@click.option("--use_backup", "use_backup", required=False, is_flag=True)
@click.option("--use_alt_opendap", "use_alt_opendap", required=False, is_flag=True)
def main(files, use_backup, use_alt_opendap):

    with open(files) as f:
        filesets = [r.strip() for r in f.readlines()]

    fileset = []
    for d in filesets:
        if os.path.isdir(d):
            df = glob.glob(f"{d}/**/*.*", recursive=True)
            fileset += df
        else:
            fileset += [d]

    for file in fileset:
        if os.path.isdir(file):
            continue
        try:
            record = pull_from_fix_index(file)
        except NotFoundError:
            continue
        record = make_backup_changes(
            record, use_backup=use_backup, use_alt_opendap=use_alt_opendap
        )
        push_to_fix_index(file, record)


if __name__ == "__main__":

    main()
