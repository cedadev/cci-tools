import click

from cci_tools.stac.create_aggregation import handle_collection_aggregation

def handle_quoted_csv(line):
    items = []
    opened_quote = False
    item = ''
    for char in line:

        if not opened_quote and char == ',':
            items.append(item)
            item = ''
        if char == '"':
            opened_quote = not opened_quote
        elif char != ',':
            item += char

    items.append(item)
    return items


@click.command()
@click.argument("config")

def main(config: str):
    with open(config) as f:
        content = [r.strip() for r in f.readlines()]

    for line in content:
        formatted_line = handle_quoted_csv(line)

        moles_uuid = formatted_line[1]
        drs_id = formatted_line[2].lower()
        if drs_id == 'n/a':
            drs_id = None

        endpoint = formatted_line[3]#.replace('https://dap.ceda.ac.uk','')

        handle_collection_aggregation(endpoint, moles_uuid, drs_id, engine='kerchunk')

if __name__ == '__main__':
    main()