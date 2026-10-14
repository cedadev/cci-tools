from cci_tools.core.utils import get_moles_data, get_providers, order_properties

MAPPINGS = {
    'project':        'cci:project',
    'collections':    'cci:collections',
    'drsId':          'cci:drsId',
    'ecv':            'cci:ecv',
    'dataType':       'cci:dataType',
    'sensor':         'cci:sensor',
    'platforms':      'cci:platform',
    'platformGroup':  'cci:platformGroup',
    'frequency':      'cci:frequency',
    'productString':  'cci:product',
    'productVersion': 'cci:productVersion',
    'institute':      'cci:institute',

    'processingLevel':'processing:level',

    'datasetId':      'ceda:uuid',
    'aggregation':    'ceda:aggregation',
    'opensearch_url': 'ceda:opensearch_url',
}

def add_if_property(properties: dict, label: str, maplabel: str) -> dict:

    value = properties.pop(label, None)
    if value:
        properties[maplabel] = value
    return properties

def upgrade_record(stac_item: dict, uuid: str, moles_info: dict, providers: dict) -> dict:

    # Handle Extensions
    exts = [
        "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
    ]

    for prop in stac_item['properties']:
        if 'proj:' in prop:
            exts.append("https://stac-extensions.github.io/projection/v1.1.0/schema.json")

    # Handle Unique Mappings
    version = stac_item['properties'].pop('version',None)
    if version:
        if isinstance(version,list):
            version = version[0]
        numericVersion = version
        if version.startswith('v'):
            numericVersion = version[1:]
        
        stac_item['properties']['cci:productVersion'] = version
        stac_item['properties']['processing:version'] = numericVersion

        exts.append("https://stac-extensions.github.io/processing/v1.2.0/schema.json")

    properties = stac_item['properties']

    # Handle basic mappings
    for label, mapping in MAPPINGS.items():
        properties = add_if_property(properties, label, mapping)

    moles_data = None
    if properties.get('ceda:uuid'):
        if moles_info is None:
            uuid = properties.get('ceda:uuid')
            moles_data = get_moles_data(uuid)
        else:
            moles_data = moles_info

        properties['cci:esa_url'] = f"https://climate.esa.int/en/catalogue/{uuid}"
        properties['ceda:opensearch_url'] =  f"https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={uuid}"

        properties['title'] = moles_data['title']
        properties["description"] = moles_data['abstract'] + \
            f'\r\n\n\n See CEDA Catalogue Record for citation details: https://catalogue.ceda.ac.uk/uuid/{uuid}'

        # Via Link
        add_via = True
        for link in stac_item['links']:
            if link['rel'] == 'via':
                add_via = False

        if add_via:
            stac_item['links'].append({
                "rel": "via",
                "type": "text/html",
                "href": f"https://catalogue.ceda.ac.uk/uuid/{uuid}"
            })

    if properties.get('cci:institute'):
        if providers is None:
            providers = get_providers(properties['cci:institute'])
        properties['providers'] = providers

    stac_item['properties'] = order_properties(properties)
    stac_item['stac_extensions'] = exts

    return stac_item, uuid, moles_data, providers