#!/usr/bin/env python
__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

def openeo_collection(
        id: str,
        description: str,
        bbox: list,
        start_datetime: str,
        end_datetime: str,
        title: str = None,
        moles_uuid: str = None,
        thumbnail: str = "https://brand.esa.int/files/2020/05/ESA_logo_2020_Deep-1024x643.jpg",
        keywords: list = None,
        summary_bands: dict = None,
        license: str = 'other'
):
    
    keywords = keywords or ['ESACCI']

    collection = {
        'id': id,
        'description': description,
        "stac_version": "1.1.0",
        "links": [
            {
            "rel": "items",
            "type": "application/geo+json",
            "href": f"https://api.stac.164.30.69.113.nip.io/collections/{id}/items"
            },
            {
            "rel": "parent",
            "type": "application/json",
            "href": "https://api.stac.164.30.69.113.nip.io/"
            },
            {
            "rel": "queryables",
            "type": "application/json",
            "href": f"https://api.stac.164.30.69.113.nip.io/collections/{id}/queryables"
            },
            {
            "rel": "root",
            "type": "application/json",
            "href": "https://api.stac.164.30.69.113.nip.io/"
            },
            {
            "rel": "self",
            "type": "application/json",
            "href": f"https://api.stac.164.30.69.113.nip.io/collections/{id}"
            },
            {
            "href": "https://catalogue.ceda.ac.uk/uuid/b1bd715112ca43ab948226d11d72b85e",
            "rel": "ceda_catalogue",
            "type": "text/html"
            },
            {
            "href": "https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier=b1bd715112ca43ab948226d11d72b85e",
            "rel": "opensearch",
            "type": "text/html"
            }
        ],
        "stac_extensions": [
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/classification/v1.0.0/schema.json",
            "https://stac-extensions.github.io/eo/v1.1.0/schema.json"
        ],
        "title": title or id,
        "type": "Collection",
        "assets": {
            "thumbnail": {
                "href": thumbnail,
                "type": f"image/{thumbnail.split('.')[-1]}",
                "roles": [
                    "thumbnail"
                ]
            }
        },
        "license": license,
        "extent": {
            "spatial": {
                "bbox": bbox
            },
            "temporal": {
                "interval": [
                    [
                    start_datetime,
                    end_datetime
                    ]
                ]
            }
        },
        "keywords": keywords,
        "providers": [
            {
            "name": "Centre for Environmental Data Analysis (CEDA)",
            "roles": [
                "host"
            ],
            "url": "https://catalogue.ceda.ac.uk"
            },
            {
            "name": "ESA Open Data Portal (ODP)",
            "roles": [
                "host"
            ],
            "url": "https://climate.esa.int/data/"
            }
        ],
        "summaries": {
            "eo:bands": [
                {
                    "name": b,
                    "common_name": binfo.get('long_name'),
                    "description": binfo.get('description'),
                } for b, binfo in summary_bands.items()
            ]
        }
    }
    
    if moles_uuid is not None:
        collection['links'].append({
            "href": f"https://catalogue.ceda.ac.uk/uuid/{moles_uuid}",
            "rel": "ceda_catalogue",
            "type": "text/html"
        })
        collection['links'].append(
        {
            "href": f"https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier={moles_uuid}",
            "rel": "opensearch",
            "type": "text/html"
        })

        collection['assets']["CEDA Catalogue Record"] = {
            "href": f"https://catalogue.ceda.ac.uk/uuid/{moles_uuid}",
            "type": "application/html",
            "roles": [
                "documentation"
            ]
        }

    return collection