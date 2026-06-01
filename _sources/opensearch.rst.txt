==================
Opensearch & MOLES
==================

opensearch-collections
----------------------

The ``opensearch-collections`` Elasticsearch index is effectively the source of truth for the published CCI datasets, as it is the backend resource used by the opensearch service, which in turn is used by the CCI Open Data Portal to display datasets. Collection records are regenerated nightly by aggregating results from ``opensearch-files`` which is where the tagged data file records are stored.

Each document in the collections index contains the tags for a particular Moles Dataset (including the ECV/project) as well as the DRS' covered by that dataset. It is therefore possible to obtain a full picture of the ECV-MOLES-DRS landscape by querying the collections index.

Opensearch service
------------------

The opensearch service supports multiple query types to get collection-level and file-level information based on the tagged file index ``opensearch-files`` and the ``opensearch-collections`` index described above. The service is a django application supporting complex elasticsearch queries made via URL queries to the front-end service. Queries from elasticsearch (including aggregations) are interpreted and displayed in JSON/XML format by the opensearch service, where download/ftp/manifest links may have been collected from multiple sources.

MOLES
-----

The MOLES catalog is the CEDA source of truth for published datasets, where CCI datasets must first be published and made available as MOLES records before they become visible to the CCI ODP. Once a MOLES record has been generated, listed as 'Citable/Published' and has the ``ESACCI`` discovery keyword added it becomes visible to the Collection Regen nightly build, where a new entry will be added for the new MOLES ID. This will then be consumed by the ODP and should become visible the following day. Please check the aggregated tags in the ``opensearch-collections`` index for a given dataset at the first sign of missing or incorrect data in the ODP.

All the above provide sources of information for generating the STAC items/collections, discussed in the next section.