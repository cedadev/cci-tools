====================
CCI STAC Collections
====================

The CCI STAC Index is laid out as follows:

- CCI (top level collection)
   - Project/ECV (Essential Climate Variable)
      - UUID/Dataset (Moles Catalog Record)
         - DRS-level collections
            - File-based Items in each collection.

Additionally, a separate ``cci_openeo`` collection exists to store OpenEO compliant collections separate to normal STAC records for ease of use. This is mostly due to restrictions on non-data items which are present in the plain STAC collections but are not allowed in OpenEO - the set of items in a collection must provide a full regular spatio-temporal description of the dataset.