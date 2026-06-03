================
OpenEO Explained
================

Background
----------

OpenEO provides an API that allows users to connect to EO data/cloud backends in a simple and unified way. The API and process specifications are open-source and client/server implementations are also available openly. The OpenEO specification is used in production by so-called OpenEO cloud platforms like "OpenEO Platform" and the Copernicus Data Space Ecosystem (CDSE), where the cloud platforms provide an interface for the OpenEO API as well as a full cloud processing backend. 

The CCI Implementation is limited to local processing only, where the datasets are available in OpenEO compliant STAC format but no processing backend is directly available for CCI data. A fully developed processing backend is essential for using some of the core cloud-processing tools the OpenEO provides, including pre-defined or user-defined processes, interconnected tasks and so on. The idea is to effectively "order" the cloud processing tasks to act on specific datasets, by configuring requests via the API. To learn more about this see the main `[OpenEO documentation](https://openeo.org/documentation/1.0/udfs.html#users)_`.

.. note::

    The CCI OpenEO datasets are available for local-only processing as a processing backend has not been implemented. This is mostly due to resource requirements on external or CEDA-maintained cloud. Instead, cci datasets can be accessed via the openeo API client, but require local processing of data. The data is still cloud-based, using a combination of COG, Zarr and Virtual Zarr datasets, but all processing is done locally.

Creating OpenEO datasets
------------------------

This can be accomplished for existing static aggregations (Kerchunk/Zarr) using the following:

.. code::

    $ create_openeo <path/to/file>

Additional options are available as follows:
- ``--did`` - Dataset (DRS) ID to name this dataset (may be different to simply the name of the kerchunk/zarr file)
- ``--uuid`` - Moles UUID to add to the metadata of the collection
- ``--ecv`` - ECV to which this OpenEO dataset belongs.

This will create an OpenEO compliant collection and adjoining item representing the static aggregation file. This will not be attached to the ``cci_openeo`` collection automatically as it is a good idea to test that this collection is valid and works with the OpenEO local client - at which time it may be ``migrated`` (see other tools) under the ``cci_openeo`` collection

Validating the collection
-------------------------

The following code snippet shows an example use case for the kerchunk/zarr OpenEO dataset:

.. code::

    from openeo.local import LocalConnection

    # Setup the local connection
    connection = LocalConnection('./')

    url = 'https://api.stac.164.30.69.113.nip.io/collections/<YOUR_COLLECTION_DID>'.lower()

    # You can select one or multiple bands/variables to load but at least one must be provided.
    datacube = connection.load_stac(url=url, bands=['<variable_from_your_dataset>'])
    dataset = datacube.execute()

    print(dataset)

You may also want to run some kind of calculation example on the data to check there are no error messages immediately with the data.
