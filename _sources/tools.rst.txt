========================
Using the CCI STAC Tools
========================

.. note::

    For all the commands below, you can access further help using the ``--help`` flag, to see the options available for each.

Creating Collections
--------------------

Create a new collection (ECV/MOLES/DRS level) using the command below:

```
$ new_collection <parent> <child>
```

Where the ``parent`` is the collection above the one you are trying to create (i.e for a new MOLES entry, the parent is the ECV), and the ``child`` is the name of the new collection. Note that this tool will create all sub-collections of your new collection that exist, by checking the ``opensearch-collections`` elasticsearch index, which is effectively a source of truth for published CCI datasets.

Update Collections
------------------

It is also possible to make a manual adjustment to CCI collections, where a specific field needs some correction. The index can be downloaded via wget/curl from the STAC index from ``STAC_API/collections/COLLECTION``. Save the JSON file locally and make any changes necessary. The collection can then be reuploaded using:

```
$ update_collection path/to/file <parent>
```

Where the parent is the collection above the one being adjusted. Generally this should not be required as the parent remains the same. If the parent is different as part of the adjustment you will also need to ``delete`` the existing collection, or alternatively reupload this collection with local changes then ``migrate`` the collection to a new parent (see below).

Delete Collections/Items
------------------------

Deleting collections or items can be done via the ``delete_collections`` command, where any deletion requires the ``-r`` flag to actually REALLY delete anything. You may also choose to:
- keep collections (``--keep_collections``) and only delete items
- delete item aggregations (``--item_aggregations``) where i.e kerchunk/zarr items will not be deleted without this option.
- delete the specified collection only (``--top_only``) but leave the collections underneath orphaned (not recommended)
- delete the lowest collections (DRS') only (``--lowest_only``) for a given MOLES/ECV collection.
- delete collections at a certain depth with (``--delete depth <INT>``)

For more complex deletions where deleting each item/collection is not feasible individually, custom scripts may be required to handle this case. See the section on the STAC shell which gives tips on how to build these applications.

