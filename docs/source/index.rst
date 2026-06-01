.. ceda-datapoint documentation master file, created by
   sphinx-quickstart on Tue Oct 15 15:34:57 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

CCI STAC Tools
==============

This is the start of the CCI STAC Tools official documentation for CEDA CCI Staff Members.

The CCI STAC Tools repo (``cci-tools``) contains the functionality to create/update/delete STAC items/collections in the CCI STAC index. This repo is necessary over more general tools like the CEDA ``stac-generator`` because of the integration of templated items from the Opensearch index as well as the nesting of different collection levels.

The CCI STAC Index is laid out as follows:

- CCI (top level collection)
   - Project/ECV (Essential Climate Variable)
      - UUID/Dataset (Moles Catalog Record)
         - DRS-level collections
            - File-based Items in each collection.

Install the CCI STAC tools by running ``pip install -e .`` after cloning this repository.

.. note::

   The CCI tools requires a virtual environment using python 3.11 or later. Be sure you have a compatible python environment activated before installing the tools.

.. toctree::
   :maxdepth: 1
   :caption: Sections:
   Opensearch & MOLES <opensearch>
   CCI STAC Collections Explained <collections>
   Using The Tools <tools>
   Source Code <source>
   The STAC Tools Shell <shell>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`