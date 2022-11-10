=======================
Google Sheets connector
=======================

.. raw:: html

  <img src="../_static/img/google-sheets.png" class="connector-logo">

The Google Sheets connector allows reading and writing `Google Sheets <https://www.google.com/sheets/about/>`_ spreadsheets as tables in Trino.

Configuration
-------------

Create ``etc/catalog/sheets.properties``
to mount the Google Sheets connector as the ``sheets`` catalog,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=gsheets
    credentials-path=/path/to/google-sheets-credentials.json
    metadata-sheet-id=exampleId

Configuration properties
------------------------

The following configuration properties are available:

=================================== =====================================================================
Property name                       Description
=================================== =====================================================================
``credentials-path``                Path to the Google API JSON key file
``metadata-sheet-id``               Sheet ID of the spreadsheet, that contains the table mapping
``sheets-data-max-cache-size``      Maximum number of spreadsheets to cache, defaults to ``1000``
``sheets-data-expire-after-write``  How long to cache spreadsheet data or metadata, defaults to ``5m``
=================================== =====================================================================

Credentials
-----------

The connector requires credentials in order to access the Google Sheets API.

1. Open the `Google Sheets API <https://console.developers.google.com/apis/library/sheets.googleapis.com>`_
   page and click the *Enable* button. This takes you to the API manager page.

2. Select a project using the drop down menu at the top of the page.
   Create a new project, if you do not already have one.

3. Choose *Credentials* in the left panel.

4. Click *Manage service accounts*, then create a service account for the connector.
   On the *Create key* step, create and download a key in JSON format.

The key file needs to be available on the Trino coordinator and workers.
Set the ``credentials-path`` configuration property to point to this file.
The exact name of the file does not matter -- it can be named anything.

Metadata sheet
--------------

The metadata sheet is used to map table names to sheet IDs.
The first row must be a header row containing the following columns in this order (the actual header value does not matter):

================ ============ =================================================
Column           Mandatory    Description
================ ============ =================================================
``Table Name``   yes          Name of the table as it will show up in Trino
``Sheet ID``     yes          Sheet ID of the Google sheet. See `Sheet ID`_
``Owner``        no           Documentation purpose only
``Notes``        no           Documentation purpose only
``Column types`` no           Specify types of the columns. See `Column types`_
================ ============ =================================================

A sample metadata table could look like

============= ========================================================== ======== =========================================================== ==================================
Table Name    Sheet ID                                                   Owner    Notes                                                       Colum types
============= ========================================================== ======== =========================================================== ==================================
customers     1234                                                                Will fetch 10k lines from first tab
sales_data    1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM#Sales data    Bob      Sales data of the last year residing in tab "Sales data"    department=varchar,sales=bigint
============= ========================================================== ======== =========================================================== ==================================

See this `example sheet <https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM>`_
as a reference.

The metadata sheet must be shared with the service account user,
the one for which the key credentials file was created. Click the *Share*
button to share the sheet with the email address of the service account.

Set the ``metadata-sheet-id`` configuration property to the ID of this sheet.

Sheet ID
^^^^^^^^
Sheet ID of the Google sheet.
You can extract this from the URL when editing a sheet in a browser.
Given the URL https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM/edit#gid=0, the sheet id is ``1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM``.

As a default, this connector will fetch 10,000 rows from the first tab in the sheet.
If you need to read more rows or rows from a different tab, you can prepend that tab after the sheet ID separated with ``#``.
This could for example look like ``1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM#Customer Orders``.

Column types
^^^^^^^^^^^^
Specify types of the columns.
The format is ``mycol1=varchar,mycol2=bigint,mycol3=bigint``.
The columns names need to be called the same way as they show up in the Trino table, which is lowercase.
If no type is specified for a column or no column types are specified at all, ``varchar`` will be used.
The supported column types are documented in `Type mapping`_.

Querying sheets
---------------

The service account user must have access to the sheet containing the data in order for Trino to query it.
Click the *Share* button to share the sheet with the email address of the service account.

Writing to sheets
-----------------
The same way sheets can be queried, they can also be written by appending data to existing sheets.
In this case the service account user must also have **write** access to the sheet.

After data is written to a table, the table contents are removed from the cache described in `Caching`_.
If the table is accessed imitatively after the write, querying the Google Sheets API may not reflect the change yet.
In that case the old version of the table will be read and cached for the configured amount of time.
So it might take some time for the written changes to propagate properly.

Caching
-------

The Google Sheets API has `usage limits <https://developers.google.com/sheets/api/limits>`_,
that may impact the usage of this connector. Increasing the cache duration and/or size
may prevent the limit from being reached. Running queries on the ``information_schema.columns``
table without a schema and table name filter may lead to hitting the limit, as this requires
fetching the sheet data for every table, unless it is already cached.

Type mapping
------------

Because Trino and Google Sheets each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading data.

The section `Column types`_ describes how to specify the types for table columns in the metadata table.

Google Sheets type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Google Sheets types to the corresponding Trino types using the provided column type.
The possible types are listed in the following table.

.. list-table:: Supported sheet column types
  :widths: 40, 20
  :header-rows: 1

  * - Sheet column type
    - Trino type
  * - <not specified>
    - ``VARCHAR``
  * - ``varchar``
    - ``VARCHAR``
  * - ``bigint``
    - ``BIGINT``

No other types are supported.

When writing data the correct types of the columns will be checked and all the rows will be appended as text to the sheet.

.. _google-sheets-sql-support:

SQL support
-----------

In addition to the :ref:`globally available <sql-globally-available>` and :ref:`read operation <sql-read-operations>` statements,
the connector supports the following features:

* :doc:`/sql/insert`
