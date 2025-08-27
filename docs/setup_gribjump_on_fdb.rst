Generating a GribJump Index for your FDB
========================================

In order to use GribJump to efficiently extract data from an FDB without reading the whole fields, you must generate a GribJump Index. This consists of ``.gribjump`` files that are stored alongside the ``.data`` files in the FDB. There are two modes of generating the index:

- By retroactively "scanning" data that has already been archived. This uses the ``gribjump-scan`` tool
- By having FDB write the index automatically when new data is archived. This uses the ``gribjump-plugin``.


Scanning an existing FDB
------------------------

Set your ``FDB_HOME`` or ``FDB5_CONFIG_FILE``, and then run the ``gribjump-scan`` tool::

  gribjump-scan --minimum-keys="" class=od,expver=abcd,date=20250101


The tool will internally perform an fdb.list on your request and scan the matching fields.
Alternatively, you can scan individual GRIB files using ``gribjump-scan-files``::

  gribjump-scan-files /path/to/file1 /path/to/file2 ...


Activating the gribjump plugin
-------------------------------

To create new gribjump indexes on the fly, you must configure the gribjump-plugin on your FDB writer process. On this process, you must set the following:

- Tell FDB to enable the plugin: Set the environment variable ``FDB_ENABLE_GRIBJUMP=1``
- Tell GribJump where to find it's configuration: Either set ``GRIBJUMP_CONFIG_FILE=/path/to/file``, OR set ``GRIBJUMP_HOME=/path/to/home/`` in which case you must have a file called ``$GRIBJUMP_HOME/etc/gribjump/config.yaml``

If you are using RemoteFDB, the above environment must be set on the FDB Store Servers.

The ``GRIBJUMP_CONFIG_FILE`` specifies which fields to index. As an example this config::

  plugin:
    select: class=(od),stream=(enfo|oper),expver=(00..)

will index all ``class=od`` fields in ``stream=enfo`` or ``stream=oper`` with an ``expver`` starting with ``00``.
To automatically index everything written by your FDB process, a config like this can be used::

  plugin:
    select: class=(.)

Did it work?
------------
Regardless if which of the above approaches you use, should see a number of ``.gribjump`` files in your FDB, one for each scanned ``.data`` file. You can inspect the contents of the gribjump index files using ``gribjump-dump-info``::

  gribjump-dump-info /path/to/file.gribjump

which will show offsets and decoding information for each of the scanned fields. See gribjump_index_ for a breakdown of its contents.


.. _gribjump_index: gribjump_index.rst
