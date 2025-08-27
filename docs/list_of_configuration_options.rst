
GribJump Configuration
======================

Configuration File
------------------
The following options can be added to the ``GRIBJUMP_CONFIG_FILE``:

- ``type``: Whether GribJump will work locally or forward work to a remote server. Allowed values are ``local`` and ``remote``, default is ``local``.
- ``uri``: If ``type=remote``, this specifies the ``host:port`` of a gribjump-server the client should forward to.
- ``server`` : Configuration options used only by the ``gribjump-server``:
    - ``server.port``: Port the server listens on for incoming requests.
- ``threads``: Number of worker threads for carring out extraction tasks. Default is 1.
- ``ignoreGridHash``: If ``true``, GribJump will not verify against a user-provided grid hash of GRIB files before extracting data. Default is ``false``.
- ``cache``: Configuration options for the GribJump Index:
    - ``cache.enable``: Whether to look at the GribJump Index at all. Default is ``true``.
    - ``cache.shadowfdb``: If ``true``, the index files will be stored in the same directory as data files. Default is ``true``.
    - ``cache.directory``: The directory where the index will be stored, instead of shadowing an FDB.
    - ``cache.lazy``: If ``false``, extracting from a GRIB file without a corresponding index file is considered an error. If ``true``, the metadata will be lazily extracted if the index file is missing. Default is ``true``.
- ``plugin``: Configuration options for using GribJump as a plugin to FDB, which generates a gribjump index on the fly for ``fdb.archive()``.
    - ``plugin.select``: Defines regex for selecting which FDB keys to generate a gribjump index for. If unset, no gribjump indexes will be generated. Example: ``select: date=(20*),stream=(oper|test)``.

Environment variables
---------------------
Several environment variables can be used to configure GribJump.
Some of these overlap with the configuration file options. In these cases, the environment variable takes precedence over the configuration file option.
These are:

- ``GRIBJUMP_CONFIG_FILE``: Path to the GribJump configuration file.
- ``GRIBJUMP_DEBUG``: Enable verbose debug logging for GribJump.
- ``FDB_ENABLE_GRIBJUMP``: Enable GribJump as a plugin to FDB. Must be set on the process calling ``fdb.archive()``.
- ``GRIBJUMP_THREADS``: Overrides the ``threads`` option in the configuration file.
- ``GRIBJUMP_SERVER_PORT``: Overrides the ``server.port`` option in the configuration file.

.. this list is incomplete.