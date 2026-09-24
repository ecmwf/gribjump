
GribJump Configuration
======================


Per-object configuration (C++)
------------------------------

Each ``GribJump(config)`` owns a snapshot of its configuration. Constructing a
second object, or modifying the original ``Config``, does not change an existing
object. Different objects may be used concurrently with different settings.
Configuration is passed explicitly to engines and worker tasks, not installed in
a global singleton.

.. code-block:: cpp

    gribjump::Config checked;
    checked.set("ignoreGridHash", false);
    checked.set("cache.directory", "/existing/cache/checked");
    gribjump::GribJump a(checked);

    gribjump::Config unchecked;
    unchecked.set("ignoreGridHash", true);
    unchecked.set("cache.enabled", false);
    gribjump::GribJump b(unchecked);

The following settings are per-object:

* ``type``, ``uri``;
* ``ignoreGridHash``, ``ignoreYearMonth`` (default ``true``), ``allowMissing``;
* ``inefficientExtraction``, ``forwardExtraction``, ``forwardScan``, ``servermap``;
* ``cache.enabled``, ``cache.directory``, ``cache.shadowfdb``, ``cache.size``, ``cache.lazy``;
* ``scanCorrupted`` (default ``false``).

Existing environment/eckit resource overrides retain precedence over supplied
values. Per-object overrides are resolved at construction, not at first use by a
worker. Set environment variables before starting threads; do not use environment
changes to configure individual objects.

``GribJump()`` uses the process-default configuration loaded from
``GRIBJUMP_CONFIG_FILE`` (or the normal default file). ``GribJump(config)`` uses
the supplied configuration and built-in defaults for omitted options; it does
not merge with or replace the process-default file. ``Config(path)`` loads YAML
without changing logging or any other process state. Programmatic ``set()`` calls,
including setting ``servermap``, are supported before constructing the object.

The worker pool (``threads``), server listener (``server.port``), logging
(``logging``), and FDB archive plugin (``plugin`` and its enable/disable resources)
remain process-wide. ``requestParsing`` also remains process-wide because C and
Python request objects are constructed independently of a GribJump handle.
Set these via the process-default file or existing resource/environment controls.
An explicit ``GribJump(config)`` rejects the top-level keys ``threads``, ``server``,
``logging``, ``plugin``, and ``requestParsing`` with an error rather than silently
ignoring them. The new config constructor is a C++ API; it does not add C or Python
configuration constructors.

Local objects have independent in-memory caches and listing policies. Disk indexes
are still shared if objects select the same directory or shadow the same GRIB
files; concurrent writes to the same index are not coordinated across objects.
Use distinct directories when independent disk caches are needed. With
``cache.enabled=false``, no memory/disk index is read or written. Extraction then
requires ``cache.lazy=true``; scanning still reads the GRIB data but does not
persist an index. ``cache.size`` must be positive.

Remote clients retain their own endpoint, but extraction/cache policies on a
remote server are governed by that server's configuration: this interface does
not transmit client configuration over the protocol.

Configuration File
------------------
The following options can be added to the ``GRIBJUMP_CONFIG_FILE``:

- ``type``: Whether GribJump will work locally or forward work to a remote server. Allowed values are ``local`` and ``remote``, default is ``local``.
- ``uri``: If ``type=remote``, this specifies the ``host:port`` of a gribjump-server the client should forward to.
- ``server`` : Configuration options used only by the ``gribjump-server``:
    - ``server.port``: Port the server listens on for incoming requests.
- ``threads``: Number of worker threads for carring out extraction tasks. Default is 1.
- ``ignoreYearMonth``: Ignore year/month aliases when date is present. Default is ``true``.
- ``scanCorrupted``: Attempt to recover offsets from corrupted GRIB files. Default is ``false``.
- ``ignoreGridHash``: If ``true``, GribJump will not verify against a user-provided grid hash of GRIB files before extracting data. Default is ``false``.
- ``cache``: Configuration options for the GribJump Index:
    - ``cache.enabled``: Whether to look at the GribJump Index at all. Default is ``true``.
    - ``cache.shadowfdb``: If ``true``, the index files will be stored in the same directory as data files. Default is ``true``.
    - ``cache.directory``: The directory where the index will be stored, instead of shadowing an FDB.
    - ``cache.lazy``: If ``false``, extracting from a GRIB file without a corresponding index file is considered an error. If ``true``, the metadata will be lazily extracted if the index file is missing. Default is ``true``.
- ``plugin``: Configuration options for using GribJump as a plugin to FDB, which generates a GribJump index on the fly for ``fdb.archive()``.
    - ``plugin.select``: Defines regex for selecting which FDB keys to generate a GribJump index for. If unset, no GribJump indexes will be generated. Example: ``select: date=(20*),stream=(oper|test)``.

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


Other Config Options
---------------------
All of GribJump's configuration options are documented in the C++ API reference, which is generated from the source code. The following Doxygen class documents the configuration options available in GribJump:

.. doxygenclass:: gribjump::ConfigOptions
   :members:
