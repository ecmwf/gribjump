GribJump Configuration
======================

Configuration scopes
--------------------

GribJump has two categories of settings:

* **Per-object options** control a particular client's extraction, listing and
  forwarding behaviour. Each ``GribJump`` owns an immutable snapshot. Objects with
  different options can be used concurrently.
* **Process-wide options** configure the shared cache, worker pool, server
  listener, logging, FDB archive plugin and standalone request parsing. All
  objects in a process use the same values.

The sections below list the options in each category. Names containing dots
refer to nested YAML keys, or to keys passed to ``Config::set()``.

Per-object options
------------------

Implementation and endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``type``: ``local`` (default) or ``remote``.
* ``uri``: ``host:port`` of the GribJump server; required for ``type: remote``.

Extraction, listing and scanning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``ignoreGridHash``: Skip checking the requested grid hash during extraction.
  Default: ``false``. Environment override: ``GRIBJUMP_IGNORE_GRID``.
* ``ignoreYearMonth``: Ignore year/month aliases when date is present.
  Default: ``true``. Environment override: ``GRIBJUMP_IGNORE_YEARMONTH``.
* ``allowMissing``: Allow fields to be missing from FDB listing results.
  Default: ``false``. Environment/resource overrides: ``GRIBJUMP_ALLOW_MISSING``
  and ``allowMissing``.
* ``scanCorrupted``: Attempt to recover offsets when scanning corrupted GRIB
  files. Default: ``false``. Environment override: ``GRIBJUMP_SCAN_CORRUPTED``.

Forwarding
~~~~~~~~~~

* ``inefficientExtraction``: Extract remote FDB data by reading complete messages.
  Default: ``false``.
* ``forwardExtraction``: Forward extraction work to GribJump servers associated
  with remote FDB stores. Default: ``false``.
* ``forwardScan``: Forward scan work to those servers. Default: ``false``.
* ``servermap``: List of mappings from FDB endpoints to GribJump endpoints.
  Default: empty. For example:

  .. code-block:: yaml

      servermap:
        - fdb: store.example:9000
          gribjump: store.example:9777

Remote servers apply their own extraction and cache settings. Client options
are not transmitted as configuration over the remote protocol.

Process-wide options
--------------------

Shared cache
~~~~~~~~~~~~

The cache is shared by all local objects, the archive plugin and standalone
cache tools. Both its in-memory entries and disk-index policy are process-wide.

* ``cache.enabled``: Enable the memory cache and disk indexes. Default: ``true``.
  When disabled, extraction obtains metadata directly from GRIB files and
  requires ``cache.lazy: true``. Scanning still reads GRIB metadata, but leaves
  disk indexes untouched.
* ``cache.directory``: Existing directory for disk indexes. Default: empty.
* ``cache.shadowfdb``: Store indexes next to their GRIB files, taking precedence
  over ``cache.directory``. Defaults to ``true`` when ``cache.directory`` is empty,
  otherwise ``false``.
* ``cache.size``: Positive capacity of the in-memory LRU cache. Default: ``1024``.
  Resource override: ``gribjumpCacheSize``.
* ``cache.lazy``: Generate missing metadata from GRIB files during extraction.
  With ``false``, a missing cache entry is an error. Default: ``true``.
  Resource override: ``gribjumpLazyInfo``.

Concurrent writes to the same index from different processes are not coordinated.

Worker pool and server listener
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``threads``: Positive number of worker threads in the shared pool. Default: ``1``.
  Environment/resource overrides: ``GRIBJUMP_THREADS`` and ``gribjumpThreads``.
  Threads are started on first use of the pool.
* ``server.port``: Listening port for ``gribjump-server``. Default: ``9777``.
  Environment override: ``GRIBJUMP_SERVER_PORT``.

Logging, archive plugin and request parsing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``logging``: Map of log-channel aliases to ``debug``, ``info``, ``error`` or
  ``default``. For example, ``logging: {progress: info}``.
* ``plugin.select``: Selection expression for the FDB archive plugin, for example
  ``date=(20*),stream=(oper|test)``. Default: empty (select no fields).
* ``requestParsing``: Parse/expand request strings when constructing standalone
  C/Python request objects. Default: ``false``. Environment override:
  ``GRIBJUMP_REQUEST_PARSING``. These requests are created independently of a
  GribJump object, so this setting is process-wide.

The plugin enable/disable controls are environment/eckit resources only:

* ``FDB_ENABLE_GRIBJUMP`` / ``fdbEnableGribjump``: Enable the archive plugin.
  Default: ``false``.
* ``FDB_DISABLE_GRIBJUMP`` / ``fdbDisableGribjump``: Disable the archive plugin,
  taking precedence over the enable control. Default: ``false``.

``GRIBJUMP_DEBUG`` enables verbose library logging through eckit's library debug
control.

Configuration sources and precedence
-----------------------------------

The library loads its default configuration from ``GRIBJUMP_CONFIG_FILE``, or
``~gribjump/etc/gribjump/config.yaml`` if present, otherwise built-in defaults.

* ``GribJump()`` uses the per-object options from this default configuration.
* ``GribJump(config)`` uses the supplied per-object options, with built-in defaults
  for omitted per-object keys.
* On first process configuration, explicitly supplied process keys override the
  corresponding file defaults. Omitted process keys retain their file defaults.
* Existing environment/eckit resource overrides take precedence over configuration
  values. They are resolved when the corresponding options are initialized.

Set environment/resource controls before using GribJump or starting threads.
``Config(path)`` loads YAML without side effects; programmatic ``set()`` calls,
including setting ``servermap``, are supported before constructing the object.
Later edits to a ``Config`` leave constructed objects unchanged.

One-time process configuration
------------------------------

The first GribJump constructor fixes the process-wide settings. They can also be
fixed earlier by a process service, such as the FDB plugin or cache, or by an
explicit call to ``ProcessOptions::configure(config)`` or ``ProcessOptions::get()``.
The library being loaded alone does not fix these settings.

Later constructors and ``configure()`` calls accept matching process settings.
A conflicting setting raises ``eckit::BadValue`` identifying the option. Omitted
keys preserve the established settings. Compatibility is checked after applying
environment/resource overrides. Configuration is synchronized, so concurrent
initializers cannot install conflicting values.

This fixes **all process-wide settings together**, even if a service such as the
worker pool has not yet been created. Configure them at application startup,
before constructing objects or using the FDB plugin. Services remain lazily
created. Reading ``ConfigOptions::defaultOptions()`` alone only reads the default
per-object options.

C++ example
-----------

.. code-block:: cpp

    gribjump::Config checked;
    checked.set("threads", 4);                    // establishes the shared pool size
    checked.set("cache.directory", "/existing/cache");
    checked.set("ignoreGridHash", false);         // applies to a only
    gribjump::GribJump a(checked);

    gribjump::Config unchecked;
    unchecked.set("ignoreGridHash", true);        // applies to b only
    gribjump::GribJump b(unchecked);              // shares a's process settings

    gribjump::Config compatible;
    compatible.set("threads", 4);                // repeating a setting is allowed
    gribjump::GribJump c(compatible);

Alternatively, establish process settings before creating clients:

.. code-block:: cpp

    gribjump::ProcessOptions::configure(processConfig);
    gribjump::GribJump a(clientConfigA);
    gribjump::GribJump b(clientConfigB);

These configuration constructors are currently C++ APIs. The C and Python
interfaces use the process/file defaults.

API reference
-------------

.. doxygenclass:: gribjump::ConfigOptions
   :members:

.. doxygenclass:: gribjump::ProcessOptions
   :members:
