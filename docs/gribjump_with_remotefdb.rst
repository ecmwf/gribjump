GribJump with RemoteFDB
=======================

See also: gribjump_server_

In a RemoteFDB deployment there are at least two FDB servers:

- A single Catalogue Server, hosting the FDB index.
- One or more FDB Store Servers, responsible for storing the actual data.

When GribJump is used with RemoteFDB, the GribJump Index files (``.gribjump``) are stored beside the ``.data`` files on the FDB stores. This page details the configuration and usage of GribJump in a RemoteFDB environment.

Generating the Index
--------------------
To automatically index data on archival to the FDB, the plugin must be enabled on the FDB Store Server.
See setup_plugin_ for details.

Extracting Data
---------------
To read data from each of the stores, a GribJump Server is deployed alongside each of the FDB Store servers.

The client needs to have the following:

- A suitable RemoteFDB configuration file, capable of listing the FDB Catalogue.
- A GribJump configuration file, mapping the FDB Store Servers to their respective GribJump Servers.


Example RemoteFDB client configuration::

    type: remote
    store: remote
    engine: remote
    host: catalogue_host0
    port: 9000


Example GribJump client configuration::

    forwardExtraction: true
    servermap:
    - fdb: 'store_host1:9000'
      gribjump: 'store_host1:9001'
    - fdb: 'store_host2:9000'
      gribjump: 'store_host2:9001'
    threads: 2  

Example GribJump server configuration::

    forwardExtraction: true
    servermap:
    - fdb: 'store_host1:9000'
      gribjump: 'store_host1:9001'
    - fdb: 'store_host2:9000'
      gribjump: 'store_host2:9001'
    threads: 2

The ``forwardExtraction`` flag configures the client to forward extraction requests to the appropriate GribJump server based on the ``servermap`` configuration.
The ``servermap`` configuration maps each FDB Store Server to its corresponding GribJump Server.
The ``threads`` parameter specifies how many tasks GribJump will perform in parallel. When forwarding extraction requests, this corresponds to the number of GribJump servers that will be contacted in parallel.

A simple GribJump server config file might look like::

    server:
      port: 9002
    threads: 32
    plugin:
      select: expver=(.)


This will configure the GribJump server to listen on port 9002, use 32 threads for extraction requests. The GribJump Plugin on the FDB Store Server will create an index file for every GRIB message it writes.

.. _setup_plugin: docs/setup_gribjump_on_fdb.rst
.. _gribjump_server: docs/gribjump_server.rst