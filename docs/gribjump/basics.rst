GribJump Basics
===============

GribJump extracts selected values from GRIB fields without decoding every value
in the field. This tutorial starts with files on disk, then uses FDB to locate
fields by their MARS metadata. Select a language tab to see the equivalent
Python or C++ example.

The Python examples use :doc:`pygribjump <../pygribjump/index>` and the C++ examples
use the :doc:`GribJump C++ API <api>`.

GribJump Extraction from File Paths
-----------------------------------

Suppose ``data/`` contains two GRIB files:

.. code-block:: text

   data/
   ├── forecast-0.grib
   └── forecast-6.grib

If you already know the file paths and byte offsets of the GRIB messages you want to read,
(e.g. you have some sort of lookup table for the byte offsets of each field)
you can extract them directly without using FDB.

The examples below extract from the first message in each file; substitute
your known offsets to select other fields.
The ranges specify **zero-based point indices** in the GRIB data. Each range is
half-open: ``(0, 10)`` selects points 0 through 9. Both fields in this example
must contain at least 30 points.

.. tab-set::

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         from pygribjump import GribJump, PathExtractionRequest

         gribjump = GribJump(config={"ignoreGridHash": True})
         ranges = [(0, 10), (20, 30)]
         locations = [
             ("data/forecast-0.grib", 0),
             ("data/forecast-6.grib", 0),
         ]
         requests = [
             PathExtractionRequest(
                 path=path,
                 offset=offset,
                 ranges=ranges,
             )
             for path, offset in locations
         ]

         for result in gribjump.extract_from_paths(requests):
             print(result.values)       # One NumPy array per requested range
             print(result.values_flat)  # All requested ranges concatenated

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include <iostream>
         #include <vector>

         #include "eckit/runtime/Main.h"
         #include "gribjump/GribJump.h"

         int main(int argc, char** argv) {
             eckit::Main::initialise(argc, argv);
             gribjump::Config config;
             config.set("ignoreGridHash", true);
             gribjump::GribJump gribjump(config);
             const std::vector<gribjump::Range> ranges{{0, 10}, {20, 30}};
             std::vector<gribjump::PathExtractionRequest> requests{
                 {"data/forecast-0.grib", "file", 0, "", 0, ranges},
                 {"data/forecast-6.grib", "file", 0, "", 0, ranges},
             };

             auto results = gribjump.extract(requests);
             while (results.hasNext()) {
                 auto result = results.next();
                 for (const auto& range : result->values()) {
                     for (double value : range) {
                         std::cout << value << ' ';
                     }
                     std::cout << '\n';
                 }
             }
         }

Each result represents one field and contains one array or vector per range.
Missing values are accompanied by masks (``result.masks`` in Python,
``result->mask()`` in C++).

For simplicity, these examples set ``ignoreGridHash`` to ``true``. To check that
point indices refer to the expected grid, leave grid checking enabled and pass
the expected ``md5GridSection`` as ``grid_hash`` in Python or as the final
``PathExtractionRequest`` constructor argument in C++.

With the default lazy-indexing settings, GribJump obtains any missing decoding
metadata during extraction. To avoid this initial work when serving extraction
requests, see :ref:`basics-building-index` below.

.. _basics-building-index:

Efficiency: Building a GribJump Index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extraction efficiency can improve dramatically when a small amount of decoding
information is pre-extracted from each GRIB field. For example, CCSDS-packed
fields need offsets into the compressed data so GribJump can jump directly to
the required blocks. Computing this information on demand can require reading
the whole field on the first extraction.

Scan the files once before extracting:

.. code-block:: bash

   gribjump-scan-files data/*.grib

With the default cache configuration, this writes a ``.gribjump`` index beside
each GRIB file:

.. code-block:: text

   data/
   ├── forecast-0.grib
   ├── forecast-0.grib.gribjump
   ├── forecast-6.grib
   └── forecast-6.grib.gribjump

The extraction code stays the same: GribJump discovers and uses the indexes
automatically.

You can inspect an index with:

.. code-block:: bash

   gribjump-dump-info data/forecast-0.grib.gribjump

See :doc:`gribjump_index` for its contents and
:doc:`list_of_configuration_options` for alternative cache locations.

GribJump Extraction from FDB
----------------------------

FDB indexes GRIB fields by their MARS metadata and provides a convenient API
for finding their locations. You can list the fields first and pass those
locations to GribJump, or let GribJump resolve a MARS request for you.

The examples here assume a configured **local FDB** whose data files are
accessible to the client. Set ``FDB5_CONFIG_FILE`` or ``FDB_HOME`` to your FDB
configuration before running them, and adapt the MARS selection to data in your
archive. The Python listing example also requires ``pyfdb``. For remote stores,
see :doc:`gribjump_with_remotefdb`.

List, then extract
~~~~~~~~~~~~~~~~~~

Use `FDB.list` to obtain
each field's file path and byte offset to build the same kind of path extraction
request as above.

.. tab-set::

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import pyfdb
         from pygribjump import GribJump, PathExtractionRequest

         selection = {
             "class": "od", "expver": "0001", "stream": "oper",
             "date": "20230508", "time": "1200", "domain": "g",
             "type": "fc", "levtype": "sfc", "param": "167",
             "step": ["0", "6"],
         }
         ranges = [(0, 10), (20, 30)]
         fdb = pyfdb.FDB()
         gribjump = GribJump(config={"ignoreGridHash": True})
         requests = []
         for field in fdb.list(selection, level=3):
             print(field.combined_key())
             uri = field.uri
             requests.append(PathExtractionRequest(
                 path=uri.path(),
                 scheme=uri.scheme(),
                 offset=field.offset(),
                 ranges=ranges,
             ))

         if requests:
             for result in gribjump.extract_from_paths(requests):
                 print(result.values)

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include <iostream>
         #include <string>
         #include <vector>

         #include "eckit/runtime/Main.h"
         #include "eckit/filesystem/URI.h"
         #include "fdb5/api/FDB.h"
         #include "fdb5/api/helpers/FDBToolRequest.h"
         #include "gribjump/GribJump.h"

         int main(int argc, char** argv) {
             eckit::Main::initialise(argc, argv);
             auto selection = fdb5::FDBToolRequest::requestsFromString(
                 "class=od,expver=0001,stream=oper,date=20230508,"
                 "time=1200,domain=g,type=fc,levtype=sfc,param=167,step=0/6")[0];
             const std::vector<gribjump::Range> ranges{{0, 10}, {20, 30}};
             fdb5::FDB fdb;
             gribjump::Config config;
             config.set("ignoreGridHash", true);
             gribjump::GribJump gribjump(config);
             std::vector<gribjump::PathExtractionRequest> requests;
             auto fields = fdb.list(selection, true);  // Deduplicate fields
             fdb5::ListElement field;
             while (fields.next(field)) {
                 std::cout << field.combinedKey() << '\n';
                 const auto uri = field.location().fullUri();
                 requests.emplace_back(uri.path(), uri.scheme(),
                                       std::stoull(uri.fragment()), "", 0, ranges);
             }

             if (!requests.empty()) {
                 auto results = gribjump.extract(requests);
                 while (results.hasNext()) {
                     auto result = results.next();
                     std::cout << *result << '\n';
                 }
             }
         }

Extract with a list of extraction requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Build an ``ExtractionRequest`` for each field and pass the list to ``extract``.
GribJump performs the FDB lookup internally. Each request must identify exactly
one field: the examples below create separate requests for forecast steps 0 and
6. Each request can specify its own ranges; here both use the same ranges.

.. tab-set::

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         from pygribjump import ExtractionRequest, GribJump

         gribjump = GribJump(config={"ignoreGridHash": True})
         selection = {
             "class": "od", "expver": "0001", "stream": "oper",
             "date": "20230508", "time": "1200", "domain": "g",
             "type": "fc", "levtype": "sfc", "param": "167",
         }
         ranges = [(0, 10), (20, 30)]
         requests = [
             ExtractionRequest({**selection, "step": step}, ranges)
             for step in ("0", "6")
         ]

         for result in gribjump.extract(requests):
             print(result.values)

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include <iostream>
         #include <string>
         #include <vector>

         #include "eckit/runtime/Main.h"
         #include "gribjump/GribJump.h"

         int main(int argc, char** argv) {
             eckit::Main::initialise(argc, argv);
             const std::string selection =
                 "class=od,expver=0001,stream=oper,date=20230508,"
                 "time=1200,domain=g,type=fc,levtype=sfc,param=167";
             gribjump::Config config;
             config.set("ignoreGridHash", true);
             gribjump::GribJump gribjump(config);
             const std::vector<gribjump::Range> ranges{{0, 10}, {20, 30}};
             std::vector<gribjump::ExtractionRequest> requests;
             for (const auto* step : {"0", "6"}) {
                 requests.emplace_back(selection + ",step=" + step, ranges);
             }

             auto results = gribjump.extract(requests);
             while (results.hasNext()) {
                 auto result = results.next();
                 std::cout << *result << '\n';
             }
         }

FDB's metadata index locates fields; the GribJump index provides the additional
decoding information needed for efficient extraction. To generate
``.gribjump`` indexes for existing FDB data with ``gribjump-scan``, or to enable
automatic index generation when archiving new data, follow
:doc:`setup_gribjump_on_fdb`.
