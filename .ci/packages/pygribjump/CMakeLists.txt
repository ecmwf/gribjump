# SPDX-FileCopyrightText: 2026- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# pygribjump on its own, against an installed gribjump:
#   cmake -S .ci/packages/pygribjump -B build -DCMAKE_PREFIX_PATH=<gribjump and its deps>

cmake_minimum_required( VERSION 3.18 FATAL_ERROR )

project( pygribjump LANGUAGES CXX C )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# libgribjump lists its dependencies as plain link names (e.g. fdb5), so their
# imported targets must exist before gribjump's exports are consumed. Otherwise
# the linker is asked for -lfdb5 and finds nothing.
find_package( eckit REQUIRED )
find_package( metkit REQUIRED )
find_package( eccodes REQUIRED )
find_package( fdb5 REQUIRED )
find_package( dhskit QUIET )
find_package( gribjump REQUIRED )
find_package( Python 3.11 REQUIRED COMPONENTS Interpreter Development )
find_package( pybind11 3.0.1 REQUIRED )

set( gribjump_VERSION_STR ${gribjump_VERSION} )
set( PYGRIBJUMP_STANDALONE ON )

include( ${CMAKE_CURRENT_SOURCE_DIR}/../../../cmake/pygribjump.cmake )

enable_testing()
add_subdirectory( ${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/pygribjump ${CMAKE_CURRENT_BINARY_DIR}/tests )
