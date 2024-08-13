# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

# - Try to find AEC (Adaptive Entropy Coding library)
# See https://www.dkrz.de/redmine/projects/aec/wiki

# Once done this will define
#  AEC_FOUND        - System has AEC
#  AEC_INCLUDE_DIRS - The AEC include directories
#  AEC_LIBRARIES    - The libraries needed to use AEC
#
# The following paths will be searched with priority if set in CMake or env
#
#  AEC_DIR          - prefix path of the AEC installation
#  AEC_PATH         - prefix path of the AEC installation
#  LIBAEC_DIR
#  libaec_DIR
#  LIBAEC_PATH
#  libaec_PATH
#  libaec_ROOT

find_path( AEC_INCLUDE_DIR libaec.h
           PATHS ${AEC_DIR} ${AEC_PATH} ${LIBAEC_DIR} ${libaec_DIR} ${LIBAEC_PATH} ${libaec_PATH} ${libaec_ROOT} ENV AEC_DIR ENV AEC_PATH ENV LIBAEC_DIR ENV libaec_DIR ENV LIBAEC_PATH ENV libaec_PATH ENV libaec_ROOT
           PATH_SUFFIXES include include/aec NO_DEFAULT_PATH )
find_path( AEC_INCLUDE_DIR libaec.h PATH_SUFFIXES include include/aec )

find_library( AEC_LIBRARY  NAMES aec
              PATHS ${AEC_DIR} ${AEC_PATH} ${LIBAEC_DIR} ${libaec_DIR} ${LIBAEC_PATH} ${libaec_PATH} ${libaec_ROOT} ENV AEC_DIR ENV AEC_PATH ENV LIBAEC_DIR ENV libaec_DIR ENV LIBAEC_PATH ENV libaec_PATH ENV libaec_ROOT
              PATH_SUFFIXES lib lib64 lib/aec lib64/aec NO_DEFAULT_PATH )
find_library( AEC_LIBRARY NAMES aec PATH_SUFFIXES lib lib64 lib/aec lib64/aec )

set( AEC_LIBRARIES    ${AEC_LIBRARY} )
set( AEC_INCLUDE_DIRS ${AEC_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(AEC  DEFAULT_MSG AEC_LIBRARY AEC_INCLUDE_DIR)

# Need to look explicitly in the header file for the version. Note that old versions of AEC (too old for gribjump) do not have version information.

file(STRINGS ${AEC_INCLUDE_DIR}/libaec.h AEC_VERSION_MAJOR REGEX "#define AEC_VERSION_MAJOR")
file(STRINGS ${AEC_INCLUDE_DIR}/libaec.h AEC_VERSION_MINOR REGEX "#define AEC_VERSION_MINOR")
file(STRINGS ${AEC_INCLUDE_DIR}/libaec.h AEC_VERSION_PATCH REGEX "#define AEC_VERSION_PATCH")

string(REGEX REPLACE "#define AEC_VERSION_MAJOR " "" AEC_VERSION_MAJOR "${AEC_VERSION_MAJOR}")
string(REGEX REPLACE "#define AEC_VERSION_MINOR " "" AEC_VERSION_MINOR "${AEC_VERSION_MINOR}")
string(REGEX REPLACE "#define AEC_VERSION_PATCH " "" AEC_VERSION_PATCH "${AEC_VERSION_PATCH}")

# check that the version is not empty
if(NOT AEC_VERSION_MAJOR OR NOT AEC_VERSION_MINOR OR NOT AEC_VERSION_PATCH)
  set(AEC_VERSION "")
else()
  set(AEC_VERSION "${AEC_VERSION_MAJOR}.${AEC_VERSION_MINOR}.${AEC_VERSION_PATCH}")
endif()

mark_as_advanced(AEC_INCLUDE_DIR AEC_LIBRARY )
