cmake_minimum_required( VERSION 3.12 FATAL_ERROR )

find_package( ecbuild 3.7 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)

project( gribjump LANGUAGES C CXX )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

########################################################################################################################
### dependencies and options

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support
ecbuild_find_package( NAME eckit VERSION 1.25.2 REQUIRED )
ecbuild_find_package( NAME metkit VERSION 1.5 REQUIRED )

# Set "GRIBJUMP_LOCAL_EXTRACT" to build everything. If it is off, build only minimal clientside functionality.
ecbuild_add_option( FEATURE GRIBJUMP_LOCAL_EXTRACT
                    DEFAULT ON
                    DESCRIPTION "Build local extraction and serverside functionality")

if (HAVE_GRIBJUMP_LOCAL_EXTRACT)
  ecbuild_find_package( NAME fdb5 VERSION 5.13.1 REQUIRED )
  set(GRIBJUMP_HAVE_FDB 1)

  ecbuild_find_package( NAME eccodes VERSION 2.32.1 REQUIRED )

  ### AEC
  # Override eccodes' aec with our own: we need a newer version.
  unset(AEC_INCLUDE_DIRS CACHE)
  unset(AEC_LIBRARIES CACHE)
  ecbuild_find_package( NAME AEC VERSION 1.1.1 REQUIRED )

  # ecbuild_find_package's version checking does not work if the version is not specified in the package.
  if (NOT AEC_VERSION)
    message(FATAL_ERROR "AEC version is too old (version unspecified). Minimum supported version is 1.1.1.")
  endif()
  if (AEC_VERSION VERSION_LESS 1.1.1)
    message(FATAL_ERROR "AEC version is too old. Minimum supported version is 1.1.1. Found version: ${AEC_VERSION}")
  endif()

  # Optional dependency: dhskit
  ecbuild_find_package( NAME dhskit VERSION 0.8.6 )
  set(GRIBJUMP_HAVE_DHSKIT ${dhskit_FOUND})

endif()

# python api test are currently by default disabled because we cannot run them in the ci
ecbuild_add_option(
    FEATURE PYTHON_API_TESTS
    DEFAULT OFF
    DESCRIPTION "Will execute python tests against pygribjump / libgribjump")

########################################################################################################################
# contents

include(compiler_warnings) # optionally handle compiler specific warnings
include(find_python_module)

set( gribjump_INCLUDE_DIRS  ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}/src )

include_directories(
  ${AEC_INCLUDE_DIRS}
  ${gribjump_INCLUDE_DIRS}
  ${eckit_INCLUDE_DIRS}
  )

set_directory_properties( PROPERTIES COMPILE_DEFINITIONS "${ECKIT_DEFINITIONS};${ECCODES_DEFINITIONS}" )
get_directory_property( gribjump_DEFINITIONS COMPILE_DEFINITIONS )

### source files

add_subdirectory( src )

if (HAVE_GRIBJUMP_LOCAL_EXTRACT)
  add_subdirectory( tests )
endif()

if(HAVE_PYTHON_API_TESTS)
    ecbuild_find_package( NAME Python
                          VERSION 3.10
                          COMPONENTS Interpreter
                          REQUIRED )

    find_python_module(cffi)
    find_python_module(numpy)
    find_python_module(pytest)
    find_python_module(setuptools)
    find_python_module(findlibs)

    # Depending on if gribjumps dependencies are installed or inside a build
    # tree locations must be made known to fdb and eccodes.
    # If this cmake file is not the top-level cmake file we deduce that this is
    # a bundle build and all dependencies are inside a build tree.
    # Otherwise it is expected that all dependencies reside in an install tree
    get_property(is_subproject DIRECTORY PROPERTY PARENT_DIRECTORY)
    if(is_subproject)
        set(FDB5_DIR "${CMAKE_BINARY_DIR}")
        # This is not required if eccodes is build with ENABLE_MEMFS=ON, but in
        # that case the environment variable is ignored.
        set(REDEFINED_ECCODES_DEFINITION_PATH
            "${CMAKE_BINARY_DIR}/share/eccodes/definitions")
    else()
        set(REDEFINED_ECCODES_DEFINITION_PATH "")
        set(FDB5_DIR "${fdb5_DIR}/../../..")
    endif()

    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/run_pytest.sh.in
        ${CMAKE_CURRENT_BINARY_DIR}/run_pytest.sh
        @ONLY
    )

    add_test(
        NAME pygribjump_pytests
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run_pytest.sh -vv -s
                --basetemp=${CMAKE_CURRENT_BINARY_DIR}/pytest-out
    )

endif()

########################################################################################################################
### finalize

ecbuild_install_project( NAME gribjump )

ecbuild_print_summary()
