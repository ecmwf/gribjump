cmake_minimum_required( VERSION 3.12 FATAL_ERROR )

find_package( ecbuild 3.7 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)

project( gribjump LANGUAGES C CXX )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

########################################################################################################################
### dependencies and options

### eckit

set( PERSISTENT_NAMESPACE "eckit" CACHE INTERNAL "" ) # needed for generating .b files for persistent support

ecbuild_find_package( NAME AEC VERSION 1.1.1 REQUIRED )
ecbuild_find_package( NAME eckit VERSION 1.25.2 REQUIRED )
ecbuild_find_package( NAME eccodes VERSION 2.32.1 REQUIRED )
ecbuild_find_package( NAME metkit VERSION 1.5 REQUIRED )
ecbuild_find_package( NAME fdb5 VERSION 5.11.28 REQUIRED ) # TODO: Will have to support axes requests...

########################################################################################################################
# contents

include(cmake/compiler_warnings.cmake) # optionally handle compiler specific warnings

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
add_subdirectory( tests )


### finalize

ecbuild_install_project( NAME gribjump )

ecbuild_print_summary()
