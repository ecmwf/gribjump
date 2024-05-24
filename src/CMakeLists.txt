############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory( gribjump )
add_subdirectory( tools )

find_package(Protobuf REQUIRED)
include_directories(${protobuf_INCLUDE_DIR})

