############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory( gribjump )
add_subdirectory( tools )

if (HAVE_PYTHON_GRIBJUMP_INTERFACE)
    include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/pygribjump.cmake)
endif()
