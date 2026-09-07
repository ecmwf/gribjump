# Python API tests for the pybind11 based pygribjump interface.
#
# The tests run against the python package staged by the build (see
# src/CMakeLists.txt), i.e. against the freshly built bindings, not against
# any pygribjump which may be installed in the environment.

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp")

function(add_pygribjump_test scenario test)
    get_filename_component(stem ${test} NAME_WLE)
    add_test(
        NAME ${scenario}_${stem}
        COMMAND ${Python_EXECUTABLE}
            -m pytest
            --basetemp ${CMAKE_CURRENT_BINARY_DIR}/pytest-tmp/${scenario}/
            -vv -s ${CMAKE_CURRENT_SOURCE_DIR}/${test}
    )
    # If pyfdb is built alongside (e.g. in a bundle build), run the tests which
    # extract from an FDB as well, against the freshly built pyfdb.
    set(_pythonpath "${CMAKE_BINARY_DIR}/pygribjump-python-package-staging")
    set(_enable_fdb_tests 0)
    if(TARGET pyfdb_bindings)
        set(_pythonpath "${_pythonpath}:${CMAKE_BINARY_DIR}/pyfdb-python-package-staging")
        set(_enable_fdb_tests 1)
    endif()
    set(_env
        "PYTHONPATH=${_pythonpath}:$ENV{PYTHONPATH}"
        "PATH=${CMAKE_BINARY_DIR}/bin:$ENV{PATH}"
        # prevent findlibs in pygribjump from picking up any globally installed gribjump
        "GRIBJUMP_DIR=${CMAKE_BINARY_DIR}"
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        "FINDLIBS_DISABLE_PACKAGE=yes"
        # prevent findlibs in eccodes-python from picking up any globally installed eccodes
        "ECCODES_PYTHON_USE_FINDLIBS=1"
        "GRIBJUMP_IGNORE_GRID=1"
        "PYGRIBJUMP_TEST_ENABLE_FDB=${_enable_fdb_tests}"
        "${gribjump_env}"
    )
    set_tests_properties(${scenario}_${stem}
        PROPERTIES
        RESOURCE_LOCK ${scenario}
        ENVIRONMENT "${_env}"
        LABELS ${scenario}
    )
endfunction()

set(test_files
    test_version.py
    test_mappers.py
    test_extraction_request.py
    test_gribjump.py
    test_extract_from_paths.py
    # Only run if pyfdb is built alongside, see PYGRIBJUMP_TEST_ENABLE_FDB above
    test_extract_fdb.py
)

foreach(test_file ${test_files})
    add_pygribjump_test("pygribjump" ${test_file})
endforeach()
