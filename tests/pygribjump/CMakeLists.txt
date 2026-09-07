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
    set(_pythonpath "${CMAKE_BINARY_DIR}/pygribjump-python-package-staging")
    set(_enable_fdb_tests 0)
    if(TARGET pyfdb_bindings)
        set(_pythonpath "${_pythonpath}:${CMAKE_BINARY_DIR}/pyfdb-python-package-staging")
        set(_enable_fdb_tests 1)
    endif()
    set(_env
        "PYTHONPATH=${_pythonpath}:$ENV{PYTHONPATH}"
        "PATH=${CMAKE_BINARY_DIR}/bin:$ENV{PATH}"
        "GRIBJUMP_DIR=${CMAKE_BINARY_DIR}"
        "FDB5_DIR=${CMAKE_BINARY_DIR}"
        "FINDLIBS_DISABLE_PACKAGE=yes"
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
    test_cffi_compat.py
    test_extract_fdb.py
)

foreach(test_file ${test_files})
    add_pygribjump_test("pygribjump" ${test_file})
endforeach()
