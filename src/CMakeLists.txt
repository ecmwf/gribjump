############################################################################################
# libraries

ecbuild_find_project_files()

add_subdirectory( gribjump )
add_subdirectory( tools )

if (HAVE_PYTHON_GRIBJUMP_INTERFACE)
    set(PYGRIBJUMP_STAGING "${CMAKE_BINARY_DIR}/pygribjump-python-package-staging")
    file(MAKE_DIRECTORY "${PYGRIBJUMP_STAGING}")
    file(CREATE_LINK
        "${CMAKE_CURRENT_SOURCE_DIR}/pygribjump"
        "${PYGRIBJUMP_STAGING}/pygribjump" SYMBOLIC
    )
    add_custom_command(
        OUTPUT ${PYGRIBJUMP_STAGING}/README.md
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/pygribjump/README.md"
            "${PYGRIBJUMP_STAGING}/README.md"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/pygribjump/README.md"
        COMMENT "Copying pygribjump README.md to staging..."
    )
    add_custom_command(
        OUTPUT ${PYGRIBJUMP_STAGING}/LICENSE
        COMMAND ${CMAKE_COMMAND} -E copy
            "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
            "${PYGRIBJUMP_STAGING}/LICENSE"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
        COMMENT "Copying LICENSE to staging..."
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/pygribjump_setup.py.in
        ${PYGRIBJUMP_STAGING}/setup.py
        @ONLY
    )
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/pygribjump_setup.cfg.in
        ${PYGRIBJUMP_STAGING}/setup.cfg
        @ONLY
    )
    add_subdirectory(pygribjump_bindings)
    file(GLOB_RECURSE
        _pygribjump_package_files
        CONFIGURE_DEPENDS
        "pygribjump/*.py"
        "pygribjump/_internal/*.py"
    )
    list(APPEND _pygribjump_package_files
        "${CMAKE_CURRENT_SOURCE_DIR}/pygribjump/README.md"
        "${CMAKE_CURRENT_SOURCE_DIR}/../LICENSE"
    )
    add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/pygribjump.wheel.stamp
        COMMAND ${Python_EXECUTABLE} -m build --wheel ${PYGRIBJUMP_STAGING} -o .
        COMMAND ${CMAKE_COMMAND} -E touch pygribjump.wheel.stamp
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        DEPENDS ${_pygribjump_package_files}
                pygribjump_bindings $<TARGET_FILE:pygribjump_bindings>
                ${PYGRIBJUMP_STAGING}/README.md ${PYGRIBJUMP_STAGING}/LICENSE
        COMMENT "Building Python wheel for pygribjump..."
    )
    add_custom_target(pygribjump-wheel ALL DEPENDS ${CMAKE_BINARY_DIR}/pygribjump.wheel.stamp)
endif()

