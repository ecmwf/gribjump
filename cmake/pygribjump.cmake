# Stages, builds and installs the pygribjump wheel, from gribjump's own build
# or from .ci/packages/pygribjump against an installed gribjump.

set(_pygribjump_root "${CMAKE_CURRENT_LIST_DIR}/..")
set(_pygribjump_src "${_pygribjump_root}/src")

# We create the complete python package layout at this location.
# This allows us to run python wheel creation at this path and
# to put this path on the PYTHONPATH to allow direct use of
# pygribjump, e.g. for testing or local exploration.
set(PYGRIBJUMP_STAGING "${CMAKE_BINARY_DIR}/pygribjump-python-package-staging")
file(MAKE_DIRECTORY "${PYGRIBJUMP_STAGING}")
file(CREATE_LINK
    "${_pygribjump_src}/pygribjump"
    "${PYGRIBJUMP_STAGING}/pygribjump" SYMBOLIC
)
# Copy README.md and LICENSE at build time so changes are picked up
# without needing to re-run cmake configure
add_custom_command(
    OUTPUT ${PYGRIBJUMP_STAGING}/README.md
    COMMAND ${CMAKE_COMMAND} -E copy
        "${_pygribjump_src}/pygribjump/README.md"
        "${PYGRIBJUMP_STAGING}/README.md"
    DEPENDS "${_pygribjump_src}/pygribjump/README.md"
    COMMENT "Copying pygribjump README.md to staging..."
)
add_custom_command(
    OUTPUT ${PYGRIBJUMP_STAGING}/LICENSE
    COMMAND ${CMAKE_COMMAND} -E copy
        "${_pygribjump_root}/LICENSE"
        "${PYGRIBJUMP_STAGING}/LICENSE"
    DEPENDS "${_pygribjump_root}/LICENSE"
    COMMENT "Copying LICENSE to staging..."
)
configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/pygribjump_setup.py.in
    ${PYGRIBJUMP_STAGING}/setup.py
    @ONLY
)
configure_file(
    ${CMAKE_CURRENT_LIST_DIR}/pygribjump_setup.cfg.in
    ${PYGRIBJUMP_STAGING}/setup.cfg
    @ONLY
)
add_subdirectory(
    "${_pygribjump_src}/pygribjump_bindings"
    "${CMAKE_BINARY_DIR}/pygribjump_bindings"
)
file(GLOB_RECURSE
    _pygribjump_package_files
    CONFIGURE_DEPENDS
    "${_pygribjump_src}/pygribjump/*.py"
    "${_pygribjump_src}/pygribjump/_internal/*.py"
)
list(APPEND _pygribjump_package_files
    "${_pygribjump_src}/pygribjump/README.md"
    "${_pygribjump_root}/LICENSE"
)
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/pygribjump.wheel.stamp
    COMMAND ${Python_EXECUTABLE} -m build --wheel ${PYGRIBJUMP_STAGING} -o .
    COMMAND ${CMAKE_COMMAND} -E touch pygribjump.wheel.stamp
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    # Target-level dependency (build ordering) on the bindings target, plus a
    # file-level dependency on its output so the wheel is rebuilt when the
    # compiled extension changes.
    DEPENDS ${_pygribjump_package_files}
            pygribjump_bindings $<TARGET_FILE:pygribjump_bindings>
            ${PYGRIBJUMP_STAGING}/README.md ${PYGRIBJUMP_STAGING}/LICENSE
    COMMENT "Building Python wheel for pygribjump..."
)
add_custom_target(pygribjump-wheel ALL DEPENDS ${CMAKE_BINARY_DIR}/pygribjump.wheel.stamp)

install(CODE "
    file(GLOB _whl \"${CMAKE_BINARY_DIR}/pygribjump-*.whl\")
    file(INSTALL \${_whl} DESTINATION \"\${CMAKE_INSTALL_PREFIX}\")
")
