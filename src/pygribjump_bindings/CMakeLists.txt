pybind11_add_module(pygribjump_bindings MODULE bindings.cc)

target_link_libraries(pygribjump_bindings
    PRIVATE pybind11::module
            pybind11::lto
            eckit
            metkit
            gribjump
            # We need to explicity link to stdc++fs on gcc 8.x
            $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,8.0>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,9.0>>:stdc++fs>
)

if(NOT MSVC AND NOT "${CMAKE_BUILD_TYPE}" MATCHES "Debug|RelWithDebInfo")
    # Strip unnecessary sections of the binary on Linux/macOS
    pybind11_strip(pygribjump_bindings)
endif()

set_target_properties(pygribjump_bindings
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${PYGRIBJUMP_STAGING}/pygribjump_bindings
)

add_custom_command(TARGET pygribjump_bindings POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_CURRENT_SOURCE_DIR}/__init__.py
            $<TARGET_FILE_DIR:pygribjump_bindings>
    COMMENT "Copying __init__.py for pygribjump_bindings"
)
