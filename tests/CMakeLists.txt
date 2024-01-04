if( NOT EXISTS "${CMAKE_BINARY_DIR}/data" )
  execute_process( COMMAND "${CMAKE_COMMAND}" "-E" "create_symlink"
    "${CMAKE_SOURCE_DIR}/data"
    "${CMAKE_BINARY_DIR}/data" )
endif()

ecbuild_add_test( TARGET    "gribjump_test_gribjump"
SOURCES   "test_gribjump.cc"
INCLUDES  "${ECKIT_INCLUDE_DIRS}"
ENVIRONMENT   "${gribjump_env}"
WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
NO_AS_NEEDED
LIBS      gribjump)

ecbuild_add_test( TARGET    "gribjump_test_compression"
SOURCES   "test_compression.cc"
INCLUDES  "${ECKIT_INCLUDE_DIRS}"
ENVIRONMENT   "${gribjump_env}"
WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
NO_AS_NEEDED
LIBS      gribjump)
