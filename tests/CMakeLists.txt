#if( NOT EXISTS "${CMAKE_BINARY_DIR}/data" )
#  execute_process( COMMAND "${CMAKE_COMMAND}" "-E" "create_symlink"
#    "${CMAKE_SOURCE_DIR}/data"
#    "${CMAKE_BINARY_DIR}/data" )
#endif()

list( APPEND gribjump_test_data_files
const.grib
no_mask.grib
sl_mask.grib
synth11.grib
synth12.grib
)

if(ECCODES_DEFINITION_PATH AND (EXISTS ${ECCODES_DEFINITION_PATH}))
    list( APPEND gribjump_env 
    ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH})
endif()

ecbuild_get_test_multidata( TARGET gribjump_test_data_files
                            NAMES ${gribjump_test_data_files}
                            NOCHECK )

ecbuild_add_test( TARGET    "gribjump_test_gribjump"
    SOURCES   "test_gribjump.cc"
    INCLUDES  "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT   "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS      gribjump)

ecbuild_add_test( TARGET    "gribjump_test_compression"
    SOURCES   "test_compression.cc"
    INCLUDES  "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT   "${gribjump_env}"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    NO_AS_NEEDED
    LIBS      gribjump)
