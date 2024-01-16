
list( APPEND gribjump_test_data_files
const.grib
no_mask.grib
sl_mask.grib
synth11.grib
synth12.grib
synth11_ccsds_bitmap.grib2
synth11_ccsds_no_bitmap.grib2
extract_ranges.grib
)

list( APPEND gribjump_test_O1280_data_files
2t_O1280.grib
2t_O1280.values
ceil_O1280.grib
ceil_O1280.values
)

if(ECCODES_DEFINITION_PATH AND (EXISTS ${ECCODES_DEFINITION_PATH}))
    list( APPEND gribjump_env 
    ECCODES_DEFINITION_PATH=${ECCODES_DEFINITION_PATH})
endif()

ecbuild_get_test_multidata( TARGET gribjump_test_data_files
                            NAMES ${gribjump_test_data_files}
                            NOCHECK )

ecbuild_get_test_multidata( TARGET gribjump_test_O1280_data_files
                            NAMES ${gribjump_test_O1280_data_files}
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
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS      gribjump)

ecbuild_add_test( TARGET "gribjump_test_api"
    SOURCES   "test_api.cc"
    INCLUDES  "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT   "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS      gribjump)

ecbuild_add_test( TARGET "gribjump_test_O1280"
    SOURCES   "test_O1280.cc"
    INCLUDES  "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT   "${gribjump_env}"
    TEST_DEPENDS gribjump_test_O1280_data_files
    NO_AS_NEEDED
    LIBS      gribjump)
