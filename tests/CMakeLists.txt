
list( APPEND gribjump_test_data_files
const.grib
no_mask.grib
sl_mask.grib
synth11.grib
synth12.grib
synth11_ccsds_bitmap.grib2
synth11_ccsds_no_bitmap.grib2
extract_ranges.grib
schema
axes.grib
)

list( APPEND gribjump_test_O1280_data_files
2t_O1280.grib
ceil_O1280.grib
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

# Todo merge this with test_compression.
# ecbuild_add_test( TARGET    "gribjump_test_gribjump"
#     SOURCES   "test_gribjump.cc"
#     INCLUDES  "${ECKIT_INCLUDE_DIRS}"
#     ENVIRONMENT   "${gribjump_env}"
#     TEST_DEPENDS gribjump_test_data_files
#     NO_AS_NEEDED
#     LIBS      gribjump)

ecbuild_add_test( 
    TARGET "gribjump_test_compression"
    SOURCES "test_compression.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_api"
    SOURCES "test_api.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_api_axes"
    SOURCES "test_api_axes.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_O1280"
    SOURCES "test_O1280.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_O1280_data_files
    NO_AS_NEEDED
    LIBS gribjump
)


ecbuild_add_test(
    TARGET "gribjump_test_gribinfo"
    SOURCES "test_gribinfo.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_O1280_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_engine"
    SOURCES "test_engine.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_cache"
    SOURCES "test_cache.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_plugin"
    SOURCES "test_plugin.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_misc_units"
    SOURCES "test_misc_units.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    NO_AS_NEEDED
    LIBS gribjump
)

# c compiler test
ecbuild_add_test(
    TARGET "gribjump_test_c_compile"
    SOURCES "test_c_compile.c"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    NO_AS_NEEDED
    LIBS gribjump
)

ecbuild_add_test(
    TARGET "gribjump_test_c_api"
    SOURCES "test_c_api.cc"
    INCLUDES "${ECKIT_INCLUDE_DIRS}"
    ENVIRONMENT "${gribjump_env}"
    TEST_DEPENDS gribjump_test_data_files
    NO_AS_NEEDED
    LIBS gribjump
)

if (ENABLE_FDB_BUILD_TOOLS)
    add_subdirectory(tools)
    add_subdirectory(remote)
endif()
