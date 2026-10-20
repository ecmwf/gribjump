ecbuild_add_test(
    TARGET gribjump_test_gj_85_extremes
    SOURCES test_extremes.cc
    RESOURCES extremes-100.grib2
    ENVIRONMENT "${gribjump_env}"
    NO_AS_NEEDED
    LIBS gribjump eccodes
)
