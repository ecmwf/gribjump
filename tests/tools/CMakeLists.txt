list( APPEND gribjump_test_tools_data
supported.grib
unsupported.grib
)

# TODO: Make this check md5sum. Removing NOCHECK seems to not pull the data at all...
ecbuild_get_test_multidata(TARGET gribjump_test_tools_data NAMES ${gribjump_test_tools_data} NOCHECK)

ecbuild_configure_file( callback_vs_scan.sh.in gribjump_callback_vs_scan.sh @ONLY )

ecbuild_add_test(
    TARGET   gribjump_callback_vs_scan
    CONDITION HAVE_FDB_BUILD_TOOLS
    TYPE     SCRIPT
    COMMAND  gribjump_callback_vs_scan.sh
    TEST_DEPENDS gribjump_test_tools_data
    
)
