
ecbuild_configure_file( test_server.sh.in gribjump_test_server.sh @ONLY )
ecbuild_configure_file( fdb_config.yaml.in fdb_config.yaml @ONLY )

# Client side of test
ecbuild_add_executable(
    TARGET    gribjump_test_remote_exe
    SOURCES   test_remote.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
    NOINSTALL
)

ecbuild_add_executable(
    TARGET    gribjump_test_remote_forward_exe
    SOURCES   test_remote_forward.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
    NOINSTALL
)

# Wrapper script to start the server and run the client test
ecbuild_add_test(
    TARGET    gribjump_test_remote
    TYPE      SCRIPT
    COMMAND   gribjump_test_server.sh
    TEST_DEPENDS gribjump_test_data_files gribjump_test_remote_exe gribjump_test_remote_forward_exe
    LABELS    remote
)
