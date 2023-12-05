
ecbuild_add_executable( TARGET   gj-server
    SOURCES   gj-server.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   grib-jump
    SOURCES   grib-jump.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)
ecbuild_add_executable( TARGET   gj-dbgclient
    SOURCES   debug_clientside.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)