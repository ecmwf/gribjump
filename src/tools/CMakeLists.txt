#ecbuild_add_executable( TARGET   gj-server
#    SOURCES   gj-server.cc
#    INCLUDES  ${ECKIT_INCLUDE_DIRS}
#    LIBS      gribjump
#)

#ecbuild_add_executable( TARGET   grib-jump2
#    SOURCES   grib-jump.cc
#    INCLUDES  ${ECKIT_INCLUDE_DIRS}
#    LIBS      gribjump
#)

#ecbuild_add_executable( TARGET   gj-extract
#    SOURCES   gj-extract.cc
#    INCLUDES  ${ECKIT_INCLUDE_DIRS}
#    LIBS      gribjump
#)

ecbuild_add_executable( TARGET   gj-dbgclient
    SOURCES   debug_clientside.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   gj-testc
    SOURCES   gj-test.c
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   gj-cache
    SOURCES   gj-cache.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)
