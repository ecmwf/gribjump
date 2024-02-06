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

ecbuild_add_executable( TARGET   gj-extract
    SOURCES   gj-extract.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

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

ecbuild_add_executable( TARGET   gribinfo-fromfile
    SOURCES   infofromfile.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   gribjump-scan
    SOURCES   gribjump-scan.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   compare-eccodes
    SOURCES   compare_eccodes.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)
