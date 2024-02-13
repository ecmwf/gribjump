ecbuild_add_executable( TARGET   gribjump-extract
    SOURCES   gribjump-extract.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   gribjump-server
    SOURCES   gribjump-server.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

# TODO: merge me with extract, then delete.
ecbuild_add_executable( TARGET   gribjump-old
  SOURCES   gribjump-old.cc
  INCLUDES  ${ECKIT_INCLUDE_DIRS}
  LIBS      gribjump
    )

ecbuild_add_executable( TARGET   gribjump-scan
    SOURCES   gribjump-scan.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)

ecbuild_add_executable( TARGET   gribjump-validate
    SOURCES   gribjump-validate.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS}
    LIBS      gribjump
)
