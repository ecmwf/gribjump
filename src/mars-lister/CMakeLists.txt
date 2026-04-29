ecbuild_add_executable(
    TARGET    mars-lister-server
    SOURCES   mars-lister-server.cc MarsLister.cc
    INCLUDES  ${ECKIT_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}
    LIBS      eckit metkit
)
