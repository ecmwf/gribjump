ecbuild_generate_config_headers( DESTINATION ${INSTALL_INCLUDE_DIR}/gribjump )

configure_file( gribjump_config.h.in    gribjump_config.h   )
configure_file( gribjump_version.h.in   gribjump_version.h  )
configure_file( gribjump_version.c.in   ${CMAKE_CURRENT_BINARY_DIR}/gribjump_version.c   )

install( FILES ${CMAKE_CURRENT_BINARY_DIR}/gribjump_config.h
               ${CMAKE_CURRENT_BINARY_DIR}/gribjump_version.h
         DESTINATION ${INSTALL_INCLUDE_DIR}/gribjump )

# gribjump library

list( APPEND gribjump_srcs
    ${CMAKE_CURRENT_BINARY_DIR}/gribjump_version.c

    LibGribjump.cc 
    LibGribjump.h
    gribjump_config.h.in
    gribjump_version.c.in 
    gribjump_version.h.in 

    GribHandleData.cc 
    GribHandleData.h
    GribInfo.cc
    GribInfo.h 
    GribJump.cc 
    GribJump.h 
    GribJumpException.h 
    ExtractionData.cc
    ExtractionData.h

    remote/GribJumpServer.h
    remote/GribJumpService.h
    remote/GribJumpUser.cc
    remote/GribJumpUser.h
)

ecbuild_add_library(

    TARGET  gribjump
    
    SOURCES
        ${gribjump_srcs}

    INSTALL_HEADERS LISTED

    HEADER_DESTINATION
        ${INSTALL_INCLUDE_DIR}/gribjump

    PUBLIC_INCLUDES
        $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/src>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>

    PUBLIC_LIBS
        fdb5
        metkit
        eckit
        eckit_option

)
