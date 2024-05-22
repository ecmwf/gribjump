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

    # libraries for clientside, no extraction support

    LibGribJump.cc
    LibGribJump.h
    gribjump_config.h.in
    gribjump_version.c.in
    gribjump_version.h.in

    gribjump_c.cc
    gribjump_c.h

    Config.cc
    Config.h
    Stats.cc
    Stats.h

    tools/ToolUtils.h
    tools/ToolUtils.cc

    tools/CompressedRequestTree.h
    tools/CompressedRequestTree.cc

    remote/RemoteGribJump.cc
    remote/RemoteGribJump.h

    GribJump.cc
    GribJump.h
    GribJumpBase.h
    GribJumpBase.cc
    GribJumpFactory.cc
    GribJumpFactory.h
    GribJumpException.h 
    ExtractionData.cc
    ExtractionData.h
)

if( HAVE_GRIBJUMP_LOCAL_EXTRACT )
    list( APPEND gribjump_srcs
    JumpHandle.cc
    JumpHandle.h
    GribInfo.cc
    GribInfo.h
    GribInfoCache.cc
    GribInfoCache.h
    LocalGribJump.cc
    LocalGribJump.h
    GribJumpDataAccessor.h
    FDBService.h
    FDBService.cc

    tools/EccodesExtract.h
    tools/EccodesExtract.cc

    tools/CompressedRequestTree.h
    tools/CompressedRequestTree.cc

    
    remote/Request.h
    remote/Request.cc
    remote/ExtractFileRequest.h
    remote/ExtractFileRequest.cc
    remote/ScanRequest.h
    remote/ScanRequest.cc
    remote/GribJumpServer.h
    remote/GribJumpService.h
    remote/GribJumpUser.cc
    remote/GribJumpUser.h
    remote/WorkItem.cc
    remote/WorkItem.h
    remote/WorkQueue.cc
    remote/WorkQueue.h

    compression/NumericCompressor.h
    compression/Range.h
    compression/Range.cc
    compression/DataAccessor.h

    compression/compressors/Aec.cc
    compression/compressors/Aec.h
    compression/compressors/Simple.cc
    compression/compressors/Simple.h
    compression/compressors/Ccsds.cc
    compression/compressors/Ccsds.h

    compression/compressors/algorithms/SimplePacking.h
    compression/compressors/algorithms/LibEccodes.h
    compression/compressors/algorithms/LibEccodes.cc
    )

    set( SERVER_LIBS fdb5 ${AEC_LIBRARIES} )

endif()

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
        metkit
        eckit
        eckit_option
        ${SERVER_LIBS}
)
