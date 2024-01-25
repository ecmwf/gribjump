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

    LibGribJump.cc
    LibGribJump.h
    gribjump_config.h.in
    gribjump_version.c.in
    gribjump_version.h.in

    gribjump_c.cc
    gribjump_c.h

    Config.cc
    Config.h
    GribHandleData.cc
    GribHandleData.h
    GribInfo.cc
    GribInfo.h
    GribInfoCache.cc
    GribInfoCache.h
    GribJump.cc
    GribJump.h
    GribJumpBase.h
    GribJumpBase.cc
    LocalGribJump.cc
    LocalGribJump.h
    GribJumpFactory.cc
    GribJumpFactory.h
    GribJumpException.h 
    GribJumpDataAccessor.h
    ExtractionData.cc
    ExtractionData.h
    Stats.cc
    Stats.h
    Bitmap.cc
    Bitmap.h
    FDBService.h
    FDBService.cc

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
    remote/RemoteGribJump.cc
    remote/RemoteGribJump.h
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
        ${AEC_LIBRARIES}
)
