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

    config/LibGribJump.cc
    config/LibGribJump.h
    gribjump_config.h.in
    gribjump_version.c.in
    gribjump_version.h.in

    api/gribjump_c.cc
    api/gribjump_c.h

    config/Config.cc
    config/Config.h
    config/Stats.cc
    config/Stats.h

    tools/GribJumpTool.h
    tools/GribJumpTool.cc
    tools/ToolUtils.h
    tools/ToolUtils.cc

    remote/RemoteGribJump.cc
    remote/RemoteGribJump.h

    api/GribJump.cc
    api/GribJump.h
    api/GribJumpBase.h
    api/GribJumpBase.cc
    api/GribJumpFactory.cc
    api/GribJumpFactory.h
    api/GribJumpException.h 
    api/ExtractionData.cc
    api/ExtractionData.h
    config/Metrics.h
    config/Metrics.cc
    config/LogRouter.h
    config/LogRouter.cc

    api/ExtractionIterator.h
)

if( HAVE_GRIBJUMP_LOCAL_EXTRACT )
    list( APPEND gribjump_srcs
    core/LocalGribJump.cc
    core/LocalGribJump.h
    core/GribJumpDataAccessor.h

    core/Engine.cc
    core/Engine.h
    core/Lister.cc
    core/Lister.h
    core/Task.cc
    core/Task.h
    api/ExtractionItem.cc
    api/ExtractionItem.h
    core/Forwarder.cc
    core/Forwarder.h
    core/URIHelper.h

    tools/EccodesExtract.h
    tools/EccodesExtract.cc

    info/JumpInfo.h
    info/JumpInfo.cc
    info/SimpleInfo.h
    info/SimpleInfo.cc
    info/CcsdsInfo.h
    info/CcsdsInfo.cc
    info/InfoFactory.h
    info/InfoFactory.cc
    info/InfoExtractor.h
    info/InfoExtractor.cc
    info/InfoAggregator.h
    info/InfoAggregator.cc
    info/InfoCache.cc
    info/InfoCache.h
    info/LRUCache.h
    info/UnsupportedInfo.h
    info/UnsupportedInfo.cc

    jumper/Jumper.h
    jumper/Jumper.cc
    jumper/SimpleJumper.h
    jumper/SimpleJumper.cc
    jumper/CcsdsJumper.h
    jumper/CcsdsJumper.cc
    jumper/JumperFactory.h
    jumper/JumperFactory.cc

    remote/Request.h
    remote/Request.cc
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

    compression/compressors/SimplePacking.h
    compression/compressors/SimplePacking.cc

    plugins/FDBPlugin.cc
    plugins/FDBPlugin.h
    )

    set( SERVER_LIBS fdb5 ${AEC_LIBRARIES} )

endif()

if (GRIBJUMP_HAVE_DHSKIT)
list( APPEND SERVER_LIBS dhskit )
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

############################################################################################
# Plugin
# Must happen atomically, and only after the library has been built.
# NB: In future this will be added to the ecbuild_add_library call above

set( gribjump_plugin_file_in gribjump-plugin.yml.in )
set( gribjump_plugin_file_out ${CMAKE_BINARY_DIR}/share/plugins/int.ecmwf.gribjump-plugin.yml )
set( gribjump_plugin_file_tmp gribjump-plugin.yml.tmp)

# configure to temporary file
configure_file( ${gribjump_plugin_file_in} ${gribjump_plugin_file_tmp} @ONLY )

# move file atomically
add_custom_command( TARGET gribjump POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/share/plugins
    COMMAND ${CMAKE_COMMAND} -E copy ${gribjump_plugin_file_tmp} ${gribjump_plugin_file_out}
    BYPRODUCTS ${gribjump_plugin_file_out}
)

install( FILES ${gribjump_plugin_file_out} DESTINATION share/plugins)

############################################################################################
