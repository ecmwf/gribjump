find_package(Doxygen 1.9 REQUIRED)
find_package(Sphinx REQUIRED)

# --- Paths ---
set(GRIBJUMP_API_HEADER_DIR "${PROJECT_SOURCE_DIR}/src/gribjump/api")
# The public API surface Doxygen parses: the api/ headers plus the main
# high-level entry points (GribJump C++ class and the C API), which live
# directly under src/gribjump/ rather than in src/gribjump/api/.
set(GRIBJUMP_API_EXTRA_HEADERS
    "${PROJECT_SOURCE_DIR}/src/gribjump/GribJump.h"
    "${PROJECT_SOURCE_DIR}/src/gribjump/gribjump_c.h"
    "${PROJECT_SOURCE_DIR}/src/gribjump/Config.h")
set(DOXYGEN_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/doxygen")
set(SPHINX_OUTPUT_DIR  "${CMAKE_CURRENT_BINARY_DIR}/sphinx")
set(DOXYGEN_XML_DIR    "${DOXYGEN_OUTPUT_DIR}/xml")
set(DOXYGEN_HTML_DIR   "${DOXYGEN_OUTPUT_DIR}/html")
string(JOIN " " DOXYGEN_INPUT_DIR "${GRIBJUMP_API_HEADER_DIR}" ${GRIBJUMP_API_EXTRA_HEADERS})
set(GRIBJUMP_VERSION   "${PROJECT_VERSION}")

# Staging dir whose "doxygen/" subfolder holds the generated Doxygen HTML.
# Sphinx copies it verbatim into the site (see conf.py / html_extra_path).
set(SPHINX_EXTRA_DIR   "${CMAKE_CURRENT_BINARY_DIR}/extra")

# --- Input file globs ---
file(GLOB_RECURSE GRIBJUMP_PUBLIC_HEADERS CONFIGURE_DEPENDS "${GRIBJUMP_API_HEADER_DIR}/*.h")
list(APPEND GRIBJUMP_PUBLIC_HEADERS ${GRIBJUMP_API_EXTRA_HEADERS})
file(GLOB_RECURSE PYGRIBJUMP_SRCS CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/python/pygribjump/src/pygribjump/*.py")
file(GLOB_RECURSE SPHINX_INPUT_FILES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.rst")
list(FILTER SPHINX_INPUT_FILES EXCLUDE REGEX "_build")

# --- Output stamps ---
set(DOXYGEN_INDEX_FILE "${DOXYGEN_OUTPUT_DIR}/html/index.html")
set(SPHINX_INDEX_FILE  "${SPHINX_OUTPUT_DIR}/index.html")

# --- Configure Doxyfile ---
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
    "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    @ONLY
)

# --- Doxygen ---
add_custom_command(
    OUTPUT  ${DOXYGEN_INDEX_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory "${DOXYGEN_OUTPUT_DIR}"
    COMMAND ${DOXYGEN_EXECUTABLE} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    COMMAND ${CMAKE_COMMAND} -E rm -rf "${SPHINX_EXTRA_DIR}/doxygen"
    COMMAND ${CMAKE_COMMAND} -E copy_directory "${DOXYGEN_HTML_DIR}" "${SPHINX_EXTRA_DIR}/doxygen"
    DEPENDS ${GRIBJUMP_PUBLIC_HEADERS} "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile"
    MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Running Doxygen on GribJump API headers"
)

# --- Sphinx ---
add_custom_command(
    OUTPUT  ${SPHINX_INDEX_FILE}
    COMMAND
        ${CMAKE_COMMAND} -E env
        "DOXYGEN_HTML_EXTRA_DIR=${SPHINX_EXTRA_DIR}"
        "DOXYGEN_XML_DIR=${DOXYGEN_XML_DIR}"
        ${SPHINX_EXECUTABLE} -j auto -E -a -T
        -Dbreathe_projects.GribJump=${DOXYGEN_XML_DIR}
        -Dversion=${GRIBJUMP_VERSION}
        -Drelease=${GRIBJUMP_VERSION}
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${SPHINX_OUTPUT_DIR}"
    DEPENDS ${DOXYGEN_INDEX_FILE} ${SPHINX_INPUT_FILES}
            ${PYGRIBJUMP_SRCS}
            "${CMAKE_CURRENT_SOURCE_DIR}/conf.py"
    COMMENT "Running Sphinx to build HTML documentation"
)

# --- Target ---
add_custom_target(gribjump-doc ALL DEPENDS ${SPHINX_INDEX_FILE} ${DOXYGEN_INDEX_FILE})
