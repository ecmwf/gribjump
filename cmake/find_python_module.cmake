# Finding python modules
# ======================
# This function tries to import the specified python module and print its version.
# This function will abort the configure priocess if the module cannot be imported.
# This code depends on that the python interpreter has been found with
# find_package(Python)
#
# Arguments
# ---------
#   python_module: The module to search for
#
# Example Usage
# -------------
# find_python_module(numpy)
function(find_python_module python_module)
    execute_process(
            COMMAND
            ${Python_EXECUTABLE} "-c" "import ${python_module}; print(${python_module}.__version__)"
            RESULT_VARIABLE status
            OUTPUT_VARIABLE python_module_version
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT status)
        message("Found Python module ${python_module} ${python_module_version}")
    else()
        message(FATAL_ERROR "Did not find package ${python_module}")
    endif()
endfunction()
