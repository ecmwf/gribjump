# Copyright 2023- European Centre for Medium-Range Weather Forecasts (ECMWF).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Based on pyfdb
import numpy as np
import cffi
import os
from pkg_resources import parse_version
import findlibs

ffi = cffi.FFI()

class GribJumpException(RuntimeError):
    pass

class PatchedLib:
    """
    Patch a CFFI library with error handling

    Finds the header file associated with the GribJump C API and parses it, loads the shared library,
    and patches the accessors with automatic python-C error handling.
    """
    __type_names = {}

    def __init__(self):

        libName = findlibs.find("gribjump")

        if libName is None:
            raise RuntimeError("GribJump library not found")

        hdr_path = os.path.join(os.path.dirname(__file__), 'gribjump_c.h')
        ffi.cdef(self.__read_header(hdr_path))
        self.__lib = ffi.dlopen(libName)

        # All of the executable members of the CFFI-loaded library are functions in the GribJump
        # C API. These should be wrapped with the correct error handling. Otherwise forward
        # these on directly.

        for f in dir(self.__lib):
            try:
                attr = getattr(self.__lib, f)
                setattr(self, f, self.__check_error(attr, f) if callable(attr) else attr)

            except Exception as e:
                print(e)
                print("Error retrieving attribute", f, "from library")

        # Initialise the library, and set it up for python-appropriate behaviour

        self.gribjump_initialise()

        # TODO <see pyfdb version checking here.>

    def __read_header(self, hdr_path):
        with open(hdr_path, 'r') as f:
            return f.read()

    def __check_error(self, fn, name):
        """
        If calls into the GribJump library return errors, ensure that they get detected and reported
        by throwing an appropriate python exception.
        """

        def wrapped_fn(*args, **kwargs):
            # TODO Error string. See pyfdb
            retval = fn(*args, **kwargs)
            if retval != 0:
                error_str = "Error in function {}".format(name)
                raise GribJumpException(error_str)
            return retval

        return wrapped_fn

# Bootstrap the library

lib = PatchedLib()

class GribJump:
    """This is the main container class for accessing GribJump"""

    def __init__(self):
        gribjump = ffi.new('gribjump_handle_t**')
        lib.gribjump_new_handle(gribjump)

        # Set free function
        self.__gribjump = ffi.gc(gribjump[0], lib.gribjump_delete_handle)

    def extract(self, polyrequest, copy=True):
        """
        Parameters
        ----------
        polyrequest : [
            (req1_str, [(lo, hi), (lo, hi), ...])
            (req2_str, [(lo, hi), (lo, hi), ...])
            ...
        ]

        copy : bool
            If true, copy the values into a new numpy array. Otherwise, the values will be
            stored in the original buffer, and will be garbage collected when the result object
            is garbage collected.
        """
        requests = [ExtractionRequest(reqstr, list_to_rangestr(ranges)) for reqstr, ranges in polyrequest]

        # TODO also masks...

        if copy:
            # Copy values, allow original buffer to be garbage collected.
            # TODO For now, polytope will use this.
            res = [
                res.copy_values() for req in requests for res in self.extract_single(req)
            ]
        else:
            # Original buffer will persist until the ExtractionResult object is garbage collected.
            # TODO Consider making this usable by polytope so that they don't copy data.
            res = [
                self.extract_single(req) for req in requests
            ]

        return res

    def extract_str(self, reqstr, rangestr):
        return self.extract_single(ExtractionRequest(reqstr, rangestr))

    def extract_single(self, request):
        """
        Parameters
        ----------
        request : ExtractionRequest
            The request to perform.
        """

        # resultarray[0] is pointer to array of pointers to results
        # resultarray[0][0] is pointer to first result
        resultarray = ffi.new('gribjump_extraction_result_t***')
        nfields = ffi.new('unsigned short*')
        
        lib.extract(self.__gribjump, request.ctype, resultarray, nfields)
        res = [
            ExtractionResult(resultarray[0][i]) for i in range(nfields[0])
        ]

        return res

    @property
    def ctype(self):
        return self.__gribjump

class ExtractionRequest:
    """
    A class taking owernship of a GribJump extraction request.

    Parameters
    ----------
    reqstr : str
        The request mars-retrieve string.
    rangestr : str
        The range string, formatted as a comma-separated list of ranges 
        XXX TODO (for now, probably want to use list of tuples as input)
        e.g. "1-5,10-15"
    """
    def __init__(self, reqstr, rangestr):
        request = ffi.new('gribjump_extraction_request_t**')
        c_reqstr = ffi.new("char[]", reqstr.encode())
        c_rangestr = ffi.new("char[]", rangestr.encode())
        lib.gribjump_new_request(request, c_reqstr, c_rangestr)
        self.__request = ffi.gc(request[0], lib.gribjump_delete_request)

    @property
    def ctype(self):
        return self.__request

class ExtractionResult:
    """
    A class taking owernship of the result of a GribJump extract on a field.
    Note that a single request may perform extractions on multiple fields, creating multiple
    ExtractionResult objects.
    
    Parameters
    ----------
    result : gribjump_extraction_result_t*
        Pointer to the opaque C type
    """

    def __init__(self, result_in):

        # self.__result = result_in # if using manual delete   
        self.__result = ffi.gc(result_in, lib.gribjump_delete_result)

        # TODO: Check if our values are still good after these go out of scope.
        data = ffi.new('double***')
        nrange = ffi.new('unsigned long*')
        nvalues = ffi.new('unsigned long**')

        # lib.gribjump_result_values(result, data, nrange, nvalues)
        lib.gribjump_result_values_nocopy(self.__result, data, nrange, nvalues)

        # view each value array as a numpy array. Note that we're not copying the data here.
        self.__values = [
            np.frombuffer(ffi.buffer(data[0][i], nvalues[0][i]*ffi.sizeof('double')), dtype=np.float64, count=nvalues[0][i])
            for i in range(nrange[0])
        ]

    def values(self):
        return self.__values

    def copy_values(self):
        return [v.copy() for v in self.__values]
    
    def print_values(self):
        for v in self.__values:
            print(v)

# utils
def rangestr_to_list(rangestr):
    """
    Convert a range string to a list of ranges.
    """
    return [tuple(map(int, r.split('-'))) for r in rangestr.split(',')]

def list_to_rangestr(ranges):
    """
    Convert a list of ranges to a range string.
    """
    return ','.join(['-'.join(map(str, r)) for r in ranges])
