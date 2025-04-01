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
import findlibs
import warnings
from ._version import __version__, __min_lib_version__
from packaging import version

ffi = cffi.FFI()

class GribJumpException(RuntimeError):
    pass

class PatchedLib:
    """
    Patch a CFFI library with error handling

    Finds the header file associated with the GribJump C API and parses it, loads the shared library,
    and patches the accessors with automatic python-C error handling.
    """

    def __init__(self):

        libName = findlibs.find("gribjump")

        if libName is None:
            raise RuntimeError("GribJump library not found")

        hdr_path = os.path.join(os.path.dirname(__file__), 'gribjump_c.h')
        ffi.cdef(self.__read_header(hdr_path))
        self.__lib = ffi.dlopen(libName)

        # All of the executable members of the CFFI-loaded library are functions in the GribJump
        # C API. These should be wrapped with the correct error handling.

        for f in dir(self.__lib):
            try:
                attr = getattr(self.__lib, f)
                setattr(self, f, self.__check_error(attr, f) if callable(attr) else attr)

            except Exception as e:
                print(e)
                print("Error retrieving attribute", f, "from library")

        # Initialise the library, and set it up for python-appropriate behaviour
        self.gribjump_initialise()

        # Check versions
        versionstr = ffi.string(self.gribjump_version()).decode("utf-8")

        if versionstr != __version__:
            warnings.warn(f"GribJump library version {versionstr} does not match pygribjump version {__version__}")
        
        if version.parse(versionstr) < version.parse(__min_lib_version__):
            raise RuntimeError(f"Pymetkit version {__version__} requires GribJump library version {__min_lib_version__} or later. Found {versionstr}")

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
            
            # Some functions dont return error codes. Ignore these.
            if name in ["gribjump_version", "gribjump_git_sha1", "gribjump_string_delete"]:
                return retval
            
            # error codes:
            if retval not in (
                self.__lib.GRIBJUMP_SUCCESS,
                self.__lib.GRIBJUMP_ITERATOR_SUCCESS,
                self.__lib.GRIBJUMP_ITERATOR_COMPLETE,
            ):
                err = ffi.string(self.__lib.gribjump_get_error_string(retval)).decode()
                msg = "Error in function '{}': {}".format(name, err)
                raise GribJumpException(msg)
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

    # todo: interface for one high cardinality request

    def extract(self, polyrequest, ctx=None):
        """
        Parameters
        ----------
        polyrequest : [
            (req1_str, [(lo, hi), (lo, hi), ...])
            (req2_str, [(lo, hi), (lo, hi), ...])
            ...
        ]

        dump : bool
            If true, copy the values into a new numpy array. Otherwise, the values will be
            stored in the original buffer, and will be garbage collected when the result object
            is garbage collected.
        """
        requests = self._unpack_polyrequest(polyrequest)
        logctx=str(ctx) if ctx else "pygribjump_extract"
        logctx_c = ffi.new('const char[]', logctx.encode('ascii'))
        iterator = ExtractionIterator(self.ctype, requests, logctx_c) # future: return this. For now, dump
        return self._reshape_legacy(iterator)

    def _reshape_legacy(self, iterator):
        """
        Reshape the iterator into a list of lists of lists of numpy arrays.
        """
        res = [] # of size nrequests
        for i, result in enumerate(iterator):
            res.append(
                [ # <-- pointless outer dimension for legacy reasons.
                (result.copy_values(), result.copy_masks())  # maybe we should stop returning the mask if no one is using it / testing it.
                ]
            )

        return res

    def _unpack_polyrequest(self, polyrequest):
        requests = []
        for item in polyrequest:
            if len(item) == 2:
                reqstr, ranges = item
                hash = None
            elif len(item) == 3:
                reqstr, ranges, hash = item
            else:
                raise ValueError("Polyrequest should be a list of tuples of length 2 or 3")
            requests.append(ExtractionRequest(reqstr, ranges, hash))
        return requests


    def axes(self, req, level=3, ctx=None):
        # note old axes used a dict in. This is now a string.
        logctx=str(ctx) if ctx else "pygribjump_axes"
        ctx_c = ffi.new('const char[]', logctx.encode('ascii'))
        
        requeststr = dic_to_request(req)
        newaxes = ffi.new('gribjump_axes_t**')
        reqstr = ffi.new('const char[]', requeststr.encode('ascii'))
        lib.gribjump_new_axes(self.__gribjump, reqstr, level, ctx_c, newaxes)
        ax = ffi.gc(newaxes[0], lib.gribjump_delete_axes)
        nkeys = ffi.new('size_t*')
        lib.gribjump_axes_keys_size(ax, nkeys)

        keys = ffi.new('const char*[]', nkeys[0])
        lib.gribjump_axes_keys(ax, keys, nkeys[0])
        
        values = []
        for key in keys:
            nvalues = ffi.new('size_t*')
            lib.gribjump_axes_values_size(ax, key, nvalues)
            values_out = ffi.new('const char*[]', nvalues[0])
            lib.gribjump_axes_values(ax, key, values_out, nvalues[0])
            values.append([ffi.string(values_out[i]).decode('ascii') for i in range(nvalues[0])])
        
        keys = [ffi.string(keys[i]).decode('ascii') for i in range(nkeys[0])]
        return dict(zip(keys, values))

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
    ranges : [(lo, hi), (lo, hi), ...]
        The ranges to extract.
    """
    def __init__(self, req, ranges, gridHash=None):
        self.__shape = []
        reqstr = dic_to_request(req)
        request = ffi.new('gribjump_extraction_request_t**')
        c_reqstr = ffi.new("char[]", reqstr.encode())
        c_hash = ffi.NULL if gridHash is None else ffi.new("char[]", gridHash.encode())
        # Flattened ranges
        c_ranges = ffi.new('size_t[]', len(ranges)*2)
        c_ranges_size = len(ranges)*2
        for i, r in enumerate(ranges):
            c_ranges[i*2] = r[0]
            c_ranges[i*2+1] = r[1]
            self.__shape.append(r[1] - r[0])

        lib.gribjump_new_request(request, c_reqstr, c_ranges, c_ranges_size, c_hash)
        self.__request = ffi.gc(request[0], lib.gribjump_delete_request)

    @property
    def shape(self):
        return self.__shape

    @property
    def ctype(self):
        return self.__request

class ExtractionIterator:
    """
    A class taking owernship of a GribJump extraction iterator.

    Parameters
    ----------
    reqstr : str
        The request mars-retrieve string.
    ranges : [(lo, hi), (lo, hi), ...]
        The ranges to extract.
    """

    def __init__(self, gribjump, requests: list[ExtractionRequest], ctx):
        self.__shapes = [r.shape for r in requests]
        iterator = ffi.new('gribjump_extractioniterator_t**')
        c_requests = ffi.new('gribjump_extraction_request_t*[]', [r.ctype for r in requests])
        
        lib.gribjump_extract(gribjump, c_requests, len(requests), ctx, iterator)
        self.__iterator = ffi.gc(iterator[0], lib.gribjump_extractioniterator_delete)

    def __iter__(self):
        """
        Iterate over the results of the extraction.
        """
        result_c = ffi.new('gribjump_extraction_result_t**')
        i = 0
        while lib.gribjump_extractioniterator_next(self.__iterator, result_c) == lib.GRIBJUMP_ITERATOR_SUCCESS:
            yield ExtractionResult(result_c[0], self.__shapes[i]) # Takes ownership of the result
            i += 1


class ExtractionResult:
    """
    A class taking owernship of the result of a GribJump extract on a field.
    Note that a single request may perform extractions on multiple fields, creating multiple
    ExtractionResult objects.

    Parameters
    ----------
    result : gribjump_extraction_result_t*
        Pointer to the opaque C type
    shape : list of int
        The shape of the result. This is required to unpack the result.
    """

    def __init__(self, result_in, shape):
        self.__shape = shape # required to unpack the result (need dimensions of ranges)
        self.__result = ffi.gc(result_in, lib.gribjump_delete_result) # Takes ownership of the result

    def copy_values(self):
        nvalues = sum(self.__shape)
        values_array = ffi.new("double[]", nvalues)
        values_ptr = ffi.new("double*[1]")
        values_ptr[0] = values_array

        lib.gribjump_result_values(self.__result, values_ptr, nvalues)
 
        # Values are stored in a 1D array, so we need to reshape them into the correct shape
        # @note: Copy for now as I do not trust the old frombuffer approach is playing nice with cffi garbage collection
        # We'll bring it back later.
        # Note, copying element by element here
        result = [
            np.array([values_array[i*size:(i+1)*size][j] for j in range(size)], dtype=np.float64)
            for i, size in enumerate(self.__shape)
        ]
    
        return result

    def copy_masks(self):
        # Bit mask is returned as an array of uint64
        mask_shape = [(size + 63) // 64 for size in self.__shape]
        nvalues = sum(mask_shape)
        masks = ffi.new('unsigned long long[]', nvalues)
        mask_ptr = ffi.new('unsigned long long*[1]')
        mask_ptr[0] = masks
        lib.gribjump_result_mask(self.__result, mask_ptr, nvalues)

        # Masks are stored in a 1D array, so we need to reshape them into the correct shape
        result = [
            np.array([masks[i*size:(i+1)*size][j] for j in range(size)], dtype=np.uint64)
            for i, size in enumerate(mask_shape)
        ]
        return result

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

def dic_to_request(dic):
    # e.g. {"class":"od", "expver":"0001", "levtype":"pl"} -> "class=od,expver=0001,levtype=pl"
    return ','.join(['='.join([k, v]) for k, v in dic.items()])

def version():
    return __version__

def library_version():
    tmp_str = ffi.new('char**')
    lib.gribjump_version_c(tmp_str)
    return ffi.string(tmp_str[0]).decode('utf-8')