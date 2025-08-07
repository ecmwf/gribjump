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

import numpy as np
import cffi
import os
import findlibs
import warnings
from ._version import __version__, __min_lib_version__
from packaging import version
from typing import Callable, Any, overload
from itertools import accumulate
import json
from getpass import getuser
from socket import gethostname

ffi = cffi.FFI()
CData = ffi.CData


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
                setattr(self, f, self.__check_error(
                    attr, f) if callable(attr) else attr)

            except Exception as e:
                print(e)
                print("Error retrieving attribute", f, "from library")

        # Initialise the library, and set it up for python-appropriate behaviour
        self.gribjump_initialise()

        # Check versions
        versionstr = ffi.string(self.gribjump_version()).decode("utf-8")

        if versionstr != __version__:
            warnings.warn(
                f"GribJump library version {versionstr} does not match pygribjump version {__version__}")

        if version.parse(versionstr) < version.parse(__min_lib_version__):
            raise RuntimeError(
                f"Pygribjump version {__version__} requires GribJump library version {__min_lib_version__} or later. Found GribJump library {versionstr}")

    def __read_header(self, hdr_path: str):
        with open(hdr_path, 'r') as f:
            return f.read()

    def __check_error(self, fn: Callable[..., Any], name: str) -> Callable[..., Any]:
        """
        If calls into the GribJump library return errors, ensure that they get detected and reported
        by throwing an appropriate python exception.
        """
        # Some functions dont return error codes. Ignore these.
        if name in ["gribjump_version", "gribjump_git_sha1", "gribjump_string_delete"]:
            return fn

        # Iterator has a different set of error codes
        if name in ["gribjump_extractioniterator_next"]:
            def wrapped_fn(*args: Any, **kwargs: Any) -> Any:
                retval = fn(*args, **kwargs)

                # error codes:
                if retval not in (
                    self.__lib.GRIBJUMP_ITERATOR_SUCCESS,
                    self.__lib.GRIBJUMP_ITERATOR_COMPLETE,
                ):
                    err = ffi.string(
                        self.__lib.gribjump_error_string()).decode()
                    msg = "Error in function '{}': {}".format(name, err)
                    raise GribJumpException(msg)
                return retval

        else:
            def wrapped_fn(*args: Any, **kwargs: Any) -> Any:
                retval = fn(*args, **kwargs)

                if retval != self.__lib.GRIBJUMP_SUCCESS:
                    err = ffi.string(
                        self.__lib.gribjump_error_string()).decode()
                    msg = "Error in function '{}': {}".format(name, err)
                    raise GribJumpException(msg)

                return retval

        return wrapped_fn


# Bootstrap the library
lib = PatchedLib()


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

    def __init__(self, req: dict[str, str], ranges: list[tuple[int, int]], gridHash: str = None):
        if not ranges:
            raise ValueError(
                f"Must provide at least one range but found {ranges=}")

        self.__shape = []
        self.__ranges = ranges.copy()
        reqstr = dic_to_request(req)
        request = ffi.new('gribjump_extraction_request_t**')
        c_reqstr = ffi.new("char[]", reqstr.encode())
        c_hash = ffi.NULL if gridHash is None else ffi.new(
            "char[]", gridHash.encode())

        # Flattened ranges
        c_ranges = ffi.new('size_t[]', len(ranges)*2)
        c_ranges_size = len(ranges)*2
        for i, r in enumerate(ranges):
            if not r[0] < r[1]:
                raise ValueError(
                    f"Found invalid range {r}: Expected lo < hi for ranges [lo, hi).")
            c_ranges[i*2] = r[0]
            c_ranges[i*2+1] = r[1]
            self.__shape.append(r[1] - r[0])

        lib.gribjump_new_request(
            request, c_reqstr, c_ranges, c_ranges_size, c_hash)
        self.__request = ffi.gc(request[0], lib.gribjump_delete_request)

    @classmethod
    def from_mask(cls, req: dict[str, str], mask: np.ndarray, gridHash: str = None):
        """
        Create a request from a boolean mask.
        The mask is a 1D array of booleans, where True indicates the value should be extracted.
        """

        m = np.asarray(mask, dtype=bool).ravel()

        if not m.any():
            raise ValueError("Mask must contain at least one True value")

        padded = np.concatenate(([False], m, [False]))
        d = np.diff(padded.astype(int))
        starts = np.where(d == 1)[0]
        ends = np.where(d == -1)[0]
        ranges = list(zip(starts, ends))

        return cls(req, ranges, gridHash)

    @classmethod
    def from_indices(cls, req: dict[str, str], points: np.ndarray, gridHash: str = None):
        """
        Create a request from a 1D list of indices.
        """
        ranges = [(p, p+1) for p in points]
        return cls(req, ranges, gridHash)

    @property
    def shape(self):
        return self.__shape

    @property
    def ctype(self):
        return self.__request

    @property
    def ranges(self) -> list[tuple[int, int]]:
        """
        Return the ranges as a list of tuples.
        """
        return self.__ranges

    def indices(self) -> np.ndarray:
        """
        Return a 1d array with the indices of the values that would be retrieved.
        """
        total = sum(self.__shape)
        idx_iter = (idx for (low, high)
                    in self.ranges for idx in range(low, high))
        return np.fromiter(idx_iter, int, count=total)


class PathExtractionRequest:
    def __init__(self, path: str, scheme: str, offset: int, host: str, port: int,
                 ranges: list[tuple[int, int]], gridHash: str = None):
        """
        Create a request from a file path, scheme and offset.
        """

        if not ranges:
            raise ValueError(
                f"Must provide at least one range but found {ranges=}")

        self.__shape = []
        self.__ranges = list(ranges).copy()

        # Prepare arguments for C call
        c_path = ffi.new("char[]", path.encode())
        c_scheme = ffi.new("char[]", scheme.encode())
        c_offset = offset
        c_host = ffi.new("char[]", host.encode())
        c_port = port
        c_hash = ffi.NULL if gridHash is None else ffi.new(
            "char[]", gridHash.encode())

        # Flatten ranges
        c_ranges = ffi.new("size_t[]", len(ranges) * 2)
        for i, (lo, hi) in enumerate(ranges):
            if not lo < hi:
                raise ValueError(f"Invalid range {lo, hi}: expected lo < hi.")
            c_ranges[i * 2] = lo
            c_ranges[i * 2 + 1] = hi
            self.__shape.append(hi - lo)

        request = ffi.new("gribjump_path_extraction_request_t**")
        lib.gribjump_new_request_from_path(
            request, c_path, c_scheme, c_offset, c_host, c_port, c_ranges, len(ranges) * 2, c_hash)
        self.__request = ffi.gc(request[0], lib.gribjump_delete_path_request)

    @property
    def shape(self):
        return self.__shape

    @property
    def ctype(self):
        return self.__request

    @property
    def ranges(self) -> list[tuple[int, int]]:
        """
        Return the ranges as a list of tuples.
        """
        return self.__ranges


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

    def __init__(self, gribjump: CData, requests: list[ExtractionRequest], ctx: CData):
        self._shapes = [r.shape for r in requests]
        iterator = ffi.new('gribjump_extractioniterator_t**')
        c_requests = ffi.new(
            'gribjump_extraction_request_t*[]', [r.ctype for r in requests])

        lib.gribjump_extract(gribjump, c_requests,
                             len(requests), ctx, iterator)
        self._iterator = ffi.gc(
            iterator[0], lib.gribjump_extractioniterator_delete)

    def __iter__(self):
        """
        Iterate over the results of the extraction.
        """
        result_c = ffi.new('gribjump_extraction_result_t**')
        i = 0
        while lib.gribjump_extractioniterator_next(self._iterator, result_c) == lib.GRIBJUMP_ITERATOR_SUCCESS:
            # Takes ownership of the result
            yield ExtractionResult(result_c[0], self._shapes[i])
            i += 1

    def dump_legacy(self):
        """
        Dump the iterator into its unwieldy legacy format.
        This exists for backwards compatibility with the old pygribjump interface, but it is not 
        recommended and will be removed in the future.

        Original dimensions:
        [i]          : ith request
        [i][j]       : jth field from this request. This is always a single element now, making this dimension pointless.
        [i][j][k]    : kth range requested from this field.
        [i][j][k][0] : the list of values extracted for this range
        [i][j][k][1] : the mask (list of uint64) extracted for this range
        """
        res = []  # of size nrequests

        for i, result in enumerate(self):
            values = result.copy_values()
            masks = result.copy_masks()
            li = [[]]  # pointless outer dimension for legacy reasons.
            for j in range(len(values)):
                pair = (values[j], masks[j])
                li[0].append(pair)
            res.append(li)

        return res

    def dump_values(self) -> list[list[np.ndarray]]:
        return [result.copy_values() for result in self]


class ExtractionIteratorFromPath(ExtractionIterator):
    def __init__(self, gribjump: CData, requests: list[ExtractionRequest], ctx: CData):
        self._shapes = [r.shape for r in requests]
        iterator = ffi.new('gribjump_extractioniterator_t**')
        c_requests = ffi.new(
            'gribjump_path_extraction_request_t*[]', [r.ctype for r in requests])

        lib.gribjump_extract_from_paths(gribjump, c_requests,
                                        len(requests), ctx, iterator)
        self._iterator = ffi.gc(
            iterator[0], lib.gribjump_extractioniterator_delete)


# Extraction iterator produced by a single request of arbitrary cardinality.
class ExtractionSingleIterator (ExtractionIterator):

    def __init__(self, gribjump: CData, request: dict[str, str | list], ranges: list[tuple[int, int]], gridHash: str, ctx: CData):
        self.__shape = []  # All results will have the same shape

        # @todo: Have pymetkit handle the request manipulation
        reqstr = "retrieve," + multivalued_dic_to_request(request)
        c_reqstr = ffi.new("char[]", reqstr.encode())
        c_hash = ffi.NULL if gridHash is None else ffi.new(
            "char[]", gridHash.encode())

        c_ranges = ffi.new('size_t[]', len(ranges)*2)
        c_ranges_size = len(ranges)*2
        for i, r in enumerate(ranges):
            c_ranges[i*2] = r[0]
            c_ranges[i*2+1] = r[1]
            self.__shape.append(r[1] - r[0])

        iterator = ffi.new('gribjump_extractioniterator_t**')
        lib.gribjump_extract_single(
            gribjump, c_reqstr, c_ranges, c_ranges_size, c_hash, ctx, iterator)
        self._iterator = ffi.gc(
            iterator[0], lib.gribjump_extractioniterator_delete)

    def __iter__(self):
        """
        Iterate over the results of the extraction.
        """
        result_c = ffi.new('gribjump_extraction_result_t**')
        i = 0
        while lib.gribjump_extractioniterator_next(self._iterator, result_c) == lib.GRIBJUMP_ITERATOR_SUCCESS:
            yield ExtractionResult(result_c[0], self.__shape)
            i += 1


class GribJump:
    """This is the main container class for accessing GribJump"""

    def __init__(self):
        gribjump = ffi.new('gribjump_handle_t**')
        lib.gribjump_new_handle(gribjump)

        # Set free function
        self.__gribjump = ffi.gc(gribjump[0], lib.gribjump_delete_handle)

    @overload
    def extract(self, requests: ExtractionRequest,
                ctx=None) -> ExtractionIterator: ...

    @overload
    def extract(self, requests: list[ExtractionRequest],
                ctx=None) -> ExtractionIterator: ...

    # todo: interface for one high cardinality request
    def extract(self, polyrequest: list[tuple[str, list[tuple[int, int]]]], ctx=None) -> ExtractionIterator:
        """
        Extract a list of requests.
        Note: if a list of requests is passed, they must all have cardinality 1.
        If a single request is passed, it may have any cardinality.
        Parameters
        ----------
        polyrequest : [
            (req1_str, [(lo, hi), (lo, hi), ...])
            (req2_str, [(lo, hi), (lo, hi), ...])
            ...
        ]

        """
        ctx = merge_default_context(ctx, "pygribjump_extract")

        # must be a list
        if not isinstance(polyrequest, list):
            raise ValueError(
                "Polyrequest should be a list of tuples or ExtractionRequest objects")

        if len(polyrequest) == 0:
            raise ValueError("Polyrequest should not be empty")

        # check type of first element to see if we must unpack
        if isinstance(polyrequest[0], tuple):
            requests = self._unpack_polyrequest(polyrequest)

        elif isinstance(polyrequest[0], ExtractionRequest):
            requests = polyrequest

        else:
            raise ValueError(
                "Polyrequest should be a list of tuples or ExtractionRequest objects")

        logctx = json.dumps(ctx)
        logctx_c = ffi.new('const char[]', logctx.encode('ascii'))
        return ExtractionIterator(self.ctype, requests, logctx_c)

    def extract_from_paths(self, requests: list[PathExtractionRequest], ctx=None) -> ExtractionIterator:
        ctx = merge_default_context(ctx, "pygribjump_extract")
        logctx = json.dumps(ctx)
        logctx_c = ffi.new('const char[]', logctx.encode('ascii'))
        return ExtractionIteratorFromPath(self.ctype, requests, logctx_c)

    def extract_single(self, request: dict[str, str | list], ranges: list[tuple[int, int]], gridHash: str = None, ctx=None) -> ExtractionSingleIterator:
        """
        Extract a single request with arbitrary cardinality.
        Parameters
        ----------
        request : dict
            The request mars-retrieve string.
        ranges : [(lo, hi), (lo, hi), ...]
            The ranges to extract.
        hash : str
            The hash of the request.
        """

        ctx = merge_default_context(ctx, "pygribjump_extract_single")

        logctx = json.dumps(ctx)
        logctx_c = ffi.new('const char[]', logctx.encode('ascii'))
        return ExtractionSingleIterator(self.ctype, request, ranges, gridHash, logctx_c)

    # Convenience functions for extracting from masks and indices
    def extract_from_mask(self, requests: list[dict[str, str]], mask: np.ndarray, gridHash: str = None, ctx=None) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a 1D bitmask.
        The mask is a 1D array of booleans, where True indicates the value should be extracted.
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        extraction_requests = [ExtractionRequest.from_mask(
            r, mask, gridHash) for r in requests]
        return self.extract(extraction_requests, ctx)

    def extract_from_indices(self, requests: list[dict[str, str]], indices: np.ndarray, gridHash: str = None, ctx=None) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a 1D list of indices.
        Each of the indicies corresponds to a single point to be extracted.
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        extraction_requests = [ExtractionRequest.from_indices(
            request, indices, gridHash) for request in requests]
        return self.extract(extraction_requests, ctx)

    def extract_from_ranges(self, requests: list[dict[str, str]], ranges: list[tuple[int, int]], gridHash: str = None, ctx=None) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a list of ranges.
        Each range is a tuple representing the half-open interval [start, end)
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        extraction_requests = [ExtractionRequest(
            request, ranges, gridHash) for request in requests]
        return self.extract(extraction_requests, ctx)

    def _unpack_polyrequest(self, polyrequest) -> list[ExtractionRequest]:
        requests = []
        for item in polyrequest:
            if len(item) == 2:
                reqstr, ranges = item
                hash = None
            elif len(item) == 3:
                reqstr, ranges, hash = item
            else:
                raise ValueError(
                    "Polyrequest should be a list of tuples of length 2 or 3")
            requests.append(ExtractionRequest(reqstr, list(ranges), hash))
        return requests

    def axes(self, req: dict[str, str], level: int = 3, ctx: str = None) -> dict[str, list[str]]:

        ctx = merge_default_context(ctx, "pygribjump_axes")
        logctx = json.dumps(ctx)
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
            values.append([ffi.string(values_out[i]).decode('ascii')
                          for i in range(nvalues[0])])

        keys = [ffi.string(keys[i]).decode('ascii') for i in range(nkeys[0])]
        return dict(zip(keys, values))

    @property
    def ctype(self):
        return self.__gribjump


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

    def __init__(self, result_in: CData, shape: list[int]):

        # required to unpack the result (need dimensions of ranges)
        self.__shape = shape
        self.__mask_shape = [(size + 63) // 64 for size in self.__shape]
        # Takes ownership of the result
        self.__result = ffi.gc(result_in, lib.gribjump_delete_result)

        # Pointers to a buffer which owns the data, and will be gc'd by cffi
        self.__values_cdata = None
        self.__mask_cdata = None

        # Views of the data
        self.__view_values = None
        self.__view_masks = None

        self.__view_values_flat = None
        self.__view_masks_flat = None

    @property
    def values(self) -> list[np.ndarray]:
        # Note: This is a view of the data, so must not outlive the result object.
        if (self.__view_values is None):
            self.__view_values = self._view_values()
        return self.__view_values

    @property
    def masks(self) -> list[np.ndarray]:
        # Note: This is a view of the data, so must not outlive the result object.
        if (self.__view_masks is None):
            self.__view_masks = self._view_masks()
        return self.__view_masks

    @property
    def values_flat(self) -> np.ndarray:
        # Note: This is a view of the data, so must not outlive the result object.
        if (self.__view_values_flat is None):
            self.__view_values_flat = self._view_values_flat()
        return self.__view_values_flat

    @property
    def masks_flat(self) -> np.ndarray:
        # Note: This is a view of the data, so must not outlive the result object.
        if (self.__view_masks_flat is None):
            self.__view_masks_flat = self._view_masks_flat()
        return self.__view_masks_flat

    def compute_bool_masks(self) -> list[np.ndarray]:
        """
        Return the mask as a list of boolean arrays.
        """
        result = []
        count = 0
        for i, mask in enumerate(self.masks):
            nvalues = self.__shape[i]
            boolmask = np.zeros(nvalues, dtype=bool)
            for j in range(len(mask)):
                for k in range(64):
                    bp = j*64 + k
                    if bp >= nvalues:  # We are done
                        break
                    # Check if the bit is set
                    boolmask[bp] = (mask[j] >> np.uint64(k)) & np.uint64(1)
                    count += 1
            result.append(boolmask)

        assert count == sum(
            self.__shape), f"Count mismatch: {count} != {sum(self.__shape)}"
        return result

    def copy_values(self) -> list[np.ndarray]:
        return [v.copy() for v in self.values]

    def copy_masks(self) -> list[np.ndarray]:
        return [m.copy() for m in self.masks]

    def _load_values(self):
        if self.__values_cdata is not None:
            return

        nvalues = sum(self.__shape)
        self.__values_cdata = ffi.new("double[]", nvalues)
        values_ptr = ffi.new("double*[1]")
        values_ptr[0] = self.__values_cdata

        lib.gribjump_result_values(self.__result, values_ptr, nvalues)

    def _view_values_flat(self) -> np.ndarray:
        """
        Return the values as a single flat numpy array.
        This is a view of the data, so must not outlive the result object.
        """
        self._load_values()

        # Create a view of the data
        nvalues = sum(self.__shape)
        return np.frombuffer(ffi.buffer(self.__values_cdata, nvalues * ffi.sizeof('double')), dtype=np.float64)

    def _view_values(self) -> list[np.ndarray]:
        """
        Return the values as a list of numpy arrays.
        This is a view of the data, so must not outlive the result object.
        """

        view = self._view_values_flat()

        # Split into a list of views, one for each range
        indices = list(accumulate(self.__shape))[:-1]
        return np.split(view, indices)

    def _load_masks(self):
        if self.__mask_cdata is not None:
            return

        # Bit mask is returned as an array of uint64
        nvalues = sum(self.__mask_shape)
        self.__mask_cdata = ffi.new('unsigned long long[]', nvalues)
        mask_ptr = ffi.new('unsigned long long*[1]')
        mask_ptr[0] = self.__mask_cdata

        lib.gribjump_result_mask(self.__result, mask_ptr, nvalues)

    def _view_masks_flat(self) -> np.ndarray:
        """
        Return the mask as a single flat numpy array.
        This is a view of the data, so must not outlive the result object.
        """
        self._load_masks()

        # Create a view of the data
        nvalues = sum(self.__mask_shape)
        return np.frombuffer(ffi.buffer(self.__mask_cdata, nvalues * ffi.sizeof('unsigned long long')), dtype=np.uint64)

    def _view_masks(self) -> list[np.ndarray]:
        """
        Return the mask as a list of numpy arrays.
        This is a view of the data, so must not outlive the result object.
        """
        view = self._view_masks_flat()

        # Split into a list of views, one for each range
        indices = list(accumulate(self.__mask_shape))[:-1]
        return np.split(view, indices)

# utils


def rangestr_to_list(rangestr: str) -> list[tuple[int, int]]:
    """
    Convert a range string to a list of ranges.
    e.g. "0-6,7-12" -> [(0, 6), (7, 12)]
    """
    return [tuple(map(int, r.split('-'))) for r in rangestr.split(',')]


def list_to_rangestr(ranges):
    """
    Convert a list of ranges to a range string.
    """
    return ','.join(['-'.join(map(str, r)) for r in ranges])


def dic_to_request(dic: dict[str, str]) -> str:
    # e.g. {"class":"od", "expver":"0001", "levtype":"pl"} -> "class=od,expver=0001,levtype=pl"
    return ','.join(['='.join([k, v]) for k, v in dic.items()])


def multivalued_dic_to_request(dic: dict[str, str | list]) -> str:
    # e.g. {"class":"od", "expver":"0001", "step":[1, 2, 3]} -> "class=od,expver=0001,step=1/2/3"
    out = ""
    for k, v in dic.items():
        if isinstance(v, list):
            out += f"{k}=" + '/'.join([str(i) for i in v]) + ","
        else:
            out += f"{k}={v},"
    # Remove the last comma
    out = out[:-1]
    return out

# def multivalued_dic_to_request(dic : dict [str, str | list[str]]) -> str:
#     # e.g. {"class":"od", "expver":"0001", "step":[1, 2, 3]} -> "class=od,expver=0001,step=1/2/3"
#     return ','.join(['='.join([k, '/'.join(map(str, v))]) for k, v in dic.items()])


def version() -> str:
    return __version__


def library_version() -> str:
    return ffi.string(lib.gribjump_version()).decode("utf-8")


def default_context(action: str) -> dict[str, str]:
    """
    Return the default context for the library.
    This is a dictionary with the following keys
    """

    def try_get(function: Callable[[], str], default: str) -> str:
        try:
            return function()
        except Exception as e:
            return default

    return {
        "source": "pygribjump",
        "action": action,
        "pygribjump_version": __version__,
        "gribjump_version": library_version(),
        "user": try_get(getuser, "unknown"),
        "hostname": try_get(gethostname, "unknown"),
    }


def merge_default_context(ctx: dict[str, str], action: str) -> dict[str, str]:
    """
    Insert the default context into the user provided context.
    """
    if ctx is None:
        return default_context(action)

    # Merge the two dictionaries
    defaults = default_context(action)
    out_ctx = ctx.copy()
    for k, v in defaults.items():
        if k not in ctx:
            out_ctx[k] = v

    return out_ctx
