# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging
import warnings
from collections.abc import Collection
from typing import Any, Optional

import numpy as np

from pygribjump._internal import (
    _GribJump,
    _gribjump_build_version,
    init_bindings,
    version_info,
)
from pygribjump._internal.pygribjump_internal import ContextMapper, RequestMapper, deprecated_aliases
from pygribjump.pygribjump_iterator import ExtractionIterator
from pygribjump.pygribjump_type import (
    ExtractionRequest,
    MarsSelection,
    PathExtractionRequest,
    Range,
)


class GribJump:
    """
    This is the main container class for accessing GribJump.

    Parameters
    ----------
    None

    Returns
    -------
    :returns: GribJump object

    Examples
    --------
    >>> gribjump = pygribjump.GribJump()
    >>> for result in gribjump.extract_from_ranges([{"class": "od", "expver": "0001"}], [(0, 10)]):
    ...     print(result.values)

    Or leveraging the context manager:

    >>> with pygribjump.GribJump() as gribjump:
    ...     # Call methods of gribjump
    ...     pass
    """

    def __init__(self) -> None:
        init_bindings()
        self.logger = logging.getLogger(__name__ + ".GribJump")
        self.gribjump = _GribJump()

    def __enter__(self) -> "GribJump":
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        pass

    @deprecated_aliases(polyrequest="requests")
    def extract(
        self,
        requests: list[ExtractionRequest] | list[tuple],
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract a list of requests.

        Parameters
        ----------
        `requests`: `list[ExtractionRequest]`
            The requests to extract.

            A list of tuples of the form `(request, ranges)` or
            `(request, ranges, grid_hash)` is also accepted, but deprecated: build
            `ExtractionRequest` objects instead.
        `ctx`: `dict`, *optional*
            Additional log context handed over to the gribjump library.

        Returns
        -------
        :returns: `ExtractionIterator` yielding one `ExtractionResult` per matched field.

        Note
        ----
        Every request in the list must have cardinality 1. Use `extract_single` for a
        request of arbitrary cardinality.

        Examples
        --------
        >>> requests = [ExtractionRequest(request, [(0, 10), (20, 30)])]
        >>> for result in gribjump.extract(requests):
        ...     print(result.values)
        """

        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of tuples or ExtractionRequest objects")

        if len(requests) == 0:
            raise ValueError("Requests should not be empty")

        if isinstance(requests[0], tuple):
            warnings.warn(
                "Passing (request, ranges[, grid_hash]) tuples to extract() is deprecated "
                "and will be removed in a future release. Pass ExtractionRequest objects "
                "instead, e.g. [ExtractionRequest(request, ranges) for request, ranges in ...], "
                "or use extract_from_ranges().",
                DeprecationWarning,
                stacklevel=3,
            )
            requests = self._unpack_polyrequest(requests)
        elif not isinstance(requests[0], ExtractionRequest):
            raise ValueError("Requests should be a list of tuples or ExtractionRequest objects")

        return ExtractionIterator(
            self.gribjump.extract(
                [request._request for request in requests],
                self._context(ctx, "pygribjump_extract"),
            ),
            _internal=True,
        )

    def extract_from_paths(
        self,
        requests: list[PathExtractionRequest],
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract from GRIB messages at known locations.
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of PathExtractionRequest objects")

        return ExtractionIterator(
            self.gribjump.extract_from_paths(
                [request._request for request in requests],
                self._context(ctx, "pygribjump_extract"),
            ),
            _internal=True,
        )

    @deprecated_aliases(gridHash="grid_hash")
    def extract_single(
        self,
        request: MarsSelection | str,
        ranges: Collection[Range],
        grid_hash: str | None = None,
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract a single request of arbitrary cardinality.

        Parameters
        ----------
        `request`: `MarsSelection` | `str`
            The MARS selection identifying the fields to extract from.
        `ranges`: `list[tuple[int, int]]`
            The ranges to extract, each resembling the half-open interval [lo, hi).
        `grid_hash`: `str`, *optional*
            Hash of the grid the values are expected to be defined on.
        `ctx`: `dict`, *optional*
            Additional log context handed over to the gribjump library.
        """
        return ExtractionIterator(
            self.gribjump.extract_single(
                RequestMapper.to_retrieve_string(request),
                RequestMapper.to_ranges(ranges),
                grid_hash if grid_hash is not None else "",
                self._context(ctx, "pygribjump_extract_single"),
            ),
            _internal=True,
        )

    @deprecated_aliases(gridHash="grid_hash")
    def extract_from_mask(
        self,
        requests: list[MarsSelection],
        mask: np.ndarray,
        grid_hash: str | None = None,
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a 1D bitmask.

        The mask is a 1D array of booleans, where True indicates the value should be extracted.
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        return self.extract(
            [ExtractionRequest.from_mask(request, mask, grid_hash) for request in requests],
            ctx,
        )

    @deprecated_aliases(gridHash="grid_hash")
    def extract_from_indices(
        self,
        requests: list[MarsSelection],
        indices: np.ndarray,
        grid_hash: str | None = None,
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a 1D list of indices.

        Each of the indices corresponds to a single point to be extracted.
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        return self.extract(
            [ExtractionRequest.from_indices(request, indices, grid_hash) for request in requests],
            ctx,
        )

    @deprecated_aliases(gridHash="grid_hash")
    def extract_from_ranges(
        self,
        requests: list[MarsSelection],
        ranges: Collection[Range],
        grid_hash: str | None = None,
        ctx: Optional[dict[str, Any]] = None,
    ) -> ExtractionIterator:
        """
        Extract values from a list of requests, with the region to be extracted defined by a list of ranges.

        Each range is a tuple representing the half-open interval [lo, hi).
        """
        if not isinstance(requests, list):
            raise ValueError("Requests should be a list of dictionaries")

        return self.extract(
            [ExtractionRequest(request, ranges, grid_hash) for request in requests],
            ctx,
        )

    @deprecated_aliases(req="request")
    def axes(
        self,
        request: MarsSelection | str,
        level: int = 3,
        ctx: Optional[dict[str, Any]] = None,
    ) -> dict[str, list[str]]:
        """
        Return the axes of the data matching the given MARS selection.

        Parameters
        ----------
        `request`: `MarsSelection` | `str`
            The MARS selection to be inspected.
        `level`: `int`, *optional*, default: `3`
            The schema level up to which the axes are collected.
        `ctx`: `dict`, *optional*
            Additional log context handed over to the gribjump library.

        Returns
        -------
        :returns: `dict[str, list[str]]` mapping MARS keys to the values available.
        """
        return self.gribjump.axes(
            RequestMapper.to_request_string(request),
            level,
            self._context(ctx, "pygribjump_axes"),
        )

    def scan(
        self,
        paths: list[str],
        ctx: Optional[dict[str, Any]] = None,
    ) -> int:
        """
        Scan the given files, generating and caching the extraction metadata.

        Returns
        -------
        :returns: `int` the number of fields scanned.
        """
        return self.gribjump.scan([str(path) for path in paths], self._context(ctx, "pygribjump_scan"))

    def scan_request(
        self,
        request: MarsSelection | str,
        byfiles: bool = False,
        ctx: Optional[dict[str, Any]] = None,
    ) -> int:
        """
        Scan all fields matching the given MARS selection.

        Returns
        -------
        :returns: `int` the number of fields scanned.
        """
        return self.gribjump.scan_request(
            RequestMapper.to_retrieve_string(request),
            byfiles,
            self._context(ctx, "pygribjump_scan"),
        )

    def _unpack_polyrequest(self, polyrequest: list[tuple]) -> list[ExtractionRequest]:
        requests = []
        for item in polyrequest:
            if len(item) == 2:
                request, ranges = item
                grid_hash = None
            elif len(item) == 3:
                request, ranges, grid_hash = item
            else:
                raise ValueError("Polyrequest should be a list of tuples of length 2 or 3")
            requests.append(ExtractionRequest(request, list(ranges), grid_hash))
        return requests

    def _context(self, ctx: Optional[dict[str, Any]], action: str) -> str:
        return ContextMapper.to_json(ctx, action, version(), library_version())

    def __repr__(self) -> str:
        return repr(self.gribjump)


def version() -> str:
    """The version pygribjump has been built against."""
    return _gribjump_build_version


def library_version() -> str:
    """The version of the loaded libgribjump."""
    for name, lib_version, _, _ in version_info():
        if name == "gribjump":
            return lib_version
    raise RuntimeError("Could not determine the version of the loaded libgribjump.")


# --------------------------------------------------------------------------------------
# utils


def dic_to_request(dic: MarsSelection) -> str:
    """
    Convert a MARS selection to its request string.

    e.g. {"class":"od", "expver":"0001", "levtype":"pl"} -> "class=od,expver=0001,levtype=pl"

    Values may be single values or collections of values:

    e.g. {"class":"od", "step":[1, 2, 3]} -> "class=od,step=1/2/3"
    """
    return RequestMapper.to_request_string(dic)
