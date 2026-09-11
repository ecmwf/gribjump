# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections.abc import Collection, Mapping

import numpy as np

from pygribjump._internal import (
    GribJumpException,
    _ExtractionRequest,
    _PathExtractionRequest,
)
from pygribjump._internal.pygribjump_internal import RequestMapper, deprecated_aliases
MarsSelection = Mapping[str, str | int | float | Collection[str | int | float]]
"""
Selection part of a MARS request.

This is a key-value map, with the data types allowed below
"""

Range = tuple[int, int]
"""
A single range to be extracted, resembling the half-open interval [lo, hi).
"""

# `GribJumpException` is created by the bindings (see src/pygribjump_bindings) and
# re-exported here: it is raised for every error reported by the gribjump library and
# derives from `RuntimeError`.
__all__ = [
    "GribJumpException",
    "ExtractionRequest",
    "PathExtractionRequest",
    "MarsSelection",
    "Range",
]


class ExtractionRequest:
    """
    A request to extract ranges of values from all fields matching a MARS selection.

    Parameters
    ----------
    `request`: `MarsSelection` | `str`
        The MARS selection identifying the field(s) to extract from.
    `ranges`: `list[tuple[int, int]]`
        The ranges to extract, each resembling the half-open interval [lo, hi).
    `grid_hash`: `str`, *optional*
        Hash of the grid the values are expected to be defined on. Extraction fails if it
        does not match the grid of the field.

    Note
    ----
    The keyword arguments `req` and `gridHash` of the cffi based pygribjump are still
    accepted, but deprecated.

    Examples
    --------
    >>> request = ExtractionRequest({"class": "od", "expver": "0001"}, [(0, 10), (20, 30)])
    """

    @deprecated_aliases(req="request", gridHash="grid_hash")
    def __init__(
        self,
        request: MarsSelection | str,
        ranges: Collection[Range],
        grid_hash: str | None = None,
    ) -> None:
        self._ranges = RequestMapper.to_ranges(ranges)
        self._shape = [hi - lo for lo, hi in self._ranges]
        self._request = _ExtractionRequest(
            RequestMapper.to_retrieve_string(request),
            self._ranges,
            grid_hash if grid_hash is not None else "",
        )

    @classmethod
    @deprecated_aliases(req="request", gridHash="grid_hash")
    def from_mask(
        cls,
        request: MarsSelection | str,
        mask: np.ndarray,
        grid_hash: str | None = None,
    ) -> "ExtractionRequest":
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

        return cls(request, ranges, grid_hash)

    @classmethod
    @deprecated_aliases(req="request", gridHash="grid_hash")
    def from_indices(
        cls,
        request: MarsSelection | str,
        points: np.ndarray,
        grid_hash: str | None = None,
    ) -> "ExtractionRequest":
        """
        Create a request from a 1D list of indices.
        """
        return cls(request, [(p, p + 1) for p in points], grid_hash)

    @property
    def shape(self) -> list[int]:
        """Number of values which will be extracted per range."""
        return self._shape

    @property
    def ranges(self) -> list[Range]:
        """The ranges as a list of tuples."""
        return self._ranges

    def indices(self) -> np.ndarray:
        """
        Return a 1d array with the indices of the values that would be retrieved.
        """
        total = sum(self._shape)
        idx_iter = (idx for (low, high) in self.ranges for idx in range(low, high))
        # Use an explicit dtype: plain `int` maps to a platform-dependent width.
        return np.fromiter(idx_iter, np.int64, count=total)

    def __repr__(self) -> str:
        return repr(self._request)


class PathExtractionRequest:
    """
    A request to extract ranges of values from a GRIB message at a known location.

    Parameters
    ----------
    `path`: `str`
        The path of the file containing the GRIB message.
    `scheme`: `str`
        The scheme of the URI, e.g. `file`.
    `offset`: `int`
        The offset of the GRIB message inside the file.
    `host`: `str`
        The host serving the file. May be empty for local files.
    `port`: `int`
        The port of the host serving the file.
    `ranges`: `list[tuple[int, int]]`
        The ranges to extract, each resembling the half-open interval [lo, hi).
    `grid_hash`: `str`, *optional*
        Hash of the grid the values are expected to be defined on.

    Note
    ----
    The keyword argument `gridHash` of the cffi based pygribjump is still accepted,
    but deprecated.
    """

    @deprecated_aliases(gridHash="grid_hash")
    def __init__(
        self,
        path: str,
        scheme: str,
        offset: int,
        host: str,
        port: int,
        ranges: Collection[Range],
        grid_hash: str | None = None,
    ) -> None:
        self._ranges = RequestMapper.to_ranges(ranges)
        self._shape = [hi - lo for lo, hi in self._ranges]
        self._request = _PathExtractionRequest(
            path,
            scheme,
            offset,
            host,
            port,
            self._ranges,
            grid_hash if grid_hash is not None else "",
        )

    @property
    def shape(self) -> list[int]:
        """Number of values which will be extracted per range."""
        return self._shape

    @property
    def ranges(self) -> list[Range]:
        """The ranges as a list of tuples."""
        return self._ranges

    def __repr__(self) -> str:
        return repr(self._request)
