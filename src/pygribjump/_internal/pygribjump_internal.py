# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import functools
import json
import operator
import warnings
from collections.abc import Collection
from getpass import getuser
from socket import gethostname
from typing import Any, Callable, Optional

from pygribjump_bindings import pygribjump_bindings as pygribjump_internal


def deprecated_aliases(**aliases: str) -> Callable:
    """
    Decorator accepting the keyword argument names of the (cffi based) pygribjump <= 0.13.

    Parameters
    ----------
    `**aliases`
        Mapping of the deprecated keyword argument name to its current name.

    Examples
    --------
    >>> @deprecated_aliases(gridHash="grid_hash")
    ... def extract(request, grid_hash=None): ...
    """

    def decorator(function: Callable) -> Callable:
        @functools.wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for deprecated, current in aliases.items():
                if deprecated not in kwargs:
                    continue
                if current in kwargs:
                    raise TypeError(
                        f"{function.__name__}() got both '{current}' and its deprecated alias '{deprecated}'"
                    )
                warnings.warn(
                    f"'{deprecated}' is deprecated and will be removed in a future release, "
                    f"use '{current}' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                kwargs[current] = kwargs.pop(deprecated)
            return function(*args, **kwargs)

        return wrapper

    return decorator

InternalMarsSelection = dict[str, str | Collection[str]]
"""
This is the internal representation of a MARS selection

This is a key-value map, mapping MARS keys to a string resembling values or value lists.
"""

InternalRanges = list[tuple[int, int]]
"""
This is the internal representation of the ranges to be extracted.

Each range is the half-open interval [lo, hi).
"""


class RequestMapper:
    """
    Mapper translating MARS selections into the request strings expected by the bindings.
    """

    @classmethod
    def to_request_string(cls, request: InternalMarsSelection | str) -> str:
        """
        Map a MARS selection to its string representation.

        Parameters
        ----------
        `request`: `dict` | `str`
            The MARS selection. Values may be single values or collections of values.

        Returns
        -------
        `str` resembling the MARS selection, e.g. `class=od,expver=0001,step=1/2/3`

        Examples
        --------
        >>> RequestMapper.to_request_string({"class": "od", "step": [1, 2, 3]})
        'class=od,step=1/2/3'
        """

        if isinstance(request, str):
            return request

        if not isinstance(request, dict):
            raise ValueError("Request: Unknown request type, must be str or dict.")

        entries = []
        for key, values in request.items():
            if isinstance(values, (str, int, float)):
                entries.append(f"{key}={values}")
            elif isinstance(values, Collection):
                entries.append(f"{key}=" + "/".join(str(value) for value in values))
            else:
                raise ValueError(
                    f"Unknown type for key: {key}. Type must be int, float, str or a collection of those."
                )

        return ",".join(entries)

    @classmethod
    def to_retrieve_string(cls, request: InternalMarsSelection | str) -> str:
        """
        Map a MARS selection to a full `retrieve` verb request string.
        """
        return "retrieve," + cls.to_request_string(request)

    @classmethod
    def to_ranges(cls, ranges: Collection[tuple[int, int]]) -> InternalRanges:
        """
        Validate and normalise the ranges to be extracted.
        """
        if not ranges:
            raise ValueError(f"Must provide at least one range but found {ranges=}")

        result = []
        for r in ranges:
            lo, hi = r
            try:
                lo = operator.index(lo)
                hi = operator.index(hi)
            except TypeError as error:
                raise TypeError(
                    f"Found invalid range {r}: Expected integer bounds for ranges [lo, hi)."
                ) from error
            if lo < 0:
                raise ValueError(f"Found invalid range {r}: Expected non-negative bounds.")
            if not lo < hi:
                raise ValueError(f"Found invalid range {r}: Expected lo < hi for ranges [lo, hi).")
            result.append((lo, hi))

        return result


class ContextMapper:
    """
    Mapper creating the log context handed over to the gribjump library.
    """

    @classmethod
    def default_context(cls, action: str, version: str, library_version: str) -> dict[str, str]:
        def try_get(function: Callable[[], str], default: str) -> str:
            try:
                return function()
            except Exception:
                return default

        return {
            "source": "pygribjump",
            "action": action,
            "pygribjump_version": version,
            "gribjump_version": library_version,
            "user": try_get(getuser, "unknown"),
            "hostname": try_get(gethostname, "unknown"),
        }

    @classmethod
    def to_json(cls, ctx: Optional[dict[str, Any]], action: str, version: str, library_version: str) -> str:
        """
        Merge the user provided context with the default context and serialise it to JSON.
        """
        out_ctx = cls.default_context(action, version, library_version)

        if ctx is not None:
            if not isinstance(ctx, dict):
                raise ValueError("Context: Unknown context type, must be dict.")
            out_ctx.update(ctx)

        return json.dumps(out_ctx)
