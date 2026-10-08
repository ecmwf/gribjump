# (C) Copyright 2026- ECMWF.
# This software is licensed under the terms of the Apache Licence Version 2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from collections.abc import Collection, Iterator

from pygribjump._internal import _ListIterator, _ListResult
from pygribjump.pygribjump_type import PathExtractionRequest, Range


class ListResult:
    """A discovered field: its complete URI, location components and MARS metadata.

    Results own their data and remain valid after the iterator or client is deleted.
    Metadata is supplied by the catalogue; no cross-backend normalisation is applied.
    """

    def __init__(self, result: _ListResult, *, _internal: bool = False) -> None:
        if not _internal:
            raise TypeError("ListResult objects are returned by GribJump.list().")
        self._result = result

    @property
    def uri(self) -> str:
        """Complete backend URI, including its offset and any query parameters."""
        return self._result.uri

    @property
    def path(self) -> str:
        return self._result.path

    @property
    def scheme(self) -> str:
        return self._result.scheme

    @property
    def host(self) -> str:
        return self._result.host

    @property
    def port(self) -> int:
        return self._result.port

    @property
    def offset(self) -> int:
        """Byte offset of the GRIB message."""
        return self._result.offset

    @property
    def length(self) -> int:
        """Length of the GRIB message in bytes."""
        return self._result.length

    @property
    def metadata(self) -> dict[str, str]:
        """A copy of the field's MARS key/value pairs."""
        return self._result.metadata

    @property
    def mars_request(self) -> str:
        """MARS request representation of the field metadata."""
        return self._result.mars_request

    def to_extraction_request(
        self, ranges: Collection[Range], grid_hash: str | None = None
    ) -> PathExtractionRequest:
        """Create a path-based request without performing another catalogue lookup.

        Uses the location components supported by PathExtractionRequest. Configure
        extraction/server routing on the GribJump client, not on this result.
        """
        return PathExtractionRequest(
            self.path, self.scheme, self.offset, self.host, self.port, ranges, grid_hash
        )

    def __repr__(self) -> str:
        return repr(self._result)


class ListIterator(Iterator[ListResult]):
    """Single-pass iterator over buffered list results; no network streaming yet."""

    def __init__(self, iterator: _ListIterator, *, _internal: bool = False) -> None:
        if not _internal:
            raise TypeError("ListIterator objects are returned by GribJump.list().")
        self._iterator = iterator

    def __iter__(self) -> "ListIterator":
        return self

    def __next__(self) -> ListResult:
        return ListResult(next(self._iterator), _internal=True)
