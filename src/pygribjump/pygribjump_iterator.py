# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging
from collections.abc import Iterator
from itertools import accumulate

import numpy as np

from pygribjump._internal import (
    _ExtractionIterator,
    _ExtractionResult,
)

logger = logging.getLogger(__name__)


class ExtractionResult:
    """
    The result of an extraction on a single field.

    Note
    ----
    *This class can't be instantiated / is only returned from the underlying GribJump calls*
    """

    def __init__(self, result: _ExtractionResult, *, _internal: bool = False) -> None:
        if not _internal:
            raise TypeError("Creating an ExtractionResult from user code is not supported.")
        self._result: _ExtractionResult = result
        self._shape: list[int] | None = None

        # Lazily created, then cached: `values`/`masks` are views into the flat
        # arrays, so all accessors of one result share their memory (as they did
        # in the cffi based pygribjump).
        self._values_flat: np.ndarray | None = None
        self._masks_flat: np.ndarray | None = None
        self._values: list[np.ndarray] | None = None
        self._masks: list[np.ndarray] | None = None

    @property
    def values(self) -> list[np.ndarray]:
        """
        The extracted values, one array per requested range.

        Note
        ----
        The arrays are views into `values_flat`, they are not copies.
        """
        if self._values is None:
            indices = list(accumulate(self.shape))[:-1]
            self._values = np.split(self.values_flat, indices)
        return self._values

    @property
    def masks(self) -> list[np.ndarray]:
        """
        The bitmasks of the extracted values, one array of `uint64` per requested range.

        Note
        ----
        The mask is encoded as 64-bit unsigned integers. If N values were extracted in a
        range, the mask array contains ceil(N / 64) elements. The arrays are views into
        `masks_flat`, they are not copies.
        """
        if self._masks is None:
            indices = list(accumulate((n + 63) // 64 for n in self.shape))[:-1]
            self._masks = np.split(self.masks_flat, indices)
        return self._masks

    @property
    def values_flat(self) -> np.ndarray:
        """
        The extracted values of all ranges, as a single flat array.
        """
        if self._values_flat is None:
            self._values_flat = self._result.values_flat()
        return self._values_flat

    @property
    def masks_flat(self) -> np.ndarray:
        """
        The bitmasks of all ranges, as a single flat array of `uint64`.
        """
        if self._masks_flat is None:
            self._masks_flat = self._result.mask_flat()
        return self._masks_flat

    @property
    def shape(self) -> list[int]:
        """
        Number of values extracted per range.
        """
        if self._shape is None:
            self._shape = [self._result.nvalues(i) for i in range(self._result.nrange())]
        return self._shape

    def compute_bool_masks(self) -> list[np.ndarray]:
        """
        Return the masks as a list of boolean arrays.
        """
        result = []
        for nvalues, mask in zip(self.shape, self.masks):
            bits = np.unpackbits(mask.view(np.uint8), bitorder="little")
            result.append(bits[:nvalues].astype(bool))
        return result

    def copy_values(self) -> list[np.ndarray]:
        return [v.copy() for v in self.values]

    def copy_masks(self) -> list[np.ndarray]:
        return [m.copy() for m in self.masks]

    def __repr__(self) -> str:
        return repr(self._result)


class ExtractionIterator:
    """
    Iterator over the results of an extraction.

    Note
    ----
    *This class can't be instantiated / is only returned from the underlying GribJump calls*
    """

    def __init__(self, iterator: _ExtractionIterator, *, _internal: bool = False) -> None:
        if not _internal:
            raise TypeError("Creating an ExtractionIterator from user code is not supported.")
        self._iterator: _ExtractionIterator = iterator

    def __iter__(self) -> Iterator[ExtractionResult]:
        for result in self._iterator:
            yield ExtractionResult(result, _internal=True)

    def dump_values(self) -> list[list[np.ndarray]]:
        """
        Dump the values of all results, one list of arrays per field.
        """
        return [result.copy_values() for result in self]

    def dump_legacy(self):
        """
        Dump the iterator into its unwieldy legacy format.

        This exists for backwards compatibility with the old cffi based pygribjump
        interface, but it is not recommended and will be removed in the future.

        Original dimensions:
        [i]          : ith request
        [i][j]       : jth field from this request. This is always a single element now,
                       making this dimension pointless.
        [i][j][k]    : kth range requested from this field.
        [i][j][k][0] : the list of values extracted for this range
        [i][j][k][1] : the mask (list of uint64) extracted for this range
        """
        res = []  # of size nrequests

        for result in self:
            values = result.copy_values()
            masks = result.copy_masks()
            li = [[]]  # pointless outer dimension for legacy reasons.
            for value, mask in zip(values, masks):
                li[0].append((value, mask))
            res.append(li)

        return res
