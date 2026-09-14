# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import warnings

import findlibs

# libgribjump.so and dependencies have to be loaded prior to importing pygribjump
findlibs.load("gribjump")

from pygribjump._internal.pygribjump_internal import (
    ContextMapper,
    RequestMapper,
    deprecated_aliases,
)
from pygribjump_bindings.pygribjump_bindings import (
    ExtractionRequest as _ExtractionRequest,
)
from pygribjump_bindings.pygribjump_bindings import (
    GribJumpException,
)
from pygribjump_bindings.pygribjump_bindings import (
    ExtractionResult as _ExtractionResult,
)
from pygribjump_bindings.pygribjump_bindings import (
    ExtractionIterator as _ExtractionIterator,
)
from pygribjump_bindings.pygribjump_bindings import (
    GribJump as _GribJump,
)
from pygribjump_bindings.pygribjump_bindings import (
    PathExtractionRequest as _PathExtractionRequest,
)
from pygribjump_bindings.pygribjump_bindings import (
    __gribjump_build_version__ as _gribjump_build_version,
)
from pygribjump_bindings.pygribjump_bindings import (
    init_bindings,
    version_info,
)

init_bindings()


def _check_gribjump_version_compatibility(build_version, runtime_info):
    matches = [version for name, version, _, _ in runtime_info if name == "gribjump"]
    runtime_version = matches[0] if matches else None
    if runtime_version is None:
        raise RuntimeError(
            "pygribjump could not determine the version of the loaded libgribjump. "
            "The library may not have loaded correctly. "
            "Run 'python -m pygribjump --print-home-deps' to inspect the dependency setup."
        )
    if runtime_version != build_version:
        warnings.warn(
            f"pygribjump was built against gribjump {build_version} but the loaded "
            f"libgribjump is version {runtime_version}. "
            "Behaviour may be unexpected. "
            "Run 'python -m pygribjump --print-home-deps' to inspect which libraries "
            "were picked up, or consult the pygribjump documentation.",
            UserWarning,
            stacklevel=2,
        )


_check_gribjump_version_compatibility(_gribjump_build_version, version_info())

__all__ = [
    "init_bindings",
    "version_info",
    "GribJumpException",
    "_ExtractionRequest",
    "_PathExtractionRequest",
    "_ExtractionResult",
    "_ExtractionIterator",
    "_GribJump",
    "ContextMapper",
    "RequestMapper",
    "deprecated_aliases",
]
