import io
import re

from setuptools import find_packages, setup

# VERSION file is source of truth for version
with open("VERSION", "r") as f:
    __version__ = f.read().strip()

# update the version.py file
with open("pygribjump/src/pygribjump/_version.py", "w") as f:
    f.write(f"__version__ = \"{__version__}\"\n")

setup(
    name="pygribjump",
    version=__version__,
    description="Python interface to GribJump",
    url="https://github.com/ecmwf/gribjump",
    author="ECMWF",
    author_email="software.support@ecmwf.int",
    packages = ["pygribjump"],
    package_dir={"": ".",
                "pygribjump": "./pygribjump/src/pygribjump"
    },
    include_package_data=True,
    install_requires=["cffi", "findlibs", "numpy"],
    zip_safe=False,
)
