import io
import re

from setuptools import find_packages, setup

with open("VERSION", "r") as f:
    __version__ = f.read().strip()

setup(
    name="pygribjump",
    version=__version__,
    description="Python interface to GribJump",
    url="https://github.com/ecmwf/gribjump",
    author="ECMWF",
    author_email="software.support@ecmwf.int",
    packages=find_packages(exclude=("docs", "tests")),
    include_package_data=True,
    install_requires=["cffi", "findlibs", "numpy"],
    zip_safe=False,
)
