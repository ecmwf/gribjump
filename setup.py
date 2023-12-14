import io
import re

from setuptools import find_packages, setup

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', io.open("pygribjump/version.py", encoding="utf_8_sig").read()
).group(1)

setup(
    name="pygribjump",
    version=__version__,
    description="Python interface to GribJump",
    url="https://github.com/ecmwf/gribjump",
    author="ECMWF",
    author_email="software.support@ecmwf.int",
    packages=find_packages(exclude=("docs", "tests")),
    include_package_data=True,
    install_requires=["cffi", "findlibs"],
    zip_safe=False,
)
