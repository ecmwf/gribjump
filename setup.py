import os
from setuptools import setup
from pathlib import Path

setup(
    install_requires=[
        "cffi>=2.0,<3.0",
        "numpy>=1.26",
        "findlibs>=0.1.1",
        "packaging>=20.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.3",
            "pyyaml",
            "pyfdb",
        ],
        "binary": [
            # NOTE for locally built wheel, this is not going to provide a
            # satisfiable constraint -- but we dont expect local wheels to
            # be installed with this extra
            "gribjumplib==" + Path("VERSION").read_text().strip(),
        ],
    }
)
