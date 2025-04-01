from pathlib import Path
from .pygribjump import *
import importlib.metadata

__version__ = importlib.metadata.version("pygribjump")

# @todo: Update this to 0.10.0
__min_lib_version__ = "0.9.3" # Latest breaking C-API change