from pathlib import Path
from .pygribjump import *
import importlib.metadata

__version__ = importlib.metadata.version("pygribjump")

__min_lib_version__ = "0.10.0" # Latest breaking C-API change