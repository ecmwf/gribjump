from pathlib import Path
from .pygribjump import *

with open(Path(__file__).parent / "VERSION") as f:
    __version__ = f.read().strip()
