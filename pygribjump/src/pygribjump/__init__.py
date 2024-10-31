from pathlib import Path

from .pygribjump import *

filename = Path(__file__).parent / "VERSION"
print(filename)
 
with open(filename) as version_file:
    __version__ = version_file.read().strip()