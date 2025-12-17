"""
A Python package to automatically collect distributed simulation output (snapshots) from PENCIL and combine data into non-parallel data files for easy archiving and analysis.

Written by Thomas Hartlep, Bay Area Environmental Research Institute, Moffet Field, CA
"""

# Import
from .utils import generate_full_version_info
from importlib import metadata
__version__ = metadata.version('pySnapCollate')
__full_version_info__ = generate_full_version_info(__version__, __path__[0]+'/..')
