"""
A Python packge to automatically collect distributed simulation output (snapshots) from PENCIL.

Written by Thomas Hartlep, Bay Area Environmental Research Institute, Moffet Field, CA
"""

# Import
from pySnapCollate.utilities import generate_full_version_info
from importlib import metadata
__version__ = metadata.version('pySnapCollate')
__full_version_info__ = generate_full_version_info(__version__, __path__[0]+'/..')
