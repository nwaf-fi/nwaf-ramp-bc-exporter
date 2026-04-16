import warnings
warnings.warn(
    "Importing bc_export.py directly is deprecated; use lib.bc_export instead.",
    DeprecationWarning,
    stacklevel=2,
)
from lib.bc_export import *