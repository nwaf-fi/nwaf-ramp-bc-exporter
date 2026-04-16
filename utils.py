import warnings
warnings.warn(
    "Importing utils.py directly is deprecated; use lib.utils instead.",
    DeprecationWarning,
    stacklevel=2,
)
from lib.utils import *