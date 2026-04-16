import warnings
warnings.warn(
    "Importing ramp_client.py directly is deprecated; use lib.ramp_client instead.",
    DeprecationWarning,
    stacklevel=2,
)
from lib.ramp_client import *
