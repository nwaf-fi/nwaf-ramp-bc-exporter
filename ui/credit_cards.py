import warnings
warnings.warn(
    "Importing ui.credit_cards directly is deprecated; use app.ui.credit_cards instead.",
    DeprecationWarning,
    stacklevel=2,
)
from app.ui.credit_cards import *