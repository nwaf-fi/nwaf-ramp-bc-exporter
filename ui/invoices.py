import warnings
warnings.warn(
    "Importing ui.invoices directly is deprecated; use app.ui.invoices instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export the app-level implementation
from app.ui.invoices import *