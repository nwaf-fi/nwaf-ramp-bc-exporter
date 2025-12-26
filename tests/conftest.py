import os
import sys

# Ensure the repository root is on sys.path for imports during pytest runs
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
