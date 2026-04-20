import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_app = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', 'streamlit_app.py')
with open(_app) as _f:
    exec(compile(_f.read(), _app, 'exec'), globals())