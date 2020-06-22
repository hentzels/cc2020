
from flask import jsonify
from server import app
#from cloudant.client import Cloudant
#from cloudant.query import Query

state = ""
try:
    from cloudant.client import Cloudant
except ImportError:
    state += " no cloudant client imported"
else:
    state += " cloudant client imported"

try:
    from cloudant.query import Query
except ImportError:
    state += " no cloudant Query imported"
else:
    state += " cloudant Query imported"

try:
    import numpy as np
except ImportError:
    state += " no numpy imported"
else:
    state += " numpy imported"


@app.route("/health")
def health():
    """health route"""
    return jsonify(state)
