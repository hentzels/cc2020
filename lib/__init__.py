# __init__.py
from os.path import dirname, basename, isfile
import glob
modules = glob.glob(dirname(__file__)+"/*.py")
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]

from .accessDB import getV1Page 
from .accessDB import getData
from .accessDB import formater
from .accessDB import getFields
from .accessDB import runQuery
from .accessDB import lastUpdate 

from .c4cDashboard import getFigure
from .c4cDashboard import getCompare
from .c4cDashboard import figAsHtml
