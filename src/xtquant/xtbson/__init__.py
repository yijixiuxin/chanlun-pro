
import sys

if sys.version_info.major == 3 and sys.version_info.minor == 6:
    from .bson36 import *
else:
    from .bson37 import *
