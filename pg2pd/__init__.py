import logging
import os
import sys

from . import lib
from .pg2pd import *

# Set up logging
log = logging.getLogger(__name__)

# Allow setting log-level from environment variable, default to info
try:
    log_level = os.environ['PG2PD_LOG']
except KeyError:
    log_level = logging.INFO

logging.basicConfig(
    stream=sys.stderr,
    level=log_level,
    format=
    '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)s:%(funcName)8s() | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S')
