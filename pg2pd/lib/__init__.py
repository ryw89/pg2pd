"""Package-wide helper functions, implemented in either Rust, or Python."""

import logging

log = logging.getLogger(__name__)

# Note that we intentionally import .py first so as to replace
# same-named functions with their faster Rust or C implementations.
from .py import *
from .rust import *
