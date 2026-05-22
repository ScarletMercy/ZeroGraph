"""ZeroGraph internal utilities."""

import copy
import logging

MISSING = object()
EMPTY_SEQ: tuple = ()

_logger = logging.getLogger(__name__)


def _deepcopy_or_warn(value):
    if value is MISSING:
        return value
    try:
        return copy.deepcopy(value)
    except Exception:
        _logger.debug("deepcopy failed for %s, using original reference", type(value).__name__)
        return value
