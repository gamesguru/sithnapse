#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Helpers for reporting this process's memory usage."""

import os
import sys


def get_process_rss_bytes() -> int | None:
    """Return the resident set size (RSS) of this process in bytes.

    Returns `None` when the platform doesn't expose the information. On Linux
    this reads the current RSS from `/proc`; elsewhere, and as a fallback, it
    uses `ru_maxrss`, which is a high-water mark rather than the current
    value.
    """
    try:
        with open("/proc/self/statm", "rb") as f:
            fields = f.read().split()
        resident_pages = int(fields[1])
        page_size = os.sysconf("SC_PAGE_SIZE")
        return resident_pages * page_size
    except (OSError, ValueError, IndexError):
        pass

    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # `ru_maxrss` is reported in bytes on macOS and in kilobytes on Linux.
        if sys.platform == "darwin":
            return int(usage)
        return int(usage) * 1024
    except (ImportError, OSError, ValueError):
        return None
