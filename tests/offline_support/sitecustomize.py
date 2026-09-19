"""Propagate Jobby's offline test guard into child Python processes."""

from __future__ import annotations

import os


if os.environ.get("JOBBY_TEST_OFFLINE") == "1":
    from offline_guard import install_offline_network_guard

    install_offline_network_guard()
