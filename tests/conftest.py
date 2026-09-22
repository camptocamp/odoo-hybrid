# Copyright 2026 Camptocamp SA
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)


"""
Stub every odoo.* import before test modules are collected.

server.py binds `config` at module level via `from odoo.tools import config`.
The `cfg` fixture patches both that binding and `odoo.tools.config` so that
each test gets an isolated, writable config dict.
"""

import argparse
import sys
from unittest.mock import MagicMock

import pytest


class _ConfigDict(dict):
    """dict that masquerades as Odoo's configmanager."""

    def parse_config(self, argv):
        pass


# Provide a real class so _ThreadedWorkerServer can use it as a base.
class _FakeThreadedWSGIServerReloadable:
    daemon_threads = False

    def __init__(self, host, port, app):
        self.server_address = (host, port)

    def serve_forever(self):
        pass

    def shutdown(self):
        pass


_odoo_service_server = MagicMock()
_odoo_service_server.ThreadedWSGIServerReloadable = _FakeThreadedWSGIServerReloadable
_odoo_service_server.WorkerCron = MagicMock
_odoo_service_server.empty_pipe = MagicMock()
_odoo_service_server.preload_registries = MagicMock()
_odoo_service_server.set_limit_memory_hard = MagicMock()

_odoo_tools = MagicMock()
_odoo_tools.config = _ConfigDict()  # placeholder; cfg fixture replaces per-test

_odoo_tools_osutil = MagicMock()
_odoo_tools_osutil.memory_info = MagicMock(return_value=0)

for _mod, _stub in [
    ("odoo", MagicMock()),
    ("odoo.tools", _odoo_tools),
    ("odoo.tools.config", MagicMock()),
    ("odoo.tools.cache", MagicMock()),
    ("odoo.tools.misc", MagicMock()),
    ("odoo.tools.osutil", _odoo_tools_osutil),
    ("odoo.service", MagicMock()),
    ("odoo.service.server", _odoo_service_server),
    ("odoo.sql_db", MagicMock()),
    ("odoo.http", MagicMock()),
    ("psutil", MagicMock()),
]:
    sys.modules.setdefault(_mod, _stub)


_DEFAULT_CONFIG = {
    "limit_memory_soft": 2058 * 1024 * 1024,
    "limit_memory_hard": 2560 * 1024 * 1024,
    "limit_request": 65535,
    "limit_time_real": 120.0,
    "limit_time_real_cron": 600.0,
    "http_port": 8069,
    "http_interface": "",
    "gevent_port": 8072,
    "db_name": "",
    "workers": 2,
    "max_cron_threads": 2,
    "http_enable": False,
}


@pytest.fixture
def cfg(monkeypatch):
    """Fresh _ConfigDict wired into server.py (module-level) and odoo.tools."""
    import odoo.tools as _tools

    fresh = _ConfigDict(_DEFAULT_CONFIG)
    monkeypatch.setattr(_tools, "config", fresh)
    monkeypatch.setattr("odoo_hybrid.server.config", fresh)
    monkeypatch.setattr("odoo_hybrid.__main__.config", fresh)
    return fresh


@pytest.fixture
def default_args():
    """argparse.Namespace matching _build_parser() defaults."""
    return argparse.Namespace(
        workers_thread=1,
        workers_cron=0,
        workers_gevent=0,
        http_port=8069,
        gevent_port=8072,
        host="0.0.0.0",
        preload=False,
        limit_memory_soft=None,
        limit_memory_hard=None,
        limit_request=None,
        limit_time_real=None,
        limit_time_real_cron=None,
        limit_memory_soft_thread=None,
        limit_memory_hard_thread=None,
        limit_request_thread=None,
        limit_time_real_thread=None,
        limit_memory_soft_effective=2058 * 1024 * 1024,
        limit_memory_soft_gevent=None,
        odoo_argv=[],
    )
