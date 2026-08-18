import sys
from unittest.mock import patch

from odoo_hybrid.__main__ import _BUILTIN_LIMITS, _apply_config, _build_parser, _split_argv


class TestSplitArgv:
    def test_no_separator(self):
        with patch.object(sys, "argv", ["prog", "--workers-thread", "2"]):
            hybrid_argv, odoo_argv = _split_argv()
        assert hybrid_argv == ["--workers-thread", "2"]
        assert odoo_argv == []

    def test_with_separator(self):
        with patch.object(sys, "argv", ["prog", "--workers-thread", "2", "--", "-c", "odoo.conf"]):
            hybrid_argv, odoo_argv = _split_argv()
        assert hybrid_argv == ["--workers-thread", "2"]
        assert odoo_argv == ["-c", "odoo.conf"]

    def test_separator_at_start(self):
        with patch.object(sys, "argv", ["prog", "--", "-c", "odoo.conf"]):
            hybrid_argv, odoo_argv = _split_argv()
        assert hybrid_argv == []
        assert odoo_argv == ["-c", "odoo.conf"]

    def test_empty_argv(self):
        with patch.object(sys, "argv", ["prog"]):
            hybrid_argv, odoo_argv = _split_argv()
        assert hybrid_argv == []
        assert odoo_argv == []


class TestParser:
    def test_defaults(self):
        args = _build_parser().parse_args([])
        assert args.workers_thread == 1
        assert args.workers_cron == 0
        assert args.workers_gevent == 1
        assert args.http_port == 8069
        assert args.gevent_port == 8072
        assert args.host == "localhost"
        assert args.preload is False
        # These must default to None so _apply_config detects "not set on CLI"
        assert args.limit_memory_soft is None
        assert args.limit_memory_hard is None
        assert args.limit_request is None
        assert args.limit_time_real is None
        assert args.limit_time_real_cron is None

    def test_workers_thread_explicit(self):
        args = _build_parser().parse_args(["--workers-thread", "3"])
        assert args.workers_thread == 3

    def test_workers_cron_explicit(self):
        args = _build_parser().parse_args(["--workers-cron", "2"])
        assert args.workers_cron == 2

    def test_limit_memory_soft_explicit(self):
        args = _build_parser().parse_args(["--limit-memory-soft", "1073741824"])
        assert args.limit_memory_soft == 1073741824

    def test_http_port_explicit(self):
        args = _build_parser().parse_args(["--http-port", "8080"])
        assert args.http_port == 8080

    def test_preload_flag(self):
        args = _build_parser().parse_args(["--preload"])
        assert args.preload is True


class TestApplyConfig:
    def test_cli_overrides_builtin(self, cfg, default_args):
        cfg["limit_time_real"] = 0  # absent from odoo.conf
        default_args.limit_time_real = 30.0
        _apply_config(default_args)
        # limit_time_real set before the unconditional overwrites, so check directly
        assert cfg["limit_time_real"] == 30.0

    def test_conf_used_when_cli_absent(self, cfg, default_args):
        cfg["limit_request"] = 1000  # non-zero → treated as "set in odoo.conf"
        default_args.limit_request = None
        _apply_config(default_args)
        assert cfg["limit_request"] == 1000

    def test_builtin_used_when_both_absent(self, cfg, default_args):
        cfg["limit_time_real"] = 0  # falsy → treated as absent from odoo.conf
        default_args.limit_time_real = None
        _apply_config(default_args)
        assert cfg["limit_time_real"] == _BUILTIN_LIMITS["limit_time_real"]

    def test_cli_overrides_conf(self, cfg, default_args):
        cfg["limit_request"] = 1000  # odoo.conf value
        default_args.limit_request = 500  # CLI overrides
        _apply_config(default_args)
        assert cfg["limit_request"] == 500

    def test_always_zeroes_limit_memory_soft(self, cfg, default_args):
        default_args.limit_memory_soft = 9999
        _apply_config(default_args)
        assert cfg["limit_memory_soft"] == 0

    def test_always_zeroes_workers(self, cfg, default_args):
        cfg["workers"] = 4
        _apply_config(default_args)
        assert cfg["workers"] == 0
        assert cfg["max_cron_threads"] == 0

    def test_http_enable_always_true(self, cfg, default_args):
        cfg["http_enable"] = False
        _apply_config(default_args)
        assert cfg["http_enable"] is True

    def test_sets_network_from_args(self, cfg, default_args):
        default_args.http_port = 9000
        default_args.host = "0.0.0.0"
        _apply_config(default_args)
        assert cfg["http_port"] == 9000
        assert cfg["http_interface"] == "0.0.0.0"
