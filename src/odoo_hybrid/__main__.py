# Copyright 2026 Camptocamp SA
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
import argparse
import logging
import sys

from odoo.tools import config

_logger = logging.getLogger(__name__)
_MiB = 1024 * 1024


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="odoo-hybrid",
        usage="%(prog)s [options] [-- odoo_options]",
        description="Alternative Odoo server runner with multithreaded workers.",
    )

    # Global resource limits
    p.add_argument(
        "--limit-memory-soft",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory soft limit for all processes (default: 2058 MiB)",
    )
    p.add_argument(
        "--limit-memory-hard",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory hard limit for all processes (default: 2560 MiB)",
    )
    p.add_argument(
        "--limit-request",
        type=int,
        default=None,
        help="max requests/cron jobs per process (default: 65535)",
    )
    p.add_argument(
        "--limit-time-real",
        type=float,
        default=None,
        metavar="SECONDS",
        help="max real time per request for all processes (default: 120)",
    )
    p.add_argument(
        "--preload",
        action="store_true",
        help="preload Odoo registries in master before forking workers",
    )

    # Threaded workers
    p.add_argument(
        "--workers-thread", type=int, default=1, metavar="INT", help="number of multithreaded workers (default: 1)"
    )
    p.add_argument(
        "--limit-memory-soft-thread",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory soft limit for threaded workers (default: --limit-memory-soft)",
    )
    p.add_argument(
        "--limit-memory-hard-thread",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory hard limit for threaded workers (default: --limit-memory-hard)",
    )
    p.add_argument(
        "--limit-request-thread",
        type=int,
        default=None,
        metavar="INT",
        help="max requests per threaded worker (default: --limit-request)",
    )
    p.add_argument(
        "--limit-time-real-thread",
        type=float,
        default=None,
        metavar="SECONDS",
        help="max real time per request for threaded workers (default: --limit-time-real)",
    )

    # Cron workers
    p.add_argument(
        "--workers-cron", type=int, default=0, metavar="INT", help="number of cron worker processes (default: 0)"
    )
    p.add_argument(
        "--limit-memory-soft-cron",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory soft limit for cron workers (default: --limit-memory-soft)",
    )
    p.add_argument(
        "--limit-memory-hard-cron",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory hard limit for cron workers (default: --limit-memory-hard)",
    )
    p.add_argument(
        "--limit-request-cron",
        type=int,
        default=None,
        metavar="INT",
        help="max cron jobs per cron worker (default: --limit-request)",
    )
    p.add_argument(
        "--limit-time-real-cron",
        type=float,
        default=None,
        metavar="SECONDS",
        help="max real time per cron job (default: 600)",
    )

    # Gevent worker
    p.add_argument(
        "--workers-gevent", type=int, default=1, choices=[0, 1], help="number of gevent workers, 0 or 1 (default: 1)"
    )
    p.add_argument(
        "--limit-memory-soft-gevent",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory soft limit for gevent worker (default: --limit-memory-soft)",
    )
    p.add_argument(
        "--limit-memory-hard-gevent",
        type=int,
        default=None,
        metavar="BYTES",
        help="memory hard limit for gevent worker (default: --limit-memory-hard)",
    )

    # Network
    p.add_argument("--http-port", type=int, default=8069, metavar="INT", help="port for master process (default: 8069)")
    p.add_argument(
        "--gevent-port", type=int, default=8072, metavar="INT", help="port for gevent worker (default: 8072)"
    )
    p.add_argument("--host", default="0.0.0.0", help="IP or hostname to bind (default: 0.0.0.0)")

    return p


def _split_argv() -> tuple[list[str], list[str]]:
    argv = sys.argv[1:]
    try:
        idx = argv.index("--")
        return argv[:idx], argv[idx + 1 :]
    except ValueError:
        return argv, []


_BUILTIN_LIMITS: dict[str, int | float] = {
    "limit_memory_soft": 2058 * _MiB,
    "limit_memory_hard": 2560 * _MiB,
    "limit_request": 65535,
    "limit_time_real": 120.0,
    "limit_time_real_cron": 600.0,
}


def _apply_config(args: argparse.Namespace) -> None:
    # Three-level precedence: CLI flag > odoo.conf value > built-in default
    for attr, key in [
        ("limit_memory_soft", "limit_memory_soft"),
        ("limit_memory_hard", "limit_memory_hard"),
        ("limit_request", "limit_request"),
        ("limit_time_real", "limit_time_real"),
        ("limit_time_real_cron", "limit_time_real_cron"),
    ]:
        cli_val = getattr(args, attr)
        if cli_val is not None:
            config[key] = cli_val
        elif not config[key]:
            config[key] = _BUILTIN_LIMITS[attr]

    # CLI fully owns network binding
    config["http_port"] = args.http_port
    config["http_interface"] = args.host
    config["gevent_port"] = args.gevent_port

    # Save resolved value before zeroing; HybridMaster reads it from args
    args.limit_memory_soft_effective = config["limit_memory_soft"]
    # 0 disables Odoo's own soft-limit enforcement; we enforce it in the monitor thread
    config["limit_memory_soft"] = 0
    # prevent Odoo's own worker management from activating
    config["workers"] = 0
    config["max_cron_threads"] = 0
    config["http_enable"] = True


def main() -> None:
    hybrid_argv, odoo_argv = _split_argv()
    args = _build_parser().parse_args(hybrid_argv)
    args.odoo_argv = odoo_argv

    from odoo.service.server import load_server_wide_modules

    config.parse_config(odoo_argv)
    _apply_config(args)
    load_server_wide_modules()

    import odoo.http  # noqa: F401  # initialize wsgi app before fork

    preload_dbs = config["db_name"] if args.preload else []

    from odoo_hybrid.server import HybridMaster

    sys.exit(HybridMaster(args).run(preload_dbs))


if __name__ == "__main__":
    main()
