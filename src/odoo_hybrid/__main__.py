import argparse
import logging
import sys

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
        default=2058 * _MiB,
        metavar="BYTES",
        help="memory soft limit for all processes (default: 2058 MiB)",
    )
    p.add_argument(
        "--limit-memory-hard",
        type=int,
        default=2560 * _MiB,
        metavar="BYTES",
        help="memory hard limit for all processes (default: 2560 MiB)",
    )
    p.add_argument(
        "--limit-request",
        type=int,
        default=65535,
        help="max requests/cron jobs per process (default: 65535)",
    )
    p.add_argument(
        "--limit-time-real",
        type=float,
        default=120,
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
        default=600,
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
    p.add_argument("--host", default="localhost", help="IP or hostname to bind (default: localhost)")

    return p


def _split_argv() -> tuple[list[str], list[str]]:
    argv = sys.argv[1:]
    try:
        idx = argv.index("--")
        return argv[:idx], argv[idx + 1 :]
    except ValueError:
        return argv, []


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    hybrid_argv, odoo_argv = _split_argv()
    args = _build_parser().parse_args(hybrid_argv)
    _logger.info(
        "thread_workers=%d cron_workers=%d gevent_workers=%d odoo_args=%r",
        args.workers_thread,
        args.workers_cron,
        args.workers_gevent,
        odoo_argv,
    )


if __name__ == "__main__":
    main()
