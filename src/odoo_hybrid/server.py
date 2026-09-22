# Copyright 2026 Camptocamp SA
# Copyright 2015 Odoo
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)
from __future__ import annotations

import argparse
import collections
import contextlib
import errno
import fcntl
import logging
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any

import psutil
from odoo import sql_db
from odoo.service.server import (
    ThreadedWSGIServerReloadable,
    WorkerCron,
    empty_pipe,
    preload_registries,
    set_limit_memory_hard,
)
from odoo.tools import config
from odoo.tools.cache import log_ormcache_stats
from odoo.tools.misc import dumpstacks
from odoo.tools.osutil import memory_info

_logger = logging.getLogger(__name__)


class _ThreadedWorkerServer(ThreadedWSGIServerReloadable):
    """ThreadedWSGIServerReloadable that uses an inherited master socket fd instead of binding."""

    def __init__(self, host: str, port: int, app: Any, *, inherited_fd: int) -> None:
        self.request_count = 0
        self._inherited_fd = inherited_fd
        super().__init__(host, port, app)

    def server_bind(self) -> None:
        self.reload_socket = True
        self.socket = socket.fromfd(self._inherited_fd, socket.AF_INET, socket.SOCK_STREAM)
        self.server_name = socket.getfqdn(self.server_address[0])
        self.server_port = self.server_address[1]

    def server_activate(self) -> None:
        pass  # socket already listening in master

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        self.request_count += 1
        super().process_request_thread(request, client_address)


class ThreadedWorker:
    """HTTP worker process: runs serve_forever() on an inherited socket in a forked child."""

    def __init__(self, master: HybridMaster) -> None:
        self.master = master
        self.watchdog_pipe = master.pipe_new()
        self.eintr_pipe = master.pipe_new()
        self.drain_pipe = master.pipe_new()
        self.watchdog_time = time.time()
        self.watchdog_timeout: float | None = master.timeout
        self.limit_memory_soft: int = master.limit_memory_soft_thread
        self.limit_request: int = master.limit_request_thread
        self.drain_timeout: float = master.timeout_thread
        self.drain_time: float | None = None
        self.pid: int | None = None

    def close(self) -> None:
        for fd in (*self.watchdog_pipe, *self.eintr_pipe, *self.drain_pipe):
            with contextlib.suppress(OSError):
                os.close(fd)

    def _run(self) -> None:
        """Runs in child process after os.fork()."""
        self.pid = os.getpid()
        signal.signal(signal.SIGINT, signal.default_int_handler)
        for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGCHLD, signal.SIGTTIN, signal.SIGTTOU):
            signal.signal(sig, signal.SIG_DFL)
        signal.set_wakeup_fd(self.eintr_pipe[1])
        set_limit_memory_hard()
        threading.Thread(target=self._heartbeat, daemon=True, name="odoo-hybrid.watchdog").start()
        from odoo import http

        server = _ThreadedWorkerServer(
            self.master.interface,
            self.master.http_port,
            http.root,
            inherited_fd=self.master.socket.fileno(),
        )

        def _memory_monitor() -> None:
            while True:
                time.sleep(self.master.beat)
                rss = memory_info(psutil.Process(os.getpid()))
                should_drain = (self.limit_memory_soft and rss > self.limit_memory_soft) or (
                    self.limit_request and server.request_count >= self.limit_request
                )
                if should_drain:
                    _logger.info(
                        "Worker (%s) entering drain (rss=%d, requests=%d)",
                        self.pid,
                        rss,
                        server.request_count,
                    )
                    self.master.pipe_ping(self.drain_pipe)  # notify master before shutting down
                    server.shutdown()
                    return

        threading.Thread(target=_memory_monitor, daemon=True, name="odoo-hybrid.memory").start()
        _logger.info("Worker ThreadedWorker (%s) alive", self.pid)
        with contextlib.suppress(KeyboardInterrupt):
            server.serve_forever()
        # non-daemon request threads (daemon_threads=False) hold process alive until in-flight complete

    def _heartbeat(self) -> None:
        while True:
            self.master.pipe_ping(self.watchdog_pipe)
            time.sleep(self.master.beat / 2)


class HybridMaster:
    def __init__(self, args: argparse.Namespace) -> None:
        self.beat = 4
        self.socket: socket.socket | None = None
        self.pipe: tuple[int, int] | None = None
        self.generation = 0
        self.queue: collections.deque[int] = collections.deque()
        self.workers: dict[int, Any] = {}
        self.workers_thread: dict[int, ThreadedWorker] = {}
        self.workers_draining: dict[int, ThreadedWorker] = {}
        self.workers_cron: dict[int, WorkerCron] = {}
        self.n_workers_thread = args.workers_thread
        self.n_workers_cron = min(args.workers_cron, 1)
        self.n_workers_gevent: int = args.workers_gevent
        self.gevent_pid: int | None = None
        self.gevent_port: int = config["gevent_port"]
        self.odoo_argv: list[str] = args.odoo_argv
        # read values already resolved by _apply_config
        self.timeout: float = config["limit_time_real"]
        cron_timeout = config["limit_time_real_cron"] or None
        self.cron_timeout: float | None = self.timeout if cron_timeout == -1 else cron_timeout
        self.limit_request: int = config["limit_request"]
        self.interface: str = config["http_interface"] or "0.0.0.0"
        self.http_port: int = config["http_port"]
        # per-thread-worker limits (fall back to global when not overridden)
        self.limit_memory_soft: int = args.limit_memory_soft_effective or 0
        self.limit_memory_soft_thread: int = args.limit_memory_soft_thread or self.limit_memory_soft
        self.limit_memory_soft_gevent: int = args.limit_memory_soft_gevent or self.limit_memory_soft
        self.limit_request_thread: int = args.limit_request_thread or self.limit_request
        self.timeout_thread: float = args.limit_time_real_thread or self.timeout

    # ------------------------------------------------------------------
    # IPC utilities — WorkerCron calls self.multi.pipe_new() / pipe_ping()
    # ------------------------------------------------------------------

    def pipe_new(self) -> tuple[int, int]:
        pipe = os.pipe()
        for fd in pipe:
            flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
            fcntl.fcntl(fd, fcntl.F_SETFL, flags)
            flags = fcntl.fcntl(fd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC
            fcntl.fcntl(fd, fcntl.F_SETFD, flags)
        return pipe

    def pipe_ping(self, pipe: tuple[int, int]) -> None:
        try:
            os.write(pipe[1], b".")
        except OSError as e:
            if e.errno not in [errno.EAGAIN, errno.EINTR]:
                raise

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------

    def signal_handler(self, sig: int, frame: Any) -> None:
        if len(self.queue) < 5 or sig == signal.SIGCHLD:
            self.queue.append(sig)
            self.pipe_ping(self.pipe)
        else:
            _logger.warning("Dropping signal: %s", sig)

    def process_signals(self) -> None:
        while self.queue:
            sig = self.queue.popleft()
            if sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                raise KeyboardInterrupt
            elif sig == signal.SIGQUIT:
                dumpstacks()
            elif sig in (signal.SIGUSR1, signal.SIGUSR2):
                log_ormcache_stats(sig)
            elif sig == signal.SIGTTIN:
                self.n_workers_thread += 1
            elif sig == signal.SIGTTOU:
                self.n_workers_thread = max(0, self.n_workers_thread - 1)

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def worker_pop(self, pid: int) -> None:
        if pid in self.workers:
            _logger.debug("Worker (%s) unregistered", pid)
            worker = self.workers.pop(pid)
            self.workers_thread.pop(pid, None)
            self.workers_cron.pop(pid, None)
            self.workers_draining.pop(pid, None)
            with contextlib.suppress(OSError):
                worker.close()

    def worker_kill(self, pid: int, sig: int) -> None:
        try:
            os.kill(pid, sig)
        except OSError as e:
            if e.errno == errno.ESRCH:
                self.worker_pop(pid)
            else:
                raise

    def process_zombie(self) -> None:
        while True:
            try:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid:
                    break
                if wpid == self.gevent_pid:
                    _logger.warning("GeventWorker (%s) exited", wpid)
                    self.gevent_pid = None
                    continue
                if (status >> 8) == 3:
                    _logger.critical("Critical worker error (%s)", wpid)
                    raise Exception(f"Critical worker error ({wpid})")
                self.worker_pop(wpid)
            except OSError as e:
                if e.errno == errno.ECHILD:
                    break
                raise

    def process_timeout(self) -> None:
        now = time.time()
        for pid, worker in list(self.workers_draining.items()):
            if worker.drain_time is not None and (now - worker.drain_time) >= worker.drain_timeout:
                _logger.error("ThreadedWorker (%s) drain timeout after %ss", pid, worker.drain_timeout)
                self.worker_kill(pid, signal.SIGKILL)
        for pid, worker in list(self.workers.items()):
            if pid in self.workers_draining:
                continue  # drain timeout checked above
            if worker.watchdog_timeout is not None and (now - worker.watchdog_time) >= worker.watchdog_timeout:
                _logger.error(
                    "%s (%s) timeout after %ss",
                    worker.__class__.__name__,
                    pid,
                    worker.watchdog_timeout,
                )
                self.worker_kill(pid, signal.SIGKILL)

    def _spawn_threaded_worker(self) -> None:
        self.generation += 1
        worker = ThreadedWorker(self)
        pid = os.fork()
        if pid != 0:
            worker.pid = pid
            self.workers[pid] = worker
            self.workers_thread[pid] = worker
            return
        worker._run()
        sys.exit(0)

    def _spawn_cron_worker(self) -> None:
        self.generation += 1
        worker = WorkerCron(self)
        pid = os.fork()
        if pid != 0:
            worker.pid = pid
            self.workers[pid] = worker
            self.workers_cron[pid] = worker
            return
        worker.run()
        sys.exit(0)

    def _spawn_gevent_worker(self) -> None:
        odoo_bin = shutil.which("odoo-bin")
        if odoo_bin is None:
            import odoo as _odoo_pkg

            if _odoo_pkg.__file__ is not None:
                odoo_bin = os.path.join(os.path.dirname(os.path.dirname(_odoo_pkg.__file__)), "odoo-bin")
            else:
                # namespace package: __path__ holds the package directory
                odoo_bin = os.path.join(os.path.dirname(next(iter(_odoo_pkg.__path__))), "odoo-bin")
        cmd = [
            sys.executable,
            odoo_bin,
            "gevent",
            "--gevent-port",
            str(self.gevent_port),
            "--http-interface",
            self.interface,
        ]
        if self.limit_memory_soft_gevent:
            cmd += ["--limit-memory-soft", str(self.limit_memory_soft_gevent)]
        cmd += self.odoo_argv
        proc = subprocess.Popen(cmd)
        self.gevent_pid = proc.pid
        _logger.info("GeventWorker (%s) spawned", proc.pid)

    def process_spawn(self) -> None:
        while len(self.workers_thread) < self.n_workers_thread:
            self._spawn_threaded_worker()
        while len(self.workers_thread) > self.n_workers_thread:
            self.worker_kill(next(iter(self.workers_thread)), signal.SIGINT)
        while len(self.workers_cron) < self.n_workers_cron:
            self._spawn_cron_worker()
        if self.n_workers_gevent and not self.gevent_pid:
            self._spawn_gevent_worker()

    # ------------------------------------------------------------------
    # Beat loop
    # ------------------------------------------------------------------

    def sleep(self) -> None:
        try:
            watchdog_fds = {w.watchdog_pipe[0]: w for w in self.workers.values()}
            drain_fds = {w.drain_pipe[0]: w for w in self.workers_thread.values()}
            ready = select.select([*watchdog_fds, *drain_fds, self.pipe[0]], [], [], self.beat)
            for fd in ready[0]:
                if fd in watchdog_fds:
                    watchdog_fds[fd].watchdog_time = time.time()
                elif fd in drain_fds:
                    worker = drain_fds[fd]
                    pid = worker.pid
                    worker.drain_time = time.time()
                    self.workers_thread.pop(pid, None)
                    self.workers_draining[pid] = worker
                    self._spawn_threaded_worker()
                    _logger.info("Worker (%s) entered drain; replacement spawned", pid)
                empty_pipe(fd)
        except OSError as e:
            if e.args[0] not in [errno.EINTR]:
                raise

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self.pipe = self.pipe_new()
        family = socket.AF_INET6 if ":" in self.interface else socket.AF_INET
        self.socket = socket.socket(family, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setblocking(False)
        self.socket.bind((self.interface, self.http_port))
        self.socket.listen(8)
        # clear FD_CLOEXEC so the socket fd survives os.fork()
        flags = fcntl.fcntl(self.socket.fileno(), fcntl.F_GETFD)
        fcntl.fcntl(self.socket.fileno(), fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)
        signal.signal(signal.SIGCHLD, self.signal_handler)
        signal.signal(signal.SIGTTIN, self.signal_handler)
        signal.signal(signal.SIGTTOU, self.signal_handler)
        signal.signal(signal.SIGQUIT, dumpstacks)
        signal.signal(signal.SIGUSR1, log_ormcache_stats)
        signal.signal(signal.SIGUSR2, log_ormcache_stats)
        _logger.info("HTTP service running on %s:%s", self.interface, self.http_port)

    def stop(self, graceful: bool = True) -> None:
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.gevent_pid is not None:
            with contextlib.suppress(OSError):
                os.kill(self.gevent_pid, signal.SIGKILL)
            self.gevent_pid = None
        if graceful:
            _logger.info("Stopping workers gracefully")
            for pid in list(self.workers):
                self.worker_kill(pid, signal.SIGINT)
            self.beat = 0.1
            while self.workers:
                try:
                    self.process_signals()
                except KeyboardInterrupt:
                    _logger.info("Forced shutdown.")
                    break
                self.process_zombie()
                self.sleep()
                self.process_timeout()
        else:
            _logger.info("Stopping forcefully")
            for pid in list(self.workers):
                self.worker_kill(pid, signal.SIGTERM)

    def run(self, preload_dbs: list[str]) -> int:
        self.start()
        preload_registries(preload_dbs)
        sql_db.close_all()
        _logger.debug("odoo-hybrid master loop starting")
        while True:
            try:
                self.process_signals()
                self.process_zombie()
                self.process_timeout()
                self.process_spawn()
                self.sleep()
            except KeyboardInterrupt:
                _logger.debug("odoo-hybrid clean stop")
                self.stop()
                break
            except Exception as e:
                _logger.exception(e)
                self.stop(False)
                return -1
        return 0
