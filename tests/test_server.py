# Copyright 2026 Camptocamp SA
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html)import argparse
import errno
import fcntl
import os
import signal
import time
from unittest.mock import MagicMock, patch

import pytest

from odoo_hybrid.server import HybridMaster, ThreadedWorker


@pytest.fixture
def master(cfg, default_args):
    return HybridMaster(default_args)


# ---------------------------------------------------------------------------
# HybridMaster — __init__
# ---------------------------------------------------------------------------


class TestHybridMasterInit:
    def test_reads_timeout_from_config(self, cfg, default_args):
        cfg["limit_time_real"] = 60.0
        assert HybridMaster(default_args).timeout == 60.0

    def test_reads_http_port_from_config(self, cfg, default_args):
        cfg["http_port"] = 9000
        assert HybridMaster(default_args).http_port == 9000

    def test_interface_defaults_to_all_when_empty(self, cfg, default_args):
        cfg["http_interface"] = ""
        assert HybridMaster(default_args).interface == "0.0.0.0"

    def test_interface_used_when_set(self, cfg, default_args):
        cfg["http_interface"] = "127.0.0.1"
        assert HybridMaster(default_args).interface == "127.0.0.1"

    def test_n_workers_thread_reflects_args(self, cfg, default_args):
        default_args.workers_thread = 5
        assert HybridMaster(default_args).n_workers_thread == 5

    def test_n_workers_cron_capped_at_one(self, cfg, default_args):
        default_args.workers_cron = 3
        assert HybridMaster(default_args).n_workers_cron == 1

    def test_cron_timeout_negative_one_maps_to_timeout(self, cfg, default_args):
        cfg["limit_time_real"] = 120.0
        cfg["limit_time_real_cron"] = -1
        assert HybridMaster(default_args).cron_timeout == 120.0

    def test_workers_draining_empty_on_init(self, cfg, default_args):
        assert HybridMaster(default_args).workers_draining == {}

    def test_timeout_thread_falls_back_to_timeout(self, cfg, default_args):
        cfg["limit_time_real"] = 90.0
        default_args.limit_time_real_thread = None
        assert HybridMaster(default_args).timeout_thread == 90.0

    def test_timeout_thread_overrides_global(self, cfg, default_args):
        cfg["limit_time_real"] = 90.0
        default_args.limit_time_real_thread = 30.0
        assert HybridMaster(default_args).timeout_thread == 30.0

    def test_limit_request_thread_falls_back_to_limit_request(self, cfg, default_args):
        cfg["limit_request"] = 1000
        default_args.limit_request_thread = None
        assert HybridMaster(default_args).limit_request_thread == 1000


# ---------------------------------------------------------------------------
# HybridMaster — IPC utilities
# ---------------------------------------------------------------------------


class TestPipeUtilities:
    def test_pipe_new_returns_nonblocking_cloexec_fds(self, master):
        r, w = master.pipe_new()
        try:
            assert fcntl.fcntl(r, fcntl.F_GETFL) & os.O_NONBLOCK
            assert fcntl.fcntl(w, fcntl.F_GETFL) & os.O_NONBLOCK
            assert fcntl.fcntl(r, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
            assert fcntl.fcntl(w, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        finally:
            os.close(r)
            os.close(w)

    def test_pipe_ping_writes_dot(self, master):
        r, w = master.pipe_new()
        try:
            master.pipe_ping((r, w))
            assert os.read(r, 1) == b"."
        finally:
            os.close(r)
            os.close(w)

    def test_pipe_ping_suppresses_eagain(self, master):
        r, w = master.pipe_new()
        try:
            with patch("odoo_hybrid.server.os.write", side_effect=OSError(errno.EAGAIN, "")):
                master.pipe_ping((r, w))  # must not raise
        finally:
            os.close(r)
            os.close(w)


# ---------------------------------------------------------------------------
# HybridMaster — signal processing
# ---------------------------------------------------------------------------


class TestSignalProcessing:
    def test_sigint_raises_keyboard_interrupt(self, master):
        master.queue.append(signal.SIGINT)
        with pytest.raises(KeyboardInterrupt):
            master.process_signals()

    def test_sigterm_raises_keyboard_interrupt(self, master):
        master.queue.append(signal.SIGTERM)
        with pytest.raises(KeyboardInterrupt):
            master.process_signals()

    def test_sighup_raises_keyboard_interrupt(self, master):
        master.queue.append(signal.SIGHUP)
        with pytest.raises(KeyboardInterrupt):
            master.process_signals()

    def test_sigttin_increments_worker_count(self, master):
        master.n_workers_thread = 1
        master.queue.append(signal.SIGTTIN)
        master.process_signals()
        assert master.n_workers_thread == 2

    def test_sigttou_decrements_worker_count(self, master):
        master.n_workers_thread = 2
        master.queue.append(signal.SIGTTOU)
        master.process_signals()
        assert master.n_workers_thread == 1

    def test_sigttou_does_not_go_below_zero(self, master):
        master.n_workers_thread = 0
        master.queue.append(signal.SIGTTOU)
        master.process_signals()
        assert master.n_workers_thread == 0

    def test_signal_queue_capped_at_five(self, master):
        r, w = master.pipe_new()
        master.pipe = (r, w)
        try:
            for _ in range(7):
                master.signal_handler(signal.SIGUSR1, None)
            assert len(master.queue) <= 5
        finally:
            os.close(r)
            os.close(w)


# ---------------------------------------------------------------------------
# HybridMaster — worker lifecycle
# ---------------------------------------------------------------------------


class TestWorkerLifecycle:
    def test_process_spawn_fills_thread_workers_to_target(self, master, monkeypatch):
        calls = []

        def fake_spawn():
            calls.append(1)
            master.workers_thread[1000 + len(calls)] = MagicMock()

        monkeypatch.setattr(master, "_spawn_threaded_worker", fake_spawn)
        master.process_spawn()
        assert len(calls) == 1

    def test_process_spawn_no_spawn_when_already_full(self, master, monkeypatch):
        calls = []
        monkeypatch.setattr(master, "_spawn_threaded_worker", lambda: calls.append(1))
        worker = MagicMock()
        master.workers_thread[1234] = worker
        master.workers[1234] = worker
        master.process_spawn()
        assert len(calls) == 0

    def test_process_spawn_ignores_draining_workers(self, master, monkeypatch):
        calls = []

        def fake_spawn():
            calls.append(1)
            master.workers_thread[1000 + len(calls)] = MagicMock()

        monkeypatch.setattr(master, "_spawn_threaded_worker", fake_spawn)
        draining = MagicMock()
        master.workers_draining[9876] = draining
        master.workers[9876] = draining
        # workers_thread is empty — one thread worker must be spawned
        master.process_spawn()
        assert len(calls) == 1

    def test_process_spawn_cron_when_requested(self, cfg, default_args, monkeypatch):
        default_args.workers_cron = 1
        m = HybridMaster(default_args)
        cron_calls = []

        def fake_spawn_cron():
            cron_calls.append(1)
            m.workers_cron[2000 + len(cron_calls)] = MagicMock()

        def fake_spawn_thread():
            m.workers_thread[1000 + len(m.workers_thread)] = MagicMock()

        monkeypatch.setattr(m, "_spawn_cron_worker", fake_spawn_cron)
        monkeypatch.setattr(m, "_spawn_threaded_worker", fake_spawn_thread)
        m.process_spawn()
        assert len(cron_calls) == 1

    def test_process_zombie_reaps_worker(self, master, monkeypatch):
        mock_worker = MagicMock()
        master.workers[9999] = mock_worker
        master.workers_thread[9999] = mock_worker

        call_count = [0]

        def fake_waitpid(pid, flags):
            if call_count[0] == 0:
                call_count[0] += 1
                return (9999, 0)
            raise OSError(errno.ECHILD, "No child processes")

        monkeypatch.setattr(os, "waitpid", fake_waitpid)
        master.process_zombie()
        assert 9999 not in master.workers

    def test_process_timeout_sigkills_expired_worker(self, master, monkeypatch):
        mock_worker = MagicMock()
        mock_worker.watchdog_timeout = 5.0
        mock_worker.watchdog_time = time.time() - 10.0
        master.workers[8888] = mock_worker

        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        master.process_timeout()
        assert (8888, signal.SIGKILL) in kills

    def test_process_timeout_no_kill_within_deadline(self, master, monkeypatch):
        mock_worker = MagicMock()
        mock_worker.watchdog_timeout = 120.0
        mock_worker.watchdog_time = time.time()
        master.workers[7777] = mock_worker

        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        master.process_timeout()
        assert len(kills) == 0

    def test_process_timeout_drain_timeout_kills_draining_worker(self, master, monkeypatch):
        mock_worker = MagicMock(spec=ThreadedWorker)
        mock_worker.drain_time = time.time() - 200.0
        mock_worker.drain_timeout = 120.0
        mock_worker.watchdog_timeout = 120.0
        mock_worker.watchdog_time = time.time()
        master.workers[6666] = mock_worker
        master.workers_draining[6666] = mock_worker

        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        master.process_timeout()
        assert (6666, signal.SIGKILL) in kills

    def test_process_timeout_no_watchdog_kill_for_draining_worker(self, master, monkeypatch):
        mock_worker = MagicMock(spec=ThreadedWorker)
        mock_worker.drain_time = time.time()
        mock_worker.drain_timeout = 120.0
        mock_worker.watchdog_timeout = 5.0
        mock_worker.watchdog_time = time.time() - 100.0  # would normally trigger watchdog
        master.workers[6665] = mock_worker
        master.workers_draining[6665] = mock_worker

        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        master.process_timeout()
        assert len(kills) == 0  # drain timeout not reached; watchdog skipped for draining

    def test_worker_pop_removes_from_all_dicts(self, master):
        mock_worker = MagicMock()
        master.workers[5555] = mock_worker
        master.workers_thread[5555] = mock_worker
        master.workers_draining[5555] = mock_worker
        master.worker_pop(5555)
        assert 5555 not in master.workers
        assert 5555 not in master.workers_thread
        assert 5555 not in master.workers_draining


# ---------------------------------------------------------------------------
# ThreadedWorker
# ---------------------------------------------------------------------------


class TestThreadedWorker:
    def test_init_creates_pipes(self, master):
        worker = ThreadedWorker(master)
        try:
            assert worker.watchdog_pipe[0] >= 0
            assert worker.eintr_pipe[0] >= 0
            assert worker.drain_pipe[0] >= 0
        finally:
            worker.close()

    def test_close_closes_all_fds(self, master):
        worker = ThreadedWorker(master)
        fds = [*worker.watchdog_pipe, *worker.eintr_pipe, *worker.drain_pipe]
        worker.close()
        for fd in fds:
            with pytest.raises(OSError):
                os.fstat(fd)

    def test_close_is_idempotent(self, master):
        worker = ThreadedWorker(master)
        worker.close()
        worker.close()  # must not raise

    def test_watchdog_time_set_on_init(self, master):
        before = time.time()
        worker = ThreadedWorker(master)
        try:
            assert worker.watchdog_time >= before
        finally:
            worker.close()

    def test_watchdog_timeout_matches_master_timeout(self, master):
        worker = ThreadedWorker(master)
        try:
            assert worker.watchdog_timeout == master.timeout
        finally:
            worker.close()

    def test_pid_none_before_fork(self, master):
        worker = ThreadedWorker(master)
        try:
            assert worker.pid is None
        finally:
            worker.close()


# ---------------------------------------------------------------------------
# HybridMaster — gevent worker
# ---------------------------------------------------------------------------


class TestGeventWorker:
    def test_n_workers_gevent_reflects_args(self, cfg, default_args):
        default_args.workers_gevent = 1
        assert HybridMaster(default_args).n_workers_gevent == 1

    def test_n_workers_gevent_zero(self, cfg, default_args):
        default_args.workers_gevent = 0
        assert HybridMaster(default_args).n_workers_gevent == 0

    def test_gevent_pid_none_on_init(self, master):
        assert master.gevent_pid is None

    def test_gevent_port_read_from_config(self, cfg, default_args):
        cfg["gevent_port"] = 9090
        assert HybridMaster(default_args).gevent_port == 9090

    def test_spawn_gevent_worker_stores_pid(self, master, monkeypatch):
        mock_proc = MagicMock()
        mock_proc.pid = 5555
        monkeypatch.setattr("odoo_hybrid.server.shutil.which", lambda _: "/usr/bin/odoo-bin")
        monkeypatch.setattr("odoo_hybrid.server.subprocess.Popen", lambda cmd, **kw: mock_proc)
        master._spawn_gevent_worker()
        assert master.gevent_pid == 5555

    def test_spawn_gevent_worker_cmd_includes_gevent_subcommand(self, master, monkeypatch):
        captured = {}
        monkeypatch.setattr("odoo_hybrid.server.shutil.which", lambda _: "/usr/bin/odoo-bin")
        monkeypatch.setattr(
            "odoo_hybrid.server.subprocess.Popen",
            lambda cmd, **kw: captured.update(cmd=cmd) or MagicMock(pid=1),
        )
        master._spawn_gevent_worker()
        assert "gevent" in captured["cmd"]
        assert "--gevent-port" in captured["cmd"]
        assert str(master.gevent_port) in captured["cmd"]

    def test_spawn_gevent_worker_fallback_when_no_odoo_bin(self, master, monkeypatch):
        import sys
        import types

        captured = {}
        monkeypatch.setattr("odoo_hybrid.server.shutil.which", lambda _: None)
        monkeypatch.setitem(sys.modules, "odoo", types.SimpleNamespace(__file__="/odoo/odoo/__init__.py"))
        monkeypatch.setattr(
            "odoo_hybrid.server.subprocess.Popen",
            lambda cmd, **kw: captured.update(cmd=cmd) or MagicMock(pid=1),
        )
        master._spawn_gevent_worker()
        assert captured["cmd"][1].endswith("odoo-bin")

    def test_spawn_gevent_worker_passes_odoo_argv(self, master, monkeypatch):
        master.odoo_argv = ["-c", "odoo.conf"]
        captured = {}
        monkeypatch.setattr("odoo_hybrid.server.shutil.which", lambda _: "/usr/bin/odoo-bin")
        monkeypatch.setattr(
            "odoo_hybrid.server.subprocess.Popen",
            lambda cmd, **kw: captured.update(cmd=cmd) or MagicMock(pid=1),
        )
        master._spawn_gevent_worker()
        assert "-c" in captured["cmd"]
        assert "odoo.conf" in captured["cmd"]

    def test_process_spawn_starts_gevent_when_enabled(self, master, monkeypatch):
        master.n_workers_gevent = 1
        spawned = []
        monkeypatch.setattr(master, "_spawn_threaded_worker", lambda: master.workers_thread.__setitem__(1, MagicMock()))
        monkeypatch.setattr(master, "_spawn_gevent_worker", lambda: spawned.append(1))
        master.process_spawn()
        assert len(spawned) == 1

    def test_process_spawn_no_double_gevent(self, master, monkeypatch):
        master.n_workers_gevent = 1
        master.gevent_pid = 9000
        spawned = []
        monkeypatch.setattr(master, "_spawn_threaded_worker", lambda: master.workers_thread.__setitem__(1, MagicMock()))
        monkeypatch.setattr(master, "_spawn_gevent_worker", lambda: spawned.append(1))
        master.process_spawn()
        assert len(spawned) == 0

    def test_process_spawn_no_gevent_when_disabled(self, cfg, default_args, monkeypatch):
        default_args.workers_gevent = 0
        m = HybridMaster(default_args)
        spawned = []
        monkeypatch.setattr(m, "_spawn_threaded_worker", lambda: m.workers_thread.__setitem__(1, MagicMock()))
        monkeypatch.setattr(m, "_spawn_gevent_worker", lambda: spawned.append(1))
        m.process_spawn()
        assert len(spawned) == 0

    def test_process_zombie_clears_gevent_pid(self, master, monkeypatch):
        master.gevent_pid = 7777
        call_count = [0]

        def fake_waitpid(pid, flags):
            if call_count[0] == 0:
                call_count[0] += 1
                return (7777, 0)
            raise OSError(errno.ECHILD, "No child processes")

        monkeypatch.setattr(os, "waitpid", fake_waitpid)
        pop_calls = []
        monkeypatch.setattr(master, "worker_pop", lambda pid: pop_calls.append(pid))
        master.process_zombie()
        assert master.gevent_pid is None
        assert 7777 not in pop_calls

    def test_stop_kills_gevent(self, master, monkeypatch):
        master.gevent_pid = 4321
        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        monkeypatch.setattr(master, "worker_kill", lambda pid, sig: None)
        master.socket = None
        master.stop(graceful=False)
        assert (4321, signal.SIGKILL) in kills
        assert master.gevent_pid is None

    def test_stop_graceful_kills_gevent(self, master, monkeypatch):
        master.gevent_pid = 4322
        kills = []
        monkeypatch.setattr(os, "kill", lambda pid, sig: kills.append((pid, sig)))
        monkeypatch.setattr(master, "worker_kill", lambda pid, sig: None)
        monkeypatch.setattr(master, "process_signals", lambda: None)
        monkeypatch.setattr(master, "process_zombie", lambda: None)
        monkeypatch.setattr(master, "sleep", lambda: None)
        monkeypatch.setattr(master, "process_timeout", lambda: None)
        master.socket = None
        master.workers.clear()
        master.stop(graceful=True)
        assert (4322, signal.SIGKILL) in kills
        assert master.gevent_pid is None
