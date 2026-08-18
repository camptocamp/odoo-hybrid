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

    def test_n_workers_thread_capped_at_one(self, cfg, default_args):
        default_args.workers_thread = 5
        assert HybridMaster(default_args).n_workers_thread == 1

    def test_n_workers_cron_capped_at_one(self, cfg, default_args):
        default_args.workers_cron = 3
        assert HybridMaster(default_args).n_workers_cron == 1

    def test_cron_timeout_negative_one_maps_to_timeout(self, cfg, default_args):
        cfg["limit_time_real"] = 120.0
        cfg["limit_time_real_cron"] = -1
        assert HybridMaster(default_args).cron_timeout == 120.0


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
        monkeypatch.setattr(master, "_spawn_threaded_worker", lambda: calls.append(1))
        master.process_spawn()
        assert len(calls) == 1

    def test_process_spawn_no_spawn_when_already_full(self, master, monkeypatch):
        calls = []
        monkeypatch.setattr(master, "_spawn_threaded_worker", lambda: calls.append(1))
        master.workers_thread[1234] = MagicMock()
        master.process_spawn()
        assert len(calls) == 0

    def test_process_spawn_cron_when_requested(self, cfg, default_args, monkeypatch):
        default_args.workers_cron = 1
        m = HybridMaster(default_args)
        cron_calls = []
        monkeypatch.setattr(m, "_spawn_cron_worker", lambda: cron_calls.append(1))
        monkeypatch.setattr(m, "_spawn_threaded_worker", lambda: None)
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

    def test_worker_pop_removes_from_all_dicts(self, master):
        mock_worker = MagicMock()
        master.workers[5555] = mock_worker
        master.workers_thread[5555] = mock_worker
        master.worker_pop(5555)
        assert 5555 not in master.workers
        assert 5555 not in master.workers_thread


# ---------------------------------------------------------------------------
# ThreadedWorker
# ---------------------------------------------------------------------------


class TestThreadedWorker:
    def test_init_creates_pipes(self, master):
        worker = ThreadedWorker(master)
        try:
            assert worker.watchdog_pipe[0] >= 0
            assert worker.eintr_pipe[0] >= 0
        finally:
            worker.close()

    def test_close_closes_all_fds(self, master):
        worker = ThreadedWorker(master)
        fds = [*worker.watchdog_pipe, *worker.eintr_pipe]
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
