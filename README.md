# Alternative runner for Odoo

This project provides an alternative runner for Odoo server. 

Supported Odoo versions: 19.0 for now, more planned for later. 

The odoo-hybrid runner allows to run Odoo in multithreaded mode while keeping a distinct gevent process and a distinct cron process.  In a similar fashion to what running Odoo in workers mode does, there is also an orchestrator process which listens on the HTTP port, and dispatches the http requests to the multithreaded processess in charge of handling these requests through an inherited file descriptor. This orchestrator process also monitors the resources (memory consumption) and respawns processes which terminated. 

The package provides a command line tool, `odoo-hybrid` which can be used to run Odoo server. For now no support is provided to run tests, install or update addons

## Quickstart

### Installation

```bash
pip install odoo-hybrid
```

### Installing or updating Odoo addons

`odoo-hybrid` does not support `--init` or `--update`. Use the standard Odoo runner for any addon installation or update, then start the hybrid runner for serving.

```bash
# Install or update addons using the standard runner
python odoo-bin -c odoo.conf --init my_module --stop-after-init
python odoo-bin -c odoo.conf --update my_module --stop-after-init
```

### Running Odoo with the hybrid runner

```bash
odoo-hybrid --workers-thread=2 --workers-cron=1 -- -c odoo.conf

# With registry preloading (faster worker startup, lower memory after fork due to copy-on-write)
odoo-hybrid --preload --workers-thread=2 --workers-cron=1 -- -c odoo.conf
```

All options after `--` are passed as-is to each Odoo process. The `--workers`, `--max-cron-threads`, and `--gevent-port` Odoo options are managed by `odoo-hybrid` and should not be passed directly.

## Command line interface

odoo-hybrid <options> [ -- <odoo_options> ]

options:
  --limit-memory-soft            memory soft limit for all process (can be overridden for specific processes) (default 2058MiB)
  --limit-memory-hard            memory hard limit for all process (can be overridden for specific processes) (default 2560MiB)
  --limit-request                max number of requests / cron jobs limit for all process (can be overridden for specific processes) (default 65535)
  --limit-time-real              maximum allowed real time per requests for all process (can be overriden) (default 120)
  --preload                      preload Odoo registries in the master process before forking workers,
                                 for the databases listed in the Odoo configuration file (default: False)
  --workers-thread=INT           number of multithreaded workers to start (default 1)
  --limit-memory-soft-thread=INT memory soft limit for the multithreaded workers 
  --limit-memory-hard-thread=INT memory hard limit for the multithreaded workers 
  --limit-request-thread=INT     max number of requests to be processed by threaded workers before exiting (default: limit-request)
  --limit-time-real-thread       max number of time per request for threaded workers (default: limit-time-real)
  --workers-cron=INT             number of cron worker processes to start
  --limit-memory-soft-cron=INT   memory soft limit for the cron workers
  --limit-memory-hard-cron=INT   memory hard limit for the cron workers
  --limit-request-cron           max number of requests to be processed by cron workers before exiting (default: limit-request)
  --limit-time-real-cron         maximum allowed Real time per cron job (default: 600s)

  --workers-gevent=0 or 1        number of gevent workers to start (default 1)
  --limit-memory-soft-gevent=INT memory soft limit for the gevent worker (default to the value of --limit-memory-soft)
  --limit-memory-hard-gevent=INT memory hard limit for the gevent worker (default to the value of --limit-memory-hard)
  --http-port=INT                port on which the master process listens for connections (default 8069)
  --gevent-port=INT              port on which the gevent worker listens (default 8072)
  --host                         IP address or hostname on which to bind (default: localhost)

all the <odoo_options> are passed as-is to the odoo processes

### Option precedence

For options that overlap with Odoo configuration (e.g. `--limit-memory-soft`, `--limit-time-real`), the resolution order is:

1. `odoo-hybrid` CLI flag, if explicitly set
2. Corresponding value from the Odoo configuration file (e.g. `odoo.conf`)
3. Built-in default

## Internal work

When starting `odoo-hybrid`, things behave largely as with multi worker mode of Odoo server. The main difference is that the worker processes which will process the requests are running in multithreaded mode. A master process is created, which binds / listens on the http port. If `--preload` is set, the master preloads the Odoo registries for all databases listed in the configuration file before forking any worker; this amortises startup cost across workers and reduces per-worker memory via copy-on-write. The master then spawns the specified number of multithreaded workers, gevent workers and cron workers. It keeps track of these workers and if one of them terminates, it starts it again.

The communication between the master process and the workers is with pipes, signal and postgres LISTEN/NOTIFY. See `IPC mechanisms` below. 

The multithreaded workers inherit the listening socket fd from the master process via `os.fork()`. The master clears `FD_CLOEXEC` on the socket fd before forking so each worker receives its own duplicate. Workers call `accept()` directly on the inherited fd; the kernel load-balances incoming connections across all workers. This is identical to the mechanism used by `PreforkServer` in multi worker mode. The multithreaded workers keep track of the memory they consume. If they go above the soft limit, they enter drain mode (see [Drain semantics](#drain-semantics)). As soon as the master process is notified that a multithreaded worker is about to terminate, it starts a new multithreaded worker that will take over. If the master process notices that a multithreaded worker has crashed (without notification), it respawns a multithreaded worker.

For gevent workers and cron workers, the master process works in the same way as in multi worker mode. 

The signal handling works in the same way as in multi worker mode. 



## Drain semantics

### Trigger

The worker's memory monitor runs in a dedicated thread and periodically checks `RSS > limit_memory_soft`.

### Drain entry

1. Worker closes its copy of the inherited listening socket fd. The kernel stops delivering new `accept()` calls to this worker only; other workers and the master are unaffected (each holds their own fd dup).
2. Worker writes one byte to the drain notification pipe.
3. Worker's `ThreadedWSGIServerReloadable` continues serving in-flight requests — `daemon_threads = False` ensures the server waits for active request threads before exiting.

### Master reaction (on reading drain pipe)

1. Immediately spawns a replacement threaded worker. The new worker inherits the listening socket and starts accepting without gap.
2. Records the draining worker as draining — not counted toward population target, not subject to watchdog timeout escalation.

### Drain completion

Worker exits cleanly once all non-daemon threads finish. Master reaps via `SIGCHLD` / `waitpid`. No explicit exit notification needed — a clean exit after a drain pipe write distinguishes graceful drain from a crash.

### Drain timeout

If the draining worker has not exited within `limit-time-real-thread` seconds after entering drain, the master sends `SIGKILL`. This prevents a stuck long-running request from blocking the drain indefinitely.

### Crash vs drain distinction

| Event | Drain pipe written? | Master action |
|---|---|---|
| Graceful drain | Yes, before socket close | Spawn replacement immediately |
| Crash / SIGKILL | No | Spawn replacement on SIGCHLD |

Key property: the replacement spawns **before** the draining worker exits, so the pool never drops below the target size during normal soft-limit recycling.

## Graceful reload

`SIGHUP` triggers a phoenix restart (re-exec of the master process):

1. Master sends `SIGINT` to all workers and waits for them to exit cleanly.
2. Master clears `FD_CLOEXEC` on the listening socket fd so it survives `execv`.
3. Master calls `os.execv(sys.executable, [sys.executable] + sys.argv)`, replacing itself with a fresh process.
4. The new master inherits the bound socket fd and resumes accepting without a listening gap.

The re-exec picks up any code or configuration changes. Workers are not re-used across the boundary; they are fully restarted in the new process.

## IPC mechanisms

Three distinct IPC mechanisms are used, depending on direction.

### Wakeup pipe (master internal)

The master creates a self-pipe at startup. Python does not raise `EINTR` when a syscall is interrupted by a signal, so the signal handler cannot interrupt `select()` directly. Instead, the signal handler writes a byte to the write end of the pipe. The master's sleep loop includes the read end in its `select()` fd set, so any queued signal wakes the beat loop immediately.

### Watchdog pipes (worker → master, one per worker)

Each worker allocates a dedicated watchdog pipe at creation time. The worker's run loop writes a heartbeat byte to the pipe on every iteration. The master's sleep loop collects all watchdog pipe read-ends in its `select()` fd set and records the time of last heartbeat per worker. If a worker's last heartbeat is older than its configured timeout, the master kills it with `SIGKILL`.

For multithreaded workers, an additional drain notification pipe is used: when a worker exceeds the memory soft limit, it writes to this pipe to inform the master it is entering drain mode. The master immediately spawns a replacement worker, then waits for the draining worker to exit cleanly once all in-flight requests complete.

### Signals

| Direction | Signal | Meaning |
|---|---|---|
| external → master | SIGINT / SIGTERM | shutdown |
| external → master | SIGHUP | graceful reload (phoenix restart) |
| external → master | SIGQUIT | dump stacks |
| external → master | SIGTTIN / SIGTTOU | add / remove worker |
| external → master | SIGUSR1 / SIGUSR2 | ormcache stats |
| master → worker | SIGINT | graceful stop |
| master → worker | SIGKILL | hard kill (watchdog timeout) |
| master → worker | SIGTERM | force kill (shutdown) |
| worker → master | SIGCHLD | automatic on worker exit or crash |

Each worker also has an eintr pipe whose write end is registered with `signal.set_wakeup_fd()`. This causes Python to write a byte to it on any signal receipt, allowing signals to interrupt blocking `select()` calls inside the worker.

### Gevent process

The gevent process is spawned by reusing `long_polling_spawn()` from `server.py` directly. It uses `subprocess.Popen` (not `os.fork`) for a clean process state. No shared pipes with the master — only `SIGKILL` from the master to stop it, and ppid polling inside the gevent process to detect orphan state.

### Cron workers and PostgreSQL LISTEN/NOTIFY

Each cron worker holds a persistent PostgreSQL connection with `LISTEN cron_trigger`. The connection fd is included in the worker's `select()` sleep. A `NOTIFY cron_trigger` from anywhere wakes all listening cron workers simultaneously. A small per-worker jitter (`time.sleep(pid / 100 % 0.1)`) mitigates the thundering herd effect.

## Implementation notes

### Odoo integration

`odoo-hybrid` imports Odoo directly — it runs in the same Python process as Odoo and calls Odoo internals rather than invoking `odoo-bin` as a subprocess.

Reusable components from `odoo/service/server.py` are used where they reduce implementation effort without harming maintainability:

| Component | Reuse decision |
|---|---|
| `ThreadedWSGIServerReloadable` | Reuse as-is — threaded workers run `serve_forever()` on it |
| `RequestHandler`, `CommonRequestHandler` | Reuse as-is |
| `WorkerCron` | Reuse as-is |
| `preload_registries()` | Reuse as-is, called in master when `--preload` is set |
| `load_server_wide_modules()` | Reuse as-is — called in master before forking any worker, same as multi worker mode; modules are loaded once and inherited via copy-on-write |
| `set_limit_memory_hard()`, `memory_info()` | Reuse as-is |
| `PreforkServer` master loop pattern | Adapt — pipe/signal/beat loop is the model, but reimplemented to support threaded workers and drain semantics |
| `Worker` base class | Adapt — cron worker reused; threaded worker has a different `process_work()`, a dedicated memory monitor thread, and drain logic |
| `GeventServer` / `long_polling_spawn()` | Reuse `long_polling_spawn()` logic as-is to spawn the gevent worker |

Where Odoo internals are reused, the implementation imports them directly (e.g. `from odoo.service.server import ThreadedWSGIServerReloadable`). This means `odoo-hybrid` must be installed in an environment where Odoo is importable.

### Package structure

`odoo-hybrid` is a standalone pip-installable package. It declares a `console_scripts` entry point so that `pip install odoo-hybrid` makes the `odoo-hybrid` command available on `PATH`:

```toml
[project.scripts]
odoo-hybrid = "odoo_hybrid.__main__:main"
```

Odoo itself is not declared as a package dependency — it must be present in the environment separately (installed from source or via a distribution package).

## References

`odoo/service/server.py` : reference implementation

## Roadmap

### v0.0 — Project bootstrap

- `pyproject.toml` with package metadata and `console_scripts` entry point
- pre-commit hooks with ruff (linter + formatter)
- Skeleton `odoo-hybrid` script: argument parsing, no-op startup

### v0.1 — Minimal viable runner

- At most 1 threaded worker and 1 cron worker
- No gevent worker
- No drain logic
- No soft memory / request / time-real limit enforcement
- Hard memory limit enforced (SIGKILL on OOM)

### v0.1.1 — Automated tests

- pytest test suite covering CLI parsing, HybridMaster, and ThreadedWorker
- Odoo import stubs so unit tests run without a real Odoo installation

### v0.2 — Resource management

- Drain logic for threaded workers (soft memory limit, request count, time-real)
- Multiple threaded workers (`--workers-thread` > 1)

### v0.3 — Gevent support

- Gevent worker via `long_polling_spawn()`





