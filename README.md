# phlo-daemon

An optional per-host node sidecar for [Phlo](https://phlo.tech): a **generic central engine** that
dispatches any Phlo target to a pool of persistent `phlo_serve` workers.

Core Phlo always works without it: every target is callable as a one-shot CLI process
(`php app.php <target> [args...]`). The daemon is purely an **extension**:

1. a **worker pool** that runs those same calls far more performantly (boot the app once, reuse the
   worker, instead of a fresh process per call), and
2. the **enabler for websockets and scheduled tasks**, which need a long-lived host process.

The daemon's dispatch core knows nothing about any specific feature. The WebSocket server and the
scheduler are built in; the PHP runtime helpers (`phlo_sync`/`phlo_async`/…) reach it over HTTP.

## Worker protocol

Each worker runs `php <app.php> phlo_serve`, boots the app once, then answers newline-JSON requests
on stdin (one in flight per worker; concurrency = pool size):

```
in   {"id","target","args"?,"stream"?}
out  {"t":"ready"}                                  // once, after boot
     {"id","t":"line","data"}                       // 0..N, only when stream
     {"id","t":"done","result"} | {"id","t":"error","message"}   // exactly one, terminal
```

Per request the worker resets state (`phlo('tech/reset')` + session close + GC), mirroring the
FrankenPHP HTTP worker loop, so jobs never leak into each other.

## HTTP API

Binds `127.0.0.1` by default (local-only; gate at the network boundary).

- `POST /dispatch` `{app, target, args?, stream?, async?}`: `app` is the absolute `…/app.php` path
  to run (the pool is keyed by it). The runtime helpers use this; a caller that knows its own app
  needs no host→app config.
  - response: default `{status:"ok", result}`; `async:true` → `202 {status:"ok", queued:true}`;
    `stream:true` → an `application/x-ndjson` stream of `{t:line,data}*` then `{t:done,result}` /
    `{t:error}` (used for streaming output, e.g. websocket `receive`)
- `POST /register` `{host, app, build}`: an app announces its host (persisted to `registry.json`).
- `POST /message` `{host, target, data}`: the broadcast bridge the websocket pushes through.
- `GET /health`: `{workers, cap, pools, sockets, registered}` — live worker total vs cap, per-pool
  stats keyed by app path (`workers`, `busy`, `queued`), connected sockets per host, registered hosts.

## Configuration

```js
require('phlo-daemon')(port, phpBinary, schedule?)
```

No host map, no pool sizing: apps register their own host on first request, and each pool scales on
demand up to a cap of one less than the core count, reaping idle workers. One-shot vs pooled follows
the app's registered `build` flag (a `build: true` dev app runs one-shot; a release app is pooled).

```js
require('./phlo-daemon.js')(3001, '/usr/bin/php-zts', [
  { app: '/srv/dashboard/www/app.php', target: 'fleet::poll', every: 120, build: true },
  { app: '/srv/api/www/app.php',       target: 'tasks::run',  every: 60,  build: false },
])
```

- `schedule`: `{app, target, every, build}` entries the daemon dispatches on their interval, first
  run one interval after boot, replacing cron for `tasks::run` / `fleet::poll`.
- `PHLO_DAEMON_IDLE_MS` / `PHLO_DAEMON_REAP_MS`: env overrides for the idle timeout and the reap sweep.

## Consumers

- **phloWS** (the daemon's built-in WebSocket layer): the WebSocket server runs
  in-process, resolving each connection's host to an app and running the
  `websocket::{auth,connect,receive,close}` hooks on the pool (`receive` streams).
- **runtime helpers**: `phlo_sync` / `phlo_async` / `await` / `phlo_stream` route through `/dispatch`
  by `app` path when the app sets the optional `daemon` constant
  (`phlo_app(daemon: 3001)`), otherwise they keep their one-shot subprocess
  behaviour. Adopting the daemon is opt-in, never required, and needs no host→app config.
- **Phlo WhatsApp** stays its own service (persistent phone session); it is monitored, not absorbed.

## Run

```sh
node config.js            # config.js requires this module with (port, php, schedule)
# or under a process manager
pm2 start config.js --name phlo-daemon
```

MIT.
