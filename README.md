# phlo-daemon

An optional per-host node sidecar for [Phlo](https://phlo.tech): a **generic central engine** that
dispatches any Phlo target to a pool of persistent `phlo_serve` workers.

Core Phlo always works without it — every target is callable as a one-shot CLI process
(`php app.php <target> [args...]`). The daemon is purely an **extension**:

1. a **worker pool** that runs those same calls far more performantly (boot the app once, reuse the
   worker, instead of a fresh process per call), and
2. the **enabler for websockets and scheduled tasks**, which need a long-lived host process.

The daemon knows nothing about any specific consumer. The websocket package, the runtime helpers
(`phlo_sync`/`phlo_async`/…) and schedulers are separate and talk to it over HTTP.

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

- `POST /dispatch` `{host, target, args?, stream?, async?}`
  - default: runs the target and returns `{status:"ok", result}`
  - `async:true`: queues fire-and-forget, returns `202 {status:"ok", queued:true}`
  - `stream:true`: replies with an `application/x-ndjson` stream of `{t:line,data}*` then
    `{t:done,result}` / `{t:error}` (used for streaming output, e.g. websocket `receive`)
- `GET /health`: per-host pool stats (`workers`, `busy`, `queued`) + configured hosts

## Configuration

```js
require('phlo-daemon')(port, phpBinary, hostMap, listen?, maxBody?, schedule?)
```

`hostMap` keys are hosts; each value is an app entry — a string `'/srv/app/www/app.php'` (one-shot,
`workers: 0`) or `{ app, workers, timeout?, recycle? }`:

```js
require('./phlo-daemon.js')(3002, '/usr/bin/php-zts', {
  'dev.example.com': '/srv/example/www/app.php',                          // one-shot
  'api.example.com': { app: '/srv/api/www/app.php', workers: 4 },         // pool of 4
}, '127.0.0.1', 1024 * 1024, [
  { host: 'api.example.com', target: 'tasks::run',  every: 60 },          // scheduler
  { host: 'dashboard.example.com', target: 'fleet::poll', every: 120 },
])
```

- `workers` — pool size for the host; `0` falls back to a one-shot process per call.
- `timeout` — per-request timeout (ms, default 30000).
- `recycle` — replace a worker after N requests (default 10000; `0` disables).
- `schedule` — `{host, target, every}` entries the daemon dispatches on their interval, first run
  one interval after boot — this replaces cron for `tasks::run` / `fleet::poll`.

## Consumers

- **websocket** (`phlo-websocket`): keeps the websocket protocol, delegates the
  `websocket::{auth,connect,receive,close}` hooks to `/dispatch` (`receive` streams).
- **runtime helpers**: `phlo_sync` / `phlo_async` route through `/dispatch` when the app sets the
  optional `daemon` constant (`phlo_app(daemon: 'http://127.0.0.1:3002')`), otherwise they keep their
  one-shot subprocess behaviour. So adopting the daemon is opt-in and never required.
- **whatsapp** stays its own service (persistent phone session); it is monitored, not absorbed.

## Run

```sh
node config.js            # config.js requires this module with your host map
# or under a process manager
pm2 start config.js --name phlo-daemon
```

MIT.
