// Phlo daemon: the optional always-on Phlo service. One node process that (1) runs any Phlo target
// on a dynamic pool of phlo_serve workers, (2) is the websocket server, and (3) schedules tasks.
// Core Phlo works without it (every target is a one-shot CLI call). Consumers reach it over loopback
// HTTP; websocket events are dispatched in-process. Apps self-register their host -> app.php path,
// persisted to disk, so the daemon needs no host configuration and survives a reboot.
//
// Worker protocol (engine phlo_serve): in {"id","target","args"?,"stream"?}; out {"t":"ready"} once,
// then {"id","t":"line","data"}* (when streaming) and one {"id","t":"done","result"} | {"id","t":"error"}.
//
// HTTP + WS on one loopback port:
//   POST /dispatch {app, target, args?, stream?, async?, build?}  run a target for an app
//   POST /register {host, app, build}                            map a host to its app (persisted)
//   POST /message  {host, target?, data}                         broadcast to a host's sockets
//   GET  /health
//   WS   upgrade (any path)                                      a client socket, routed by Host

const fs = require('fs')
const os = require('os')
const http = require('http')
const path = require('path')
const { spawn } = require('child_process')
const { randomBytes } = require('crypto')
const { WebSocketServer } = require('ws')

module.exports = (port, php, schedule = []) => {
	if (!port) throw new Error('Missing port.')
	if (!php) throw new Error('Missing php binary.')

	const LISTEN = '127.0.0.1'
	const MAX_BODY = 1024 * 1024
	const MAX_WORKERS = Math.max(1, os.cpus().length - 1)   // global cap; leaves a core for the webserver
	// Idle/reap timings and the registry path default to production values; the env overrides exist
	// only so the smoke test can run fast and against a throwaway registry file.
	const IDLE_MS = parseInt(process.env.PHLO_DAEMON_IDLE_MS, 10) || 60000   // reap a worker after this long idle
	const REAP_MS = parseInt(process.env.PHLO_DAEMON_REAP_MS, 10) || 30000   // how often the reaper sweeps
	const RESPAWN_BACKOFF = 250
	const MAX_QUEUE = 1000
	const TIMEOUT = 30000
	const RECYCLE = 10000
	const REGISTRY_FILE = process.env.PHLO_DAEMON_REGISTRY || path.join(__dirname, 'registry.json')

	const pools = new Map     // app path -> pool
	const clients = new Map   // host -> Map(token -> Map(socket -> ws))
	const registry = new Map  // host -> { app, build }
	let totalWorkers = 0
	let seq = 0

	const normalizeHost = (value) => {
		if (!value) return null
		const raw = String(value).split(',')[0].trim().toLowerCase()
		if (!raw) return null
		const host = raw.replace(/^https?:\/\//, '').split('/')[0]
		if (host.startsWith('[')) return host.replace(/^\[|\](?::\d+)?$/g, '')
		return host.replace(/:\d+$/, '')
	}

	// --- Host registry: self-registered by apps, persisted so it survives a daemon or app reboot --
	const loadRegistry = () => {
		try {
			const data = JSON.parse(fs.readFileSync(REGISTRY_FILE, 'utf8'))
			for (const [host, entry] of Object.entries(data)) registry.set(host, entry)
		}
		catch {}
	}
	const saveRegistry = () => {
		const out = {}
		for (const [host, entry] of registry) out[host] = entry
		try {
			fs.writeFileSync(REGISTRY_FILE + '.tmp', JSON.stringify(out))
			fs.renameSync(REGISTRY_FILE + '.tmp', REGISTRY_FILE)
		}
		catch (e){
			console.error('registry save failed:', e.message)
		}
	}

	const APP_RE = /^\/[a-zA-Z0-9_./-]+\/app\.php$/
	const runtime = (app, build) => {
		app = String(app || '')
		if (!APP_RE.test(app)) throw new Error(`Invalid app path: ${app}`)
		return { label: app, app, php, build: !!build, timeout: TIMEOUT, recycle: RECYCLE }
	}

	// --- Worker pool ----------------------------------------------------------------------------
	// Each worker runs `php <app.php> phlo_serve`, boots the app once, then answers newline-JSON on
	// stdin. One request in flight per worker; concurrency equals the (dynamic) pool size.

	const getPool = (key) => {
		let pool = pools.get(key)
		if (!pool){
			pool = { key, workers: new Set, queue: [], booting: false }
			pools.set(key, pool)
		}
		return pool
	}

	const hasIdle = (pool) => {
		for (const w of pool.workers) if (w.ready && !w.busy && !w.recycling && !w.reaping) return true
		return false
	}

	// Demand-driven scaling: add ONE worker only when there is queued work, no idle worker can take
	// it, the global cap has room, and this pool is not already booting one. Booting one at a time
	// guards the cold-app compile race. Pools shrink to zero via the idle reaper; no configured size.
	const maybeScale = (rt, pool) => {
		if (!pool.queue.length || pool.booting || hasIdle(pool) || totalWorkers >= MAX_WORKERS) return
		pool.booting = true
		spawnWorker(rt, pool)
	}

	const spawnWorker = (rt, pool) => {
		const w = { proc: null, busy: null, buffer: '', ready: false, events: 0, recycling: false, reaping: false, lastUsed: Date.now() }
		w.proc = spawn(rt.php, [rt.app, 'phlo_serve'])
		w.proc.stdout.on('data', (data) => {
			w.buffer += data.toString()
			let idx
			while ((idx = w.buffer.indexOf('\n')) !== -1){
				const line = w.buffer.slice(0, idx)
				w.buffer = w.buffer.slice(idx + 1)
				if (line.trim()) onFrame(rt, pool, w, line)
			}
		})
		w.proc.stderr.on('data', (data) => console.error(`PHP stderr (worker ${rt.label}):\n${data.toString()}`))
		w.proc.on('error', (err) => onWorkerGone(rt, pool, w, `spawn error: ${err.message}`))
		w.proc.on('exit', (code, signal) => onWorkerGone(rt, pool, w, `exited (${signal || code})`))
		pool.workers.add(w)
		totalWorkers++
		return w
	}

	const killWorker = (w) => {
		try { w.proc.kill('SIGKILL') }
		catch {}
	}

	const onFrame = (rt, pool, w, line) => {
		let frame
		try { frame = JSON.parse(line) }
		catch {
			console.error(`worker desync on ${rt.label}, raw: ${line}`)
			return killWorker(w)
		}
		if (frame.t === 'ready'){
			w.ready = true
			pool.booting = false
			pump(rt, pool)
			return maybeScale(rt, pool)
		}
		if (frame.t === 'fatal'){
			console.error(`worker fatal on ${rt.label}: ${frame.message}`)
			return killWorker(w)
		}
		const job = w.busy
		if (!job) return
		if (frame.t === 'line'){
			if (job.onLine){
				try { job.onLine(frame.data) }
				catch (e){ console.error('onLine error:', e.message) }
			}
			return
		}
		if (frame.t === 'done' || frame.t === 'error'){
			clearTimeout(job.timer)
			w.busy = null
			w.events++
			w.lastUsed = Date.now()
			frame.t === 'done' ? job.resolve(frame.result) : job.reject(new Error(frame.message || 'worker error'))
			if (rt.recycle && w.events >= rt.recycle){
				w.recycling = true
				return killWorker(w)
			}
			pump(rt, pool)
		}
	}

	const pump = (rt, pool) => {
		for (const w of pool.workers){
			if (!w.ready || w.busy || w.recycling || w.reaping) continue
			if (!pool.queue.length) break
			const job = pool.queue.shift()
			w.busy = job
			w.lastUsed = Date.now()
			job.timer = setTimeout(() => onJobTimeout(w, job), rt.timeout)
			try { w.proc.stdin.write(JSON.stringify({ id: job.id, target: job.target, args: job.args, stream: job.stream }) + '\n') }
			catch { /* the exit handler will reject the in-flight job and respawn */ }
		}
		maybeScale(rt, pool)
	}

	const onJobTimeout = (w, job) => {
		if (w.busy !== job) return
		w.busy = null
		job.reject(new Error('handler timeout'))
		killWorker(w)
	}

	const onWorkerGone = (rt, pool, w, reason) => {
		if (!pool.workers.has(w)) return
		pool.workers.delete(w)
		totalWorkers--
		if (!w.ready) pool.booting = false // it died mid-boot; free the boot slot
		const job = w.busy
		if (job){
			clearTimeout(job.timer)
			w.busy = null
			job.reject(new Error(`worker ${reason}`))
		}
		if (!w.recycling && !w.reaping) console.error(`worker gone on ${rt.label}: ${reason}`)
		if (pool.queue.length) setTimeout(() => {
			maybeScale(rt, pool)
			pump(rt, pool)
		}, RESPAWN_BACKOFF)
	}

	const dispatchPool = (rt, target, args, onLine, stream) => new Promise((resolve, reject) => {
		const pool = getPool(rt.app)
		if (pool.queue.length >= MAX_QUEUE) return reject(new Error('queue overflow'))
		pool.queue.push({ id: (++seq).toString(36), target, args, stream: !!stream, onLine, resolve, reject, timer: null })
		pump(rt, pool)
	})

	// One-shot fallback: a fresh process per call, for build-mode (dev) apps so hot-reload keeps
	// working. Args pass as CLI argv, so only string-ish positional args round-trip.
	const spawnOnce = (rt, target, args, onLine) => new Promise((resolve, reject) => {
		const proc = spawn(rt.php, [rt.app, target, ...args.map(String)])
		let buffer = ''
		let out = ''
		proc.stdout.on('data', (data) => {
			buffer += data.toString()
			out += data.toString()
			if (onLine){
				const lines = buffer.split('\n')
				buffer = lines.pop()
				for (const line of lines) if (line.trim()) onLine(line)
			}
		})
		proc.stdout.on('end', () => { if (onLine && buffer.trim()) onLine(buffer) })
		proc.stderr.on('data', (data) => console.error(`PHP stderr for '${target}' on ${rt.label}:\n${data.toString()}`))
		proc.on('error', (err) => reject(new Error(`Failed to start PHP for '${target}' on ${rt.label}: ${err.message}`)))
		proc.on('close', (code) => code === 0 ? resolve(out.trim() || null) : reject(new Error(`PHP for '${target}' on ${rt.label} exited with code ${code}`)))
	})

	// Build-mode apps run one-shot (hot reload); release apps run on the pool.
	const dispatch = (rt, target, args = [], onLine = null, stream = false) =>
		rt.build ? spawnOnce(rt, target, args, onLine) : dispatchPool(rt, target, args, onLine, stream)

	// --- WebSocket -------------------------------------------------------------------------------
	const hostClients = (host, create = false) => {
		if (!clients.has(host) && create) clients.set(host, new Map)
		return clients.get(host)
	}
	const tokenClients = (host, token, create = false) => {
		const map = hostClients(host, create)
		if (!map) return null
		if (!map.has(token) && create) map.set(token, new Map)
		return map.get(token)
	}

	const parseTarget = (target) => {
		target = String(target || 'all')
		if (target === 'all') return { mode: 'all' }
		if (target.startsWith('token:not:')) return { mode: 'not', value: target.slice(10) }
		if (target.startsWith('token:')) return { mode: 'token', value: target.slice(6) }
		throw new Error(`Invalid target: ${target}`)
	}

	const sendToHost = (host, target, dataString) => {
		const map = hostClients(host)
		if (!map) return 0
		let sent = 0
		const parsed = parseTarget(target)
		const send = (token, clientWs) => {
			if (parsed.mode === 'not' && token === parsed.value) return
			if (clientWs.readyState === 1){
				clientWs.send(dataString)
				sent++
			}
		}
		if (parsed.mode === 'all' || parsed.mode === 'not'){
			for (const [token, sockets] of map.entries()) for (const [, clientWs] of sockets.entries()) send(token, clientWs)
			return sent
		}
		if (parsed.mode === 'token'){
			const sockets = tokenClients(host, parsed.value)
			if (!sockets) return sent
			for (const [, clientWs] of sockets.entries()) send(parsed.value, clientWs)
			return sent
		}
		return sent
	}

	// Resolve a host to its registered app, then dispatch a websocket hook in-process.
	const wsDispatch = (host, hook, args, onLine = null) => {
		const entry = registry.get(host)
		if (!entry) return Promise.reject(new Error(`Host is not registered: ${host}`))
		return dispatch(runtime(entry.app, entry.build), `websocket::${hook}`, args, onLine, !!onLine)
	}

	const rejected = (value) => value === false || value === 'false'

	const wss = new WebSocketServer({ noServer: true })
	wss.on('connection', (ws, request, host, token, socket) => {
		console.log(`connected: ${host} ${token} ${socket}`)
		tokenClients(host, token, true).set(socket, ws)
		ws.host = host
		ws.token = token
		ws.socket = socket
		wsDispatch(host, 'connect', [host, token, socket]).then(ok => {
			if (rejected(ok)){
				console.log(`connect rejected: ${host} ${token} ${socket}`)
				ws.close()
			}
		}).catch(err => console.error('Phlo could not handle connect:', err.message))
		ws.on('message', (message) => {
			wsDispatch(ws.host, 'receive', [ws.host, ws.token, ws.socket, message.toString()], line => {
				if (ws.readyState === 1) ws.send(line)
			}).catch(err => console.error('Phlo could not handle receive:', err.message))
		})
		ws.on('close', () => {
			console.log(`disconnected: ${ws.host} ${ws.token} ${ws.socket}`)
			const sockets = tokenClients(ws.host, ws.token)
			if (sockets){
				sockets.delete(ws.socket)
				if (!sockets.size) hostClients(ws.host)?.delete(ws.token)
			}
			if (hostClients(ws.host)?.size === 0) clients.delete(ws.host)
			wsDispatch(ws.host, 'close', [ws.host, ws.token, ws.socket]).catch(err => console.error('Phlo could not handle close:', err.message))
		})
		ws.on('error', (error) => console.error(`Client error for ${host} ${token} ${socket}:`, error))
	})

	// --- HTTP ------------------------------------------------------------------------------------
	const requestHost = (request) => normalizeHost(request.headers['x-forwarded-host'] || request.headers.host)

	const cookies = (request) => Object.fromEntries((request.headers.cookie || '').split(';').filter(Boolean).map(part => {
		const [key, ...valParts] = part.trim().split('=')
		return [key, decodeURIComponent(valParts.join('='))]
	}))

	const getJSONBody = (req) => new Promise((resolve, reject) => {
		let body = ''
		let rejectedBody = false
		req.on('data', chunk => {
			body += chunk.toString()
			if (!rejectedBody && body.length > MAX_BODY){
				rejectedBody = true
				reject(new Error('Request body too large.'))
			}
		})
		req.on('end', () => {
			if (rejectedBody) return
			try { resolve(body ? JSON.parse(body) : {}) }
			catch { reject(new Error('Invalid JSON body.')) }
		})
		req.on('error', err => reject(err))
	})

	const server = http.createServer(async (req, res) => {
		const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`)
		if (url.pathname === '/health' && req.method === 'GET'){
			const stats = {}
			for (const [key, pool] of pools.entries()){
				let busy = 0
				for (const w of pool.workers) if (w.busy) busy++
				stats[key] = { workers: pool.workers.size, busy, queued: pool.queue.length }
			}
			const live = {}
			for (const [host, tokens] of clients.entries()){
				let sockets = 0
				for (const socketMap of tokens.values()) sockets += socketMap.size
				live[host] = { tokens: tokens.size, sockets }
			}
			res.writeHead(200, {'Content-Type': 'application/json'})
			res.end(JSON.stringify({ status: 'ok', workers: totalWorkers, cap: MAX_WORKERS, pools: stats, sockets: live, registered: [...registry.keys()] }))
			return
		}
		if (url.pathname === '/register' && req.method === 'POST'){
			try {
				const body = await getJSONBody(req)
				const host = normalizeHost(body.host)
				const app = String(body.app || '')
				if (!host) throw new Error('Host is required.')
				if (!APP_RE.test(app)) throw new Error(`Invalid app path: ${app}`)
				const entry = { app, build: !!body.build }
				const prev = registry.get(host)
				registry.set(host, entry)
				if (!prev || prev.app !== entry.app || prev.build !== entry.build) saveRegistry()
				res.writeHead(200, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'ok' }))
			}
			catch (error){
				res.writeHead(400, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'error', message: error.message }))
			}
			return
		}
		if (url.pathname === '/message' && req.method === 'POST'){
			try {
				const body = await getJSONBody(req)
				const host = normalizeHost(body.host || req.headers['x-phlo-host'])
				if (!host) throw new Error('Host is required.')
				const target = body.target || 'all'
				const dataString = JSON.stringify(body.data || {})
				const sent = sendToHost(host, target, dataString)
				console.log(`cast: ${host} ${target} ${sent}`)
				res.writeHead(200, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'ok', host, sent }))
			}
			catch (error){
				res.writeHead(400, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'error', message: error.message }))
			}
			return
		}
		if (url.pathname === '/dispatch' && req.method === 'POST'){
			try {
				const body = await getJSONBody(req)
				if (!body.app) throw new Error('Missing app.')
				if (!body.target) throw new Error('Missing target.')
				const rt = runtime(body.app, body.build)
				const target = String(body.target)
				const args = Array.isArray(body.args) ? body.args : (body.args == null ? [] : [body.args])
				if (body.async){
					dispatch(rt, target, args, null, false).catch(e => console.error(`async dispatch '${target}' on ${rt.label}: ${e.message}`))
					res.writeHead(202, {'Content-Type': 'application/json'})
					res.end(JSON.stringify({ status: 'ok', queued: true }))
					return
				}
				if (body.stream){
					res.writeHead(200, {'Content-Type': 'application/x-ndjson'})
					try {
						const result = await dispatch(rt, target, args, line => res.write(JSON.stringify({ t: 'line', data: line }) + '\n'), true)
						res.write(JSON.stringify({ t: 'done', result }) + '\n')
					}
					catch (error){
						res.write(JSON.stringify({ t: 'error', message: error.message }) + '\n')
					}
					res.end()
					return
				}
				const result = await dispatch(rt, target, args, null, false)
				res.writeHead(200, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'ok', result }))
			}
			catch (error){
				res.writeHead(400, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'error', message: error.message }))
			}
			return
		}
		res.writeHead(404).end()
	})

	// WebSocket upgrade: authenticate at the handshake from the `token` cookie, before accepting the
	// socket. The connect path is whatever the reverse proxy forwards here, so it is not pinned.
	server.on('upgrade', async (request, socketStream, head) => {
		try {
			const host = normalizeHost(requestHost(request))
			if (!host) throw new Error('Host is required.')
			const token = cookies(request)['token']
			if (!token) throw new Error('Authentication cookie not found.')
			const socket = randomBytes(8).toString('hex')
			const ok = await wsDispatch(host, 'auth', [host, token, socket])
			if (rejected(ok)) throw new Error('Authentication rejected.')
			wss.handleUpgrade(request, socketStream, head, (ws) => wss.emit('connection', ws, request, host, token, socket))
		}
		catch (error){
			console.log(`Unauthorized: ${error.message}`)
			socketStream.write('HTTP/1.1 401 Unauthorized\r\n\r\n')
			socketStream.destroy()
		}
	})

	const shutdown = () => {
		for (const pool of pools.values()){
			for (const w of pool.workers){
				try { w.proc.kill('SIGTERM') }
				catch {}
			}
		}
	}
	process.on('SIGTERM', () => {
		shutdown()
		process.exit(0)
	})
	process.on('SIGINT', () => {
		shutdown()
		process.exit(0)
	})

	// Idle reaper: kill workers unused for IDLE_MS so pools shrink back to zero, and drop empty pools.
	setInterval(() => {
		const now = Date.now()
		for (const [key, pool] of pools){
			for (const w of pool.workers){
				if (w.ready && !w.busy && !w.recycling && !w.reaping && now - w.lastUsed > IDLE_MS){
					w.reaping = true
					killWorker(w)
				}
			}
			if (!pool.workers.size && !pool.queue.length && !pool.booting) pools.delete(key)
		}
	}, REAP_MS)

	// Scheduler: run targets on their own interval on the pool (replaces cron for tasks/poller).
	// Each entry is {app, target, every}; the first run is one interval after boot, like cron.
	for (const s of schedule){
		const app = String((s && s.app) || '')
		const target = String((s && s.target) || '')
		const every = Math.max(1, parseInt(s && s.every, 10) || 0)
		const build = !!(s && s.build)
		if (!APP_RE.test(app) || !target || !every) continue
		setInterval(() => {
			try { dispatch(runtime(app, build), target, [], null, false).catch(e => console.error(`schedule '${target}' on ${app}: ${e.message}`)) }
			catch (e){ console.error(`schedule '${target}' on ${app}: ${e.message}`) }
		}, every * 1000)
	}

	loadRegistry()
	server.listen(port, LISTEN, () => console.log(`Phlo daemon listening on ${LISTEN}:${port} (${registry.size} hosts, ${schedule.length} scheduled)`))

	return { dispatch, runtime, registry, pools }
}
