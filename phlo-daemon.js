// Phlo daemon: one optional per-host node sidecar that dispatches ANY Phlo target to a pool of
// persistent `phlo_serve` workers. This is a generic central engine only: it knows nothing about
// websockets, tasks or any specific consumer. Consumers (the websocket package, the runtime
// helpers, schedulers) live in their own packages and talk to it over HTTP POST /dispatch.
//
// Worker protocol (engine `phlo_serve`): out {"t":"ready"} once, then per request {"id","t":"line",
// "data"}* (when stream) and exactly one {"id","t":"done","result"} or {"id","t":"error","message"}.
// In: {"id","target","args"?,"stream"?}. A worker handles one request at a time; concurrency is the
// pool size. Hosts with workers 0 fall back to a fresh one-shot CLI process per call.
//
// HTTP surface (bind 127.0.0.1 by default; local-only):
//   POST /dispatch {host,target,args?,stream?,async?} -> sync {status,result} | 202 {queued} |
//        when stream: an application/x-ndjson stream of {t:line,data}* then {t:done,result}|{t:error}
//   GET  /health -> pool stats per host

module.exports = (...input) => {
	const http = require('http')
	const { spawn } = require('child_process')

	const config = normalizeConfig(...input)
	const pools = new Map
	const RESPAWN_BACKOFF = 250
	const MAX_QUEUE = 1000
	let seq = 0

	const normalizeHost = (value) => {
		if (!value) return null
		const raw = String(value).split(',')[0].trim().toLowerCase()
		if (!raw) return null
		const host = raw.replace(/^https?:\/\//, '').split('/')[0]
		if (host.startsWith('[')) return host.replace(/^\[|\](?::\d+)?$/g, '')
		return host.replace(/:\d+$/, '')
	}

	const runtimeForHost = (host) => {
		host = normalizeHost(host)
		if (!host) throw new Error('Host is required.')
		const entry = config.hosts[host]
		if (!entry) throw new Error(`Host is not configured: ${host}`)
		return { host, app: entry.app, php: config.php, workers: entry.workers, timeout: entry.timeout, recycle: entry.recycle }
	}

	// A caller that knows its own app.php (the runtime helpers) dispatches by app path: no host map
	// needed. Pool tuning is inherited if the app is also a configured (ws) host, else defaults.
	const APP_RE = /^\/[a-zA-Z0-9_./-]+\/app\.php$/
	const runtimeForApp = (app) => {
		app = String(app || '')
		if (!APP_RE.test(app)) throw new Error(`Invalid app path: ${app}`)
		const cfg = Object.values(config.hosts).find(h => h.app === app)
		return { host: app, app, php: config.php, workers: cfg ? cfg.workers : config.defaultWorkers, timeout: cfg ? cfg.timeout : 30000, recycle: cfg ? cfg.recycle : 10000 }
	}

	// --- Worker pool ---------------------------------------------------------
	// Each worker runs `php <app.php> phlo_serve`, boots the app once, then answers newline-JSON
	// requests on stdin. One request in flight per worker; correlation via the frame id.

	const getPool = (host) => {
		let pool = pools.get(host)
		if (!pool){
			pool = { workers: new Set, queue: [] }
			pools.set(host, pool)
		}
		return pool
	}

	// Bring a pool up to its worker count, booting one at a time: the first worker may compile the
	// app (build mode) on boot, and letting several race would clobber the written php/*.php.
	const ensureWorkers = (runtime, pool) => {
		if (pool.booting || pool.workers.size >= runtime.workers) return
		pool.booting = true
		spawnWorker(runtime, pool)
	}

	const spawnWorker = (runtime, pool) => {
		const w = { proc: null, busy: null, buffer: '', ready: false, events: 0, recycling: false }
		w.proc = spawn(runtime.php, [runtime.app, 'phlo_serve'])
		w.proc.stdout.on('data', (data) => {
			w.buffer += data.toString()
			let idx
			while ((idx = w.buffer.indexOf('\n')) !== -1){
				const line = w.buffer.slice(0, idx)
				w.buffer = w.buffer.slice(idx + 1)
				if (line.trim()) onFrame(runtime, pool, w, line)
			}
		})
		w.proc.stderr.on('data', (data) => console.error(`PHP stderr (worker ${runtime.host}):\n${data.toString()}`))
		w.proc.on('error', (err) => onWorkerGone(runtime, pool, w, `spawn error: ${err.message}`))
		w.proc.on('exit', (code, signal) => onWorkerGone(runtime, pool, w, `exited (${signal || code})`))
		pool.workers.add(w)
		return w
	}

	const killWorker = (w) => {
		try { w.proc.kill('SIGKILL') }
		catch {}
	}

	const onFrame = (runtime, pool, w, line) => {
		let frame
		try { frame = JSON.parse(line) }
		catch {
			console.error(`worker desync on ${runtime.host}, raw: ${line}`)
			return killWorker(w)
		}
		if (frame.t === 'ready'){
			w.ready = true
			pool.booting = false
			ensureWorkers(runtime, pool)
			return pump(runtime, pool)
		}
		if (frame.t === 'fatal'){
			console.error(`worker fatal on ${runtime.host}: ${frame.message}`)
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
			frame.t === 'done' ? job.resolve(frame.result) : job.reject(new Error(frame.message || 'worker error'))
			if (runtime.recycle && w.events >= runtime.recycle){
				w.recycling = true
				return killWorker(w)
			}
			pump(runtime, pool)
		}
	}

	const pump = (runtime, pool) => {
		for (const w of pool.workers){
			if (!w.ready || w.busy || w.recycling) continue
			if (!pool.queue.length) break
			const job = pool.queue.shift()
			w.busy = job
			job.timer = setTimeout(() => onJobTimeout(w, job), runtime.timeout)
			try { w.proc.stdin.write(JSON.stringify({ id: job.id, target: job.target, args: job.args, stream: job.stream }) + '\n') }
			catch { /* the exit handler will reject the in-flight job and respawn */ }
		}
	}

	const onJobTimeout = (w, job) => {
		if (w.busy !== job) return
		w.busy = null
		job.reject(new Error('handler timeout'))
		killWorker(w)
	}

	const onWorkerGone = (runtime, pool, w, reason) => {
		if (!pool.workers.has(w)) return
		pool.workers.delete(w)
		if (!w.ready) pool.booting = false // it died mid-boot; free the boot slot
		const job = w.busy
		if (job){
			clearTimeout(job.timer)
			w.busy = null
			job.reject(new Error(`worker ${reason}`))
		}
		if (!w.recycling) console.error(`worker gone on ${runtime.host}: ${reason}`)
		setTimeout(() => {
			ensureWorkers(runtime, pool)
			pump(runtime, pool)
		}, RESPAWN_BACKOFF)
	}

	const dispatchPool = (runtime, target, args, onLine, stream) => new Promise((resolve, reject) => {
		const pool = getPool(runtime.app)
		ensureWorkers(runtime, pool)
		if (pool.queue.length >= MAX_QUEUE) return reject(new Error('queue overflow'))
		pool.queue.push({ id: (++seq).toString(36), target, args, stream: !!stream, onLine, resolve, reject, timer: null })
		pump(runtime, pool)
	})

	// One-shot fallback: a fresh PHP CLI process per call, for hosts configured without workers
	// (dev / build mode). Args pass as CLI argv, so only string-ish positional args round-trip; the
	// pool path carries full JSON args.
	const spawnOnce = (runtime, target, args, onLine) => new Promise((resolve, reject) => {
		const proc = spawn(runtime.php, [runtime.app, target, ...args.map(String)])
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
		proc.stderr.on('data', (data) => console.error(`PHP stderr for '${target}' on ${runtime.host}:\n${data.toString()}`))
		proc.on('error', (err) => reject(new Error(`Failed to start PHP for '${target}' on ${runtime.host}: ${err.message}`)))
		proc.on('close', (code) => code === 0 ? resolve(out.trim() || null) : reject(new Error(`PHP for '${target}' on ${runtime.host} exited with code ${code}`)))
	})

	const dispatch = (runtime, target, args = [], onLine = null, stream = false) =>
		runtime.workers > 0 ? dispatchPool(runtime, target, args, onLine, stream) : spawnOnce(runtime, target, args, onLine)

	const getJSONBody = (req) => new Promise((resolve, reject) => {
		let body = ''
		let rejected = false
		req.on('data', chunk => {
			body += chunk.toString()
			if (!rejected && body.length > config.maxBody){
				rejected = true
				reject(new Error('Request body too large.'))
			}
		})
		req.on('end', () => {
			if (rejected) return
			try { resolve(body ? JSON.parse(body) : {}) }
			catch { reject(new Error('Invalid JSON body.')) }
		})
		req.on('error', err => reject(err))
	})

	const server = http.createServer(async (req, res) => {
		const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`)
		if (url.pathname === '/health' && req.method === 'GET'){
			const hosts = {}
			for (const [host, pool] of pools.entries()){
				let busy = 0
				for (const w of pool.workers) if (w.busy) busy++
				hosts[host] = { workers: pool.workers.size, busy, queued: pool.queue.length }
			}
			res.writeHead(200, {'Content-Type': 'application/json'})
			res.end(JSON.stringify({ status: 'ok', hosts, configured: Object.keys(config.hosts) }))
			return
		}
		if (url.pathname === '/dispatch' && req.method === 'POST'){
			try {
				const body = await getJSONBody(req)
				const runtime = body.app ? runtimeForApp(body.app) : runtimeForHost(body.host || req.headers['x-phlo-host'])
				if (!body.target) throw new Error('Missing target.')
				const target = String(body.target)
				const args = Array.isArray(body.args) ? body.args : (body.args == null ? [] : [body.args])
				if (body.async){
					dispatch(runtime, target, args, null, false).catch(e => console.error(`async dispatch '${target}' on ${runtime.host}: ${e.message}`))
					res.writeHead(202, {'Content-Type': 'application/json'})
					res.end(JSON.stringify({ status: 'ok', host: runtime.host, queued: true }))
					return
				}
				if (body.stream){
					res.writeHead(200, {'Content-Type': 'application/x-ndjson'})
					try {
						const result = await dispatch(runtime, target, args, line => res.write(JSON.stringify({ t: 'line', data: line }) + '\n'), true)
						res.write(JSON.stringify({ t: 'done', result }) + '\n')
					}
					catch (error){
						res.write(JSON.stringify({ t: 'error', message: error.message }) + '\n')
					}
					res.end()
					return
				}
				const result = await dispatch(runtime, target, args, null, false)
				res.writeHead(200, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'ok', host: runtime.host, result }))
			}
			catch (error){
				res.writeHead(400, {'Content-Type': 'application/json'})
				res.end(JSON.stringify({ status: 'error', message: error.message }))
			}
			return
		}
		res.writeHead(404).end()
	})

	const shutdown = () => {
		for (const pool of pools.values()){
			for (const w of pool.workers){
				try { w.proc.kill('SIGTERM') }
				catch {}
			}
		}
	}
	process.on('SIGTERM', () => { shutdown(); process.exit(0) })
	process.on('SIGINT', () => { shutdown(); process.exit(0) })

	// Scheduler: run targets on their own interval via the pool (replaces cron for tasks/poller).
	// Each entry is {host, target, every}; the first run is one interval after boot, like cron.
	for (const s of config.schedule){
		setInterval(() => {
			try { dispatch(runtimeForHost(s.host), s.target, [], null, false).catch(e => console.error(`schedule '${s.target}'@${s.host}: ${e.message}`)) }
			catch (e){ console.error(`schedule '${s.target}'@${s.host}: ${e.message}`) }
		}, s.every * 1000)
	}

	server.listen(config.port, config.listen, () => console.log(`Phlo daemon listening on ${config.listen}:${config.port}${config.schedule.length ? ` (scheduling ${config.schedule.length})` : ''}`))

	return { dispatch, runtimeForHost, pools }
}

function normalizeConfig(port, php, hostMap, listen = '127.0.0.1', maxBody = 1024 * 1024, schedule = [], defaultWorkers = 2) {
	if (!port) throw new Error('Missing port.')
	if (!php) throw new Error('Missing php binary.')
	if (!hostMap || typeof hostMap !== 'object') throw new Error('Missing hosts config.')
	const hosts = {}
	for (const [host, value] of Object.entries(hostMap)){
		const key = String(host).trim().toLowerCase()
		const isObj = value && typeof value === 'object'
		const file = String((isObj ? value.app : value) || '').trim()
		if (!key || !file) continue
		if (!file.endsWith('/app.php')) throw new Error(`Host ${key} must point to app.php.`)
		if (!/^\/[a-zA-Z0-9_./-]+$/.test(file)) throw new Error(`Invalid app path for host ${key}.`)
		const workers = isObj ? Math.max(0, parseInt(value.workers, 10) || 0) : 0
		const timeout = isObj && value.timeout ? Math.max(1000, parseInt(value.timeout, 10)) : 30000
		const recycle = isObj && 'recycle' in value ? Math.max(0, parseInt(value.recycle, 10) || 0) : 10000
		hosts[key] = { app: file, workers, timeout, recycle }
	}
	const sched = (Array.isArray(schedule) ? schedule : []).map(s => ({
		host: String((s && s.host) || '').trim().toLowerCase(),
		target: String((s && s.target) || '').trim(),
		every: Math.max(1, parseInt(s && s.every, 10) || 0),
	})).filter(s => s.host && s.target && s.every)
	return { port, php, hosts, listen, maxBody, schedule: sched, defaultWorkers: Math.max(0, parseInt(defaultWorkers, 10) || 0) }
}
