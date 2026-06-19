// Smoke test for the node daemon: the behaviour phpunit can't reach. Uses a fake-php stub (no real
// Phlo app) and throwaway timings/registry via env overrides. Covers: register -> persist ->
// reload (restart-safe), demand spawn + idle reap, and the websocket token gate + cast fanout.
const assert = require('assert')
const http = require('http')
const fs = require('fs')
const os = require('os')
const path = require('path')
const WebSocket = require('ws')

const STUB = path.join(__dirname, 'fake-php.js')
const APP = '/tmp/phlo-daemon-smoke/app.php'   // never read; just has to match the daemon's app-path shape
const REG = path.join(os.tmpdir(), 'phlo-daemon-smoke-registry.json')

process.env.PHLO_DAEMON_IDLE_MS = '250'
process.env.PHLO_DAEMON_REAP_MS = '120'
process.env.PHLO_DAEMON_REGISTRY = REG

const sleep = ms => new Promise(r => setTimeout(r, ms))

const call = (port, p, body) => new Promise((resolve, reject) => {
	const data = body ? JSON.stringify(body) : null
	const r = http.request({ hostname: '127.0.0.1', port, path: p, method: body ? 'POST' : 'GET', headers: data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {} }, res => {
		let b = ''
		res.on('data', c => b += c)
		res.on('end', () => resolve({ status: res.statusCode, json: b ? JSON.parse(b) : null }))
	})
	r.on('error', reject)
	if (data) r.write(data)
	r.end()
})

let passed = 0
const ok = (cond, msg) => {
	assert.ok(cond, msg)
	console.log('  ok -', msg)
	passed++
}

;(async () => {
	try { fs.unlinkSync(REG) }
	catch {}
	const make = require('../phlo-daemon.js')

	make(3099, STUB)
	await sleep(200)

	let r = await call(3099, '/register', { host: 'test.local', app: APP, build: false })
	ok(r.json && r.json.status === 'ok', 'register accepted')
	ok(fs.existsSync(REG), 'registry persisted to disk')
	r = await call(3099, '/health')
	ok(r.json.registered.includes('test.local'), 'host appears in /health registered')

	r = await call(3099, '/dispatch', { app: APP, target: 'foo::bar', args: ['x'] })
	ok(r.json.status === 'ok' && r.json.result && r.json.result.target === 'foo::bar' && r.json.result.args[0] === 'x', 'dispatch returns the worker result')
	r = await call(3099, '/health')
	ok(r.json.workers >= 1, 'a worker spawned on demand')

	await sleep(600)
	r = await call(3099, '/health')
	ok(r.json.workers === 0, 'idle worker reaped back to zero')

	const received = await new Promise((resolve) => {
		const ws = new WebSocket('ws://127.0.0.1:3099/anything', { headers: { host: 'test.local', cookie: 'token=abc' } })
		ws.on('open', () => call(3099, '/message', { host: 'test.local', target: 'all', data: { hello: 1 } }))
		ws.on('message', m => { resolve(m.toString()); ws.close() })
		ws.on('error', () => resolve(null))
		setTimeout(() => resolve(null), 4000)
	})
	ok(received && JSON.parse(received).hello === 1, 'authed client receives a /message broadcast')

	const code = await new Promise((resolve) => {
		const ws = new WebSocket('ws://127.0.0.1:3099/anything', { headers: { host: 'test.local' } })
		ws.on('open', () => resolve('opened'))
		ws.on('unexpected-response', (q, res) => resolve(res.statusCode))
		ws.on('error', () => resolve('err'))
		setTimeout(() => resolve('timeout'), 3000)
	})
	ok(code === 401, 'upgrade without a token cookie is refused (401), got: ' + code)

	make(3098, STUB)   // a fresh daemon, same registry file
	await sleep(200)
	r = await call(3098, '/health')
	ok(r.json.registered.includes('test.local'), 'a fresh daemon reloads the persisted registry (restart-safe)')

	try { fs.unlinkSync(REG) }
	catch {}
	console.log(`\nsmoke: ${passed} checks passed`)
	process.exit(0)
})().catch(e => {
	console.error('SMOKE FAILED:', e.message)
	process.exit(1)
})
