#!/usr/bin/env node
// Test stub standing in for `php <app.php> <target> [args]`. In phlo_serve mode it speaks the
// worker protocol (ready, then a done frame per request); otherwise it runs one-shot and prints a
// JSON result. websocket::auth returns true so the upgrade is accepted; other targets echo back.
const args = process.argv.slice(2)   // [app, target, ...rest]
const target = args[1]

if (target === 'phlo_serve'){
	process.stdout.write(JSON.stringify({ t: 'ready' }) + '\n')
	let buf = ''
	process.stdin.on('data', d => {
		buf += d
		let i
		while ((i = buf.indexOf('\n')) !== -1){
			const line = buf.slice(0, i)
			buf = buf.slice(i + 1)
			if (!line.trim()) continue
			let job
			try { job = JSON.parse(line) }
			catch { continue }
			const result = job.target === 'websocket::auth' ? true : { target: job.target, args: job.args }
			if (job.stream) process.stdout.write(JSON.stringify({ id: job.id, t: 'line', data: 'chunk' }) + '\n')
			process.stdout.write(JSON.stringify({ id: job.id, t: 'done', result }) + '\n')
		}
	})
}
else {
	process.stdout.write(JSON.stringify({ target, args: args.slice(2) }))
}
