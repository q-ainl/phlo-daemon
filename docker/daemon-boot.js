// Boot the Phlo Realtime daemon for the app mounted at /app. The websocket host
// maps to the app's entry; wsCast reaches the daemon on 127.0.0.1:3001 (same
// container as FrankenPHP).
process.env.PHLO_ENGINE ??= '/phlo'
const host = process.env.WS_HOST || 'localhost'
require('/opt/phlo/phlo-daemon.js')(3001, 'php-zts', [], {
	[host]: { app: '/app/www/app.php', build: true },
})
