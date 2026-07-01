#!/bin/sh
# Start the Phlo Realtime daemon in the background, then FrankenPHP in the
# foreground (so the container lives and dies with the web server).
set -e
node /opt/phlo/daemon-boot.js &
exec frankenphp run --config /etc/frankenphp/Caddyfile
