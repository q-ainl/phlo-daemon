# ghcr.io/q-ainl/phlo-daemon
#
# The Phlo engine image plus Node and this repo's daemon, running FrankenPHP and
# the Phlo Realtime daemon side by side in one container. A realtime Phlo app
# (websocket hooks + wsCast) then works with a single `docker compose up`: the app
# serves on :80, the daemon listens on 127.0.0.1:3001, and Caddy proxies /websocket
# to it. Mount the app at /app; set WS_HOST to the host the browser uses (default:
# localhost). Build from the repo root: docker build -f docker/Dockerfile .
FROM ghcr.io/q-ainl/phlo

RUN apt-get update \
 && apt-get install -y --no-install-recommends nodejs npm \
 && rm -rf /var/lib/apt/lists/*

COPY package.json phlo-daemon.js /opt/phlo/
RUN cd /opt/phlo && npm install --omit=dev --no-audit --no-fund && rm -rf /root/.npm

COPY docker/Caddyfile /etc/frankenphp/Caddyfile
COPY docker/daemon-boot.js /opt/phlo/daemon-boot.js
COPY docker/entrypoint.sh /usr/local/bin/phlo-daemon-entrypoint
RUN chmod +x /usr/local/bin/phlo-daemon-entrypoint

ENV PHLO_ENGINE=/phlo
ENTRYPOINT ["/usr/local/bin/phlo-daemon-entrypoint"]
