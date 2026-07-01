# ghcr.io/q-ainl/phlo-daemon
#
# The Phlo engine image plus Node and the phlo-daemon package, running FrankenPHP
# and the Phlo Realtime daemon side by side in one container. A realtime Phlo app
# (websocket hooks + wsCast) then works with a single `docker compose up`: the app
# serves on :80, the daemon listens on 127.0.0.1:3001, and Caddy proxies
# /websocket to it. Mount the app at /app; set WS_HOST to the host the browser
# uses (default: localhost).
FROM ghcr.io/q-ainl/phlo

RUN apt-get update \
 && apt-get install -y --no-install-recommends nodejs npm \
 && mkdir -p /opt/phlo && cd /opt/phlo && npm init -y >/dev/null 2>&1 && npm install --no-audit --no-fund phlo-daemon \
 && apt-get purge -y npm && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* /root/.npm

COPY Caddyfile /etc/frankenphp/Caddyfile
COPY daemon-boot.js /opt/phlo/daemon-boot.js
COPY entrypoint.sh /usr/local/bin/phlo-daemon-entrypoint
RUN chmod +x /usr/local/bin/phlo-daemon-entrypoint

ENV PHLO_ENGINE=/phlo
ENTRYPOINT ["/usr/local/bin/phlo-daemon-entrypoint"]
