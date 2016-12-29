#!/bin/bash
# source https://github.com/31z4/storm-docker/tree/master/1.0.2
set -e
CONFIG="$STORM_CONF_DIR/storm.yaml"
# Allow the container to be started with `--user`
if [ "$1" = 'storm' -a "$(id -u)" = '0' ]; then
    chown -R "$STORM_USER" "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"
    su - "$STORM_USER" -c "$0" "$@"

fi

exec "$@"
