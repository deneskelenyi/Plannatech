#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
[ -f .env ] && set -a && source .env && set +a

lock_file='/tmp/plannatech_client_py_prod'
lock_retry=0
procname='plannatech_client_py'
/usr/bin/mosquitto_pub -h "${MQTT_HOST:-localhost}" -t "debug" -m "$procname try $(date)"

lock() {
        lockfile-create --use-pid -v -r "$lock_retry" -p "$lock_file" && return 0
        echo "ERROR: Can't get lock"
        /usr/bin/mosquitto_pub -h "${MQTT_HOST:-localhost}" -t "debug" -m "$procname locked $(date)"
        exit $?
}

unlock() {
        lockfile-remove "$lock_file"
}

lock
/usr/bin/mosquitto_pub -h "${MQTT_HOST:-localhost}" -t "debug" -m "$procname clean $(date)"
./lines.py -cached=0 -days=90 -pub=1 -mods=both
/usr/bin/mosquitto_pub -h "${MQTT_HOST:-localhost}" -t "debug" -m "$procname finished $(date)"
unlock
