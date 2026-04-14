# plannatech.com

This codebase pulls schedule data, processes Plannatech live feed messages, and republishes them into the local queue and downstream database flow.

## Setup

1. Fill in `.env` with MySQL, RabbitMQ, MQTT, and memcached settings.
2. Make sure the local services and remote Plannatech RabbitMQ endpoint are reachable.
3. Run the shell wrappers from this directory.

## Common commands

- `./plannatech_client.sh` runs the schedule/game sync path.
- `./stream_client.sh` starts the live local consumer.
- `./plannatech_shovel.sh` mirrors the remote feed into the local queue.
