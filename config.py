#!/usr/bin/python3
import os
import ssl
from pathlib import Path

import mysql.connector
import pika
from mysql.connector import pooling


BASE_DIR = Path(__file__).resolve().parent
_ENV_LOADED = False


def load_env(env_path: Path | None = None) -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_file = env_path or (BASE_DIR / ".env")
    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    _ENV_LOADED = True


def env(key: str, default: str = "") -> str:
    load_env()
    return os.getenv(key, default)


def env_int(key: str, default: int) -> int:
    try:
        return int(env(key, str(default)))
    except ValueError:
        return default


def mqtt_host() -> str:
    return env("MQTT_HOST", "localhost")


def mqtt_port() -> int:
    return env_int("MQTT_PORT", 1883)


def memcache_address() -> tuple[str, int]:
    return env("MEMCACHE_HOST", "localhost"), env_int("MEMCACHE_PORT", 11211)


def mysql_database(test_mode: bool = False) -> str:
    if test_mode:
        return env("MYSQL_DATABASE_TEST", "linesdb_test")
    return env("MYSQL_DATABASE", "linesdb")


def mysql_connect_kwargs(test_mode: bool = False) -> dict:
    kwargs = {
        "user": env("MYSQL_USER"),
        "passwd": env("MYSQL_PASSWORD", ""),
        "database": mysql_database(test_mode),
    }
    unix_socket = env("MYSQL_UNIX_SOCKET")
    if unix_socket:
        kwargs["unix_socket"] = unix_socket
    else:
        kwargs["host"] = env("MYSQL_HOST", "localhost")
        kwargs["port"] = env_int("MYSQL_PORT", 3306)
    return kwargs


def create_mysql_connection(test_mode: bool = False):
    return mysql.connector.connect(**mysql_connect_kwargs(test_mode))


def create_mysql_pool(pool_name: str, pool_size: int = 5, test_mode: bool = False):
    kwargs = mysql_connect_kwargs(test_mode)
    return pooling.MySQLConnectionPool(
        pool_name=pool_name,
        pool_size=pool_size,
        pool_reset_session=True,
        **kwargs,
    )


def local_rabbitmq_params(connection_name: str | None = None, heartbeat: int = 20):
    params = {
        "host": env("LOCAL_RMQ_HOST", "localhost"),
        "port": env_int("LOCAL_RMQ_PORT", 5672),
        "credentials": pika.PlainCredentials(
            username=env("LOCAL_RMQ_USERNAME"),
            password=env("LOCAL_RMQ_PASSWORD"),
            erase_on_connect=False,
        ),
        "heartbeat": heartbeat,
    }
    if connection_name:
        params["client_properties"] = {"connection_name": connection_name}
    return pika.ConnectionParameters(**params)


def remote_ssl_context():
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def remote_rabbitmq_params(connection_name: str | None = None, heartbeat: int = 20):
    params = {
        "host": env("PT_REMOTE_RMQ_HOST", "amqps.plannatech.com"),
        "port": env_int("PT_REMOTE_RMQ_PORT", 5676),
        "virtual_host": env("PT_REMOTE_RMQ_VHOST", "WebRT"),
        "credentials": pika.PlainCredentials(
            username=env("PT_REMOTE_RMQ_USERNAME"),
            password=env("PT_REMOTE_RMQ_PASSWORD"),
            erase_on_connect=False,
        ),
        "ssl_options": pika.SSLOptions(remote_ssl_context()),
        "heartbeat": heartbeat,
    }
    if connection_name:
        params["client_properties"] = {"connection_name": connection_name}
    return pika.ConnectionParameters(**params)


def remote_queue_name() -> str:
    return env("PT_REMOTE_QUEUE", "dds-BetSlipRTv4")


def local_queue_name() -> str:
    return env("PT_LOCAL_QUEUE", "plannatech")


def publish_url(is_feed_forward: bool) -> str:
    key = "PUBLISH_URL_FF" if is_feed_forward else "PUBLISH_URL_DEFAULT"
    default = (
        "http://localhost/receiver_ff.php?base=1"
        if is_feed_forward
        else "http://localhost/receiver_v2.php?base=1"
    )
    return env(key, default)


def shovel_publish_hosts() -> tuple[str, str]:
    return (
        env("SHOVEL_PRIMARY_HOST", env("LOCAL_RMQ_HOST", "localhost")),
        env("SHOVEL_SECONDARY_HOST", env("LOCAL_RMQ_HOST", "localhost")),
    )
