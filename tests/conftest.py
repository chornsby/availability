import contextlib
import os
import uuid
from typing import List

import click.testing
import kafka.admin
import psycopg2
import pytest
from psycopg2 import extensions
from psycopg2.sql import SQL, Identifier


@pytest.fixture()
def cli_runner() -> click.testing.CliRunner:
    return click.testing.CliRunner()


@pytest.fixture()
def kafka_brokers() -> List[str]:
    return os.environ["KAFKA_BROKERS"].split(",")


@pytest.fixture()
def kafka_topic(kafka_brokers) -> str:
    name = f"checks-{uuid.uuid4()}"
    topic = kafka.admin.NewTopic(name=name, num_partitions=1, replication_factor=1)

    client = kafka.KafkaAdminClient(bootstrap_servers=kafka_brokers)
    client.create_topics([topic])

    yield name

    client.delete_topics([name])


@pytest.fixture()
def postgres_uri() -> str:
    name = f"checks-{uuid.uuid4()}"
    uri = os.environ["POSTGRES_URI"]

    with contextlib.closing(psycopg2.connect(uri)) as conn:
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute(SQL("CREATE DATABASE {}").format(Identifier(name)))

        yield f"{uri}/{name}"

        conn.cursor().execute(SQL("DROP DATABASE {}").format(Identifier(name)))
