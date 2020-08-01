import contextlib
import uuid
from unittest import mock

import psycopg2
import psycopg2.extras
import pytest
import kafka

from availability import writer
from availability.common import Check, Match, value_serializer
from tests.utils import VariableBoolean


@pytest.fixture()
def messages(kafka_brokers, kafka_topic):
    producer = kafka.KafkaProducer(
        bootstrap_servers=kafka_brokers, value_serializer=value_serializer
    )
    producer.send(
        kafka_topic,
        value=Check(
            url="https://www.google.com",
            response_time=0.03,
            status_code=200,
            match=Match.MATCHED,
        ).json(),
    )
    producer.send(
        kafka_topic,
        value=Check(
            url="https://www.google.fi",
            response_time=0.1,
            status_code=200,
            match=Match.UNUSED,
        ).json(),
    )
    producer.send(
        kafka_topic,
        value=Check(
            url="https://www.google.com",
            response_time=15.0,
            status_code=404,
            match=Match.UNMATCHED,
        ).json(),
    )
    producer.flush()


def test_writer(kafka_brokers, kafka_topic, postgres_uri, cli_runner, messages):
    """Should read from Kafka and write to Postgres in an integration test."""
    with mock.patch.object(writer, "RUNNING", VariableBoolean(True, True, False)):
        result = cli_runner.invoke(
            writer.writer,
            [
                "--brokers",
                ",".join(kafka_brokers),
                "--topic",
                kafka_topic,
                "--db-uri",
                postgres_uri,
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0

    with contextlib.closing(
        psycopg2.connect(postgres_uri, cursor_factory=psycopg2.extras.DictCursor)
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM checks;")
            rows = cursor.fetchall()

    assert len(rows) == 3

    assert rows[0]["url"] == "https://www.google.com"
    assert rows[0]["response_time"] == 0.03
    assert rows[0]["status_code"] == 200
    assert rows[0]["regexp_match"] == Match.MATCHED.value

    assert rows[1]["url"] == "https://www.google.fi"
    assert rows[1]["response_time"] == 0.1
    assert rows[1]["status_code"] == 200
    assert rows[1]["regexp_match"] == Match.UNUSED.value

    assert rows[2]["url"] == "https://www.google.com"
    assert rows[2]["response_time"] == 15.0
    assert rows[2]["status_code"] == 404
    assert rows[2]["regexp_match"] == Match.UNMATCHED.value


def test_duplicate_messages(postgres_uri):
    """Should deduplicate messages with the same request_id."""
    duplicate_id = uuid.uuid4()

    check_1 = Check(
        request_id=duplicate_id,
        url="https://www.google.com",
        response_time=0.03,
        status_code=200,
        match=Match.MATCHED,
    )
    check_2 = Check(
        request_id=duplicate_id,
        url="https://www.google.fi",
        response_time=0.1,
        status_code=200,
        match=Match.UNUSED,
    )

    with contextlib.closing(psycopg2.connect(postgres_uri)) as conn:
        writer.ensure_db_schema(conn)
        writer.store_checks(conn, [check_1, check_1])

        with conn.cursor() as cursor:
            cursor.execute("SELECT request_id, url FROM checks")
            rows = cursor.fetchall()
            assert rows == [(check_1.request_id, check_1.url)]

        writer.store_checks(conn, [check_2])

        with conn.cursor() as cursor:
            cursor.execute("SELECT request_id, url FROM checks")
            rows = cursor.fetchall()
            assert rows == [(check_1.request_id, check_1.url)]


def test_invalid_messages():
    """Should ignore messages containing invalid or unparseable data."""
    class ConsumerRecord:
        value: str = '{"request_id": "asd'  # Incomplete JSON

    checks = writer.parse_topic_records({"checks": [ConsumerRecord()]})

    assert checks == []


def test_ensure_db_schema(postgres_uri):
    """Should not raise an error if we attempt to create indexes twice."""
    with contextlib.closing(psycopg2.connect(postgres_uri)) as conn:
        writer.ensure_db_schema(conn)
        writer.ensure_db_schema(conn)
