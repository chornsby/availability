import contextlib
import logging
from typing import Any, Dict, List

import click
import kafka
import psycopg2
import psycopg2.errors
import psycopg2.extras
import pydantic

from .common import Check, value_deserializer

logger = logging.getLogger(__name__)
psycopg2.extras.register_uuid()

# TODO: Consider table partitioning
# TODO: Configure logging

RUNNING = True


@click.command()
@click.option(
    "--period",
    default=1.0,
    type=click.FloatRange(min=0),
    help="How many seconds to wait when polling for batches of messages",
)
@click.option(
    "--brokers",
    default="localhost:9092",
    envvar="KAFKA_BROKERS",
    help="A comma separated list of Kafka servers",
)
@click.option(
    "--topic",
    default="checks",
    envvar="KAFKA_TOPIC",
    help="The Kafka topic to consume messages on",
)
@click.option(
    "--group-id",
    default="checks-writer",
    envvar="KAFKA_GROUP_ID",
    help="The Kafka group_id for auto resuming after crashes",
)
@click.option(
    "--db-uri",
    default="localhost:5432",
    envvar="POSTGRES_URI",
    help="A postgresql database uri to connect to",
)
def writer(period: float, brokers: str, topic: str, group_id: str, db_uri: str):
    """Store website availability check results in a Postgres database.

    This component reads check results from a Kafka topic and persists the data
    to the database.
    """
    logging.basicConfig(level=logging.INFO)

    period_ms = period * 1000
    brokers = brokers.split(",")

    logger.info("Connecting to database %s", db_uri)

    with contextlib.closing(psycopg2.connect(db_uri)) as conn:
        logger.info("Creating database table if missing")

        ensure_db_schema(conn)

        consumer = kafka.KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            value_deserializer=value_deserializer,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        with contextlib.closing(consumer) as consumer:
            logger.info("Waiting for messages")

            while RUNNING:
                topic_to_records = consumer.poll(timeout_ms=period_ms, max_records=100)
                checks = parse_topic_records(topic_to_records)

                if not checks:
                    continue

                store_checks(conn, checks)

                # For at-least-once delivery we commit the offset only after
                # persisting the check data
                consumer.commit_async()


def parse_topic_records(topic_to_records: Dict[Any, List[Any]]) -> List[Check]:
    """Parse messages from the Kafka response and validate the incoming data."""
    checks = []

    for messages in topic_to_records.values():
        for message in messages:
            try:
                check = Check.parse_raw(message.value)
            except pydantic.ValidationError:
                logger.error("Unable to deserialize message %s", message)
            else:
                checks.append(check)

    return checks


def ensure_db_schema(conn):
    """Create the necessary database tables and indexes if missing."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS checks (
                request_id uuid UNIQUE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                url VARCHAR NOT NULL,
                response_time FLOAT NOT NULL,
                status_code INT NOT NULL,
                regexp_match VARCHAR NOT NULL
            );
            """.strip()
        )

        # Try to create indexes if they do not already exist
        try:
            cursor.execute("CREATE INDEX url_timestamp ON checks (url, timestamp)")
        except psycopg2.ProgrammingError:
            logger.debug("Index already existed")

        conn.commit()


def store_checks(conn, checks: List[Check]):
    """Insert the parsed checks to Postgres."""
    with conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            """
            INSERT INTO checks (
                request_id,
                timestamp,
                url,
                response_time,
                status_code,
                regexp_match
            )
            VALUES %s
            ON CONFLICT DO NOTHING;
            """,
            [
                (
                    check.request_id,
                    check.timestamp,
                    check.url,
                    check.response_time,
                    check.status_code,
                    check.match.value,
                )
                for check in checks
            ],
        )
        conn.commit()


if __name__ == "__main__":
    writer()
