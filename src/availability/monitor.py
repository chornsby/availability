import concurrent.futures
import contextlib
import datetime
import logging
import time
import uuid
from typing import List, Optional, Pattern, TextIO

import click
import kafka
import requests
from pydantic import HttpUrl

from .common import Check, Match, Target, value_serializer

logger = logging.getLogger(__name__)


RUNNING = True


@click.command()
@click.argument("config", type=click.File("r"))
@click.option(
    "--period",
    default=60.0,
    type=click.FloatRange(min=0),
    help="How many seconds to wait between checks.",
)
@click.option(
    "--timeout",
    default=15,
    type=click.FloatRange(min=0),
    help="How long to wait before abandoning an HTTP request.",
)
@click.option(
    "--brokers",
    default="localhost:9092",
    envvar="KAFKA_BROKERS",
    help="Comma separated list of Kafka servers.",
)
@click.option(
    "--security-protocol",
    default="PLAINTEXT",
    envvar="KAFKA_SECURITY_PROTOCOL",
    type=click.Choice(["PLAINTEXT", "SSL"]),
    help="Kafka security protocol.",
)
@click.option(
    "--ssl-cafile",
    default=None,
    envvar="KAFKA_SSL_CAFILE",
    type=click.Path(exists=True, dir_okay=False),
    help="CA file for certificate verification.",
)
@click.option(
    "--ssl-certfile",
    default=None,
    envvar="KAFKA_SSL_CERTFILE",
    type=click.Path(exists=True, dir_okay=False),
    help="PEM format client certificate.",
)
@click.option(
    "--ssl-keyfile",
    default=None,
    envvar="KAFKA_SSL_KEYFILE",
    type=click.Path(exists=True, dir_okay=False),
    help="Client private key.",
)
@click.option(
    "--topic",
    default="checks",
    envvar="KAFKA_TOPIC",
    help="Kafka topic to produce messages on.",
)
def monitor(
    config: TextIO,
    period: float,
    timeout: float,
    brokers: str,
    security_protocol: str,
    ssl_cafile: Optional[str],
    ssl_certfile: Optional[str],
    ssl_keyfile: Optional[str],
    topic: str,
):
    """Periodically check the availability of the given list of websites.

    This component periodically makes requests to different websites and sends
    the check results to a Kafka topic for further processing.
    """
    logging.basicConfig(level=logging.INFO)

    targets = [Target.from_tsv(row) for row in config.readlines()]
    brokers = brokers.split(",")

    logger.info("Connecting to brokers %s", brokers)

    producer = kafka.KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=value_serializer,
        security_protocol=security_protocol,
        ssl_cafile=ssl_cafile,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
    )

    with contextlib.closing(producer):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            while RUNNING:
                logger.info("Starting monitoring round")
                start = datetime.datetime.utcnow()

                run(targets, timeout, topic, executor, producer)

                next_start = start + datetime.timedelta(seconds=period)
                sleep = next_start - datetime.datetime.utcnow()

                if sleep < datetime.timedelta(seconds=0):
                    logger.warning("Checks took longer than %s seconds to run", period)
                else:
                    time.sleep(sleep.total_seconds())


def run(
    targets: List[Target],
    timeout: float,
    topic: str,
    executor: concurrent.futures.ThreadPoolExecutor,
    producer: kafka.KafkaProducer,
):
    """Make all monitoring requests in a thread pool and send the results to Kafka."""
    future_to_url = {
        executor.submit(check_url, target.url, target.pattern, timeout): target.url
        for target in targets
    }

    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]

        try:
            check = future.result()
        except requests.Timeout:
            logger.warning("Check timed out", extra={"url": url, "timeout": timeout})
        except Exception as exc_info:
            logger.exception("Could not check url", {"url": url}, exc_info=exc_info)
        else:
            future = producer.send(topic, value=check.json())
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)


def check_url(url: HttpUrl, pattern: Optional[Pattern], timeout: float) -> Check:
    """Make the monitoring request and return the Check information."""
    timestamp = datetime.datetime.utcnow()

    response = requests.get(url, timeout=timeout)

    if pattern:
        if pattern.search(response.text):
            match = Match.MATCHED
        else:
            match = Match.UNMATCHED
    else:
        match = Match.UNUSED

    return Check(
        request_id=uuid.uuid4(),
        timestamp=timestamp,
        url=url,
        response_time=response.elapsed.total_seconds(),
        status_code=response.status_code,
        match=match,
    )


def on_send_success(record_metadata):
    logger.debug(
        "Message sending succeeded",
        extra={"topic": record_metadata.topic, "offset": record_metadata.offset},
    )


def on_send_error(exc_info):
    logger.error("Message sending failed", exc_info=exc_info)


if __name__ == "__main__":
    monitor()
