import itertools
import pathlib
from unittest import mock

import kafka
import pytest
import requests
import requests_mock

from availability import monitor
from availability.common import Check, Match, value_deserializer
from tests.utils import VariableBoolean


@pytest.fixture()
def tsv_path(tmp_path) -> pathlib.Path:
    path = tmp_path / "urls.tsv"
    path.write_text("https://www.google.com\tI'm Feeling Lucky\nhttps://www.google.fi")
    return path


@pytest.fixture()
def requests_mocker():
    with requests_mock.Mocker() as mocker:
        mocker.register_uri(
            "GET",
            "https://www.google.com",
            [
                {"text": "Google Homepage: I'm Feeling Lucky", "status_code": 200},
                {"text": "Error: 404", "status_code": 404},
            ],
        )
        mocker.register_uri(
            "GET",
            "https://www.google.fi",
            [
                {"text": "Google Homepage: Minä olen onnekas", "status_code": 200},
                {"exc": requests.exceptions.ConnectTimeout},
            ],
        )
        yield mocker


def test_monitor(kafka_brokers, kafka_topic, cli_runner, tsv_path, requests_mocker):
    """Should make mocked web requests and write to Kafka in an integration test."""
    with mock.patch.object(monitor, "RUNNING", VariableBoolean(True, True, False)):
        result = cli_runner.invoke(
            monitor.monitor,
            [
                str(tsv_path),
                "--period",
                "0",
                "--brokers",
                ",".join(kafka_brokers),
                "--topic",
                kafka_topic,
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0

    consumer = kafka.KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_brokers,
        value_deserializer=value_deserializer,
        auto_offset_reset="earliest",
    )

    messages = list(itertools.islice(consumer, 3))
    checks = [Check.parse_raw(message.value) for message in messages]
    checks = sorted(checks, key=lambda check: check.url)

    assert checks[0].url == "https://www.google.com"
    assert checks[0].status_code == 200
    assert checks[0].match == Match.MATCHED

    assert checks[1].url == "https://www.google.com"
    assert checks[1].status_code == 404
    assert checks[1].match == Match.UNMATCHED

    assert checks[2].url == "https://www.google.fi"
    assert checks[2].status_code == 200
    assert checks[2].match == Match.UNUSED
