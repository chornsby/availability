from __future__ import annotations

import datetime
import enum
import uuid
from typing import Optional, Pattern

import pydantic
from pydantic import Field, HttpUrl


class Target(pydantic.BaseModel):
    """A website to monitor and optionally an associated regexp."""

    url: HttpUrl
    pattern: Optional[Pattern]

    @classmethod
    def from_tsv(cls, row: str) -> Target:
        row = row.strip()

        if "\t" in row:
            url, pattern = row.split("\t", 1)
        else:
            url, pattern = row, None

        return Target(url=url, pattern=pattern)


class Match(enum.Enum):
    """Whether a website matched the regexp or UNUSED if none provided."""

    MATCHED = "matched"
    UNMATCHED = "unmatched"
    UNUSED = "unused"


class Check(pydantic.BaseModel):
    """A single completed monitoring check.

    This class defines the schema for the data stored as Kafka message values.
    """

    request_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    url: HttpUrl
    response_time: float
    status_code: int
    match: Match


def value_serializer(message: str) -> bytes:
    return message.encode("utf-8")


def value_deserializer(message: bytes) -> str:
    return message.decode("utf-8")
