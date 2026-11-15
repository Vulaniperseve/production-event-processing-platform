from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Float,
    ForeignKey,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()


# ----------------------------------------------------------------------
# Raw processing statuses (lifecycle of one API response)
#
#   PENDING ──> VALIDATED ──> LOADED
#      │            │
#      │            └──> NO_VALID_RECORDS
#      └──> FAILED  (structure invalid / unexpected error)
# ----------------------------------------------------------------------
class RawStatus:
    PENDING = "PENDING"
    VALIDATED = "VALIDATED"
    LOADED = "LOADED"
    NO_VALID_RECORDS = "NO_VALID_RECORDS"
    FAILED = "FAILED"


class RawAPIEvent(Base):
    """
    Raw layer: one row per API response, stored exactly as received.

    Append-only. This is the single source of truth for every downstream
    step, so tasks can be retried or replayed without calling the API again.
    """

    __tablename__ = "raw_api_events"
    __table_args__ = (
        Index("ix_raw_api_events_payload_hash", "payload_hash"),
        Index("ix_raw_api_events_status", "processing_status"),
        {"schema": "raw"},
    )

    id = Column(Integer, primary_key=True)

    source_system = Column(String(50), nullable=False)

    symbol = Column(String(20))

    raw_json = Column(JSONB, nullable=False)

    # SHA-256 of the canonical JSON payload: detects identical re-ingestions
    payload_hash = Column(String(64))

    api_status = Column(String(20))

    record_count = Column(Integer)

    processing_status = Column(
        String(20),
        nullable=False,
        server_default=RawStatus.PENDING,
    )

    error_message = Column(Text)

    extracted_at = Column(
        DateTime,
        server_default=func.now()
    )

    processed_at = Column(DateTime)


class QuarantinedRecord(Base):
    """
    Records that failed record-level validation, linked to their raw source.
    """

    __tablename__ = "quarantined_records"
    __table_args__ = {"schema": "quarantine"}

    id = Column(Integer, primary_key=True)

    raw_event_id = Column(
        Integer,
        ForeignKey("raw.raw_api_events.id"),
        nullable=False,
        index=True,
    )

    symbol = Column(String(20))

    record = Column(JSONB, nullable=False)

    reason = Column(Text, nullable=False)

    quarantined_at = Column(
        DateTime,
        server_default=func.now()
    )


class MarketEvent(Base):
    __tablename__ = "market_events"
    __table_args__ = {"schema": "staging"}

    event_id = Column(String(36), primary_key=True)

    entity_id = Column(String(20), nullable=False)

    event_time = Column(DateTime)

    event_type = Column(String(50))

    value = Column(Float)

    source_system = Column(String(50))

    # Lineage: which raw API response produced this event
    raw_event_id = Column(
        Integer,
        ForeignKey("raw.raw_api_events.id"),
        index=True,
    )

    loaded_at = Column(
        DateTime,
        server_default=func.now()
    )
