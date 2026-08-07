from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Float
)

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class RawAPIEvent(Base):
    __tablename__ = "raw_api_events"
    __table_args__ = {"schema": "raw"}

    id = Column(Integer, primary_key=True)

    source_system = Column(String(50), nullable=False)

    symbol = Column(String(20))

    raw_json = Column(Text, nullable=False)

    extracted_at = Column(
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

    loaded_at = Column(
        DateTime,
        server_default=func.now()
    )