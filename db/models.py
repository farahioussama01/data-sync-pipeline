from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, JSON, UniqueConstraint,
)
from sqlalchemy.sql import func
from db.database import Base


class SyncedRecord(Base):
    __tablename__ = "synced_records"
    __table_args__ = (
        UniqueConstraint("source_name", "external_id", name="uq_source_external_id"),
    )

    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(String(50), nullable=False)
    source_name = Column(String(100), nullable=False, index=True)
    external_id = Column(String(255), nullable=True, index=True)
    title = Column(Text, nullable=True)
    body = Column(Text, nullable=True)
    author = Column(String(255), nullable=True)
    category = Column(String(100), nullable=True)
    value = Column(Float, nullable=True)
    tags = Column(JSON, default=list)
    extra = Column(JSON, default=dict)
    source_created_at = Column(DateTime(timezone=True), nullable=True)
    synced_at = Column(DateTime(timezone=True), server_default=func.now())


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(String(50), nullable=False)
    source_name = Column(String(100), nullable=False, index=True)
    status = Column(String(50), default="running")  # running | success | error
    records_extracted = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String(100), nullable=False, default="webhook")
    payload = Column(JSON, nullable=False)
    status = Column(String(50), default="pending")  # pending | processing | processed | error
    received_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True), nullable=True)
