from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime
from enum import Enum


class SourceType(str, Enum):
    API = "api"
    CSV = "csv"
    WEBHOOK = "webhook"


class RawRecord(BaseModel):
    source_type: SourceType
    source_name: str
    raw_data: dict[str, Any]
    extracted_at: datetime = Field(default_factory=datetime.utcnow)


class NormalizedRecord(BaseModel):
    source_type: SourceType
    source_name: str
    external_id: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    value: Optional[float] = None
    tags: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
    source_created_at: Optional[datetime] = None
    synced_at: datetime = Field(default_factory=datetime.utcnow)


class WebhookPayload(BaseModel):
    data: dict[str, Any]
    source_name: str = "webhook"
