from datetime import datetime
from typing import Any

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from db.database import get_db, init_db
from db.models import WebhookEvent

app = FastAPI(title="Data Sync Webhook", version="1.0.0")


@app.on_event("startup")
async def startup() -> None:
    init_db()


@app.post("/webhook/{source_name}", status_code=201)
async def receive_event(
    source_name: str,
    payload: dict[str, Any],
    db: Session = Depends(get_db),
):
    """Accept any JSON payload and queue it for the next pipeline run."""
    event = WebhookEvent(
        source_name=source_name,
        payload=payload,
        status="pending",
        received_at=datetime.utcnow(),
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return {"id": event.id, "source_name": source_name, "status": "received"}


@app.get("/webhook/events")
async def list_events(
    status: str = None,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """List received webhook events, optionally filtered by status."""
    q = db.query(WebhookEvent).order_by(WebhookEvent.received_at.desc())
    if status:
        q = q.filter(WebhookEvent.status == status)
    return q.limit(limit).all()


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
