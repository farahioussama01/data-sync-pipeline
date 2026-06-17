from prefect import task, get_run_logger
from models.schemas import RawRecord, SourceType
from db.database import SessionLocal
from db.models import WebhookEvent
from datetime import datetime


@task(name="extract-webhook")
def extract_from_webhook() -> list[RawRecord]:
    logger = get_run_logger()
    db = SessionLocal()

    try:
        events = (
            db.query(WebhookEvent)
            .filter(WebhookEvent.status == "pending")
            .all()
        )
        logger.info(f"Found {len(events)} pending webhook events")

        records = []
        for event in events:
            records.append(
                RawRecord(
                    source_type=SourceType.WEBHOOK,
                    source_name=event.source_name,
                    raw_data=event.payload,
                    extracted_at=event.received_at,
                )
            )
            event.status = "processing"

        db.commit()
        return records
    finally:
        db.close()


@task(name="mark-webhook-processed")
def mark_webhook_events_processed(source_name: str) -> None:
    db = SessionLocal()
    try:
        db.query(WebhookEvent).filter(
            WebhookEvent.source_name == source_name,
            WebhookEvent.status == "processing",
        ).update(
            {"status": "processed", "processed_at": datetime.utcnow()},
            synchronize_session=False,
        )
        db.commit()
    finally:
        db.close()
