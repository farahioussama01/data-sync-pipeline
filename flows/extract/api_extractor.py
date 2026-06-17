import httpx
from prefect import task, get_run_logger
from models.schemas import RawRecord, SourceType
from datetime import datetime


@task(name="extract-api", retries=3, retry_delay_seconds=10)
def extract_from_api(url: str, source_name: str) -> list[RawRecord]:
    logger = get_run_logger()
    logger.info(f"Extracting from API: {url}")

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict):
        data = [data]

    records = [
        RawRecord(
            source_type=SourceType.API,
            source_name=source_name,
            raw_data=item,
            extracted_at=datetime.utcnow(),
        )
        for item in data
        if isinstance(item, dict)
    ]

    logger.info(f"Extracted {len(records)} records from {source_name}")
    return records
