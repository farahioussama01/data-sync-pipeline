import pandas as pd
from pathlib import Path
from prefect import task, get_run_logger
from models.schemas import RawRecord, SourceType
from datetime import datetime


@task(name="extract-csv")
def extract_from_csv(file_path: str, source_name: str) -> list[RawRecord]:
    logger = get_run_logger()
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    logger.info(f"Extracting from CSV: {file_path}")
    df = pd.read_csv(path)
    df = df.where(pd.notnull(df), None)  # replace NaN with None for JSON safety

    records = [
        RawRecord(
            source_type=SourceType.CSV,
            source_name=source_name,
            raw_data=row,
            extracted_at=datetime.utcnow(),
        )
        for row in df.to_dict(orient="records")
    ]

    logger.info(f"Extracted {len(records)} records from {file_path}")
    return records
