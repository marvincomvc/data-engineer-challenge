import shutil
from pathlib import Path

from app.config import settings

DOMAINS = {
    "transactions_batch_1": "transactions_batch_1.parquet",
    "transactions_batch_2": "transactions_batch_2.parquet",
    "reconciliation_runs": "reconciliation_runs.parquet",
    "reconciliation_results": "reconciliation_results.parquet",
    "enterprise_company": "enterprise_company.parquet",
}


PROCESSOR_FILES = {
    "settlement_paysettler": "settlement_paysettler.csv",
}


def copy_raw_files():

    source_path = Path(settings.RAW_SOURCE_PATH)

    raw_path = Path(settings.DATA_PATH) / "raw"

    raw_path.mkdir(parents=True, exist_ok=True)

    for file_name in list(DOMAINS.values()) + list(PROCESSOR_FILES.values()):
        source_file = source_path / file_name

        target_file = raw_path / file_name

        if not target_file.exists():
            shutil.copy(source_file, target_file)
