
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "sample-data"
OUTPUT_DIR = ROOT / "docs" / "dicionario-de-dados-origem"

SOURCES = {
    "transactions_batch_1": {
        "file": "transactions_batch_1.parquet",
        "format": "parquet",
        "sql": "DESCRIBE SELECT * FROM read_parquet(?)",
    },
    "transactions_batch_2": {
        "file": "transactions_batch_2.parquet",
        "format": "parquet",
        "sql": "DESCRIBE SELECT * FROM read_parquet(?)",
    },
    "reconciliation_runs": {
        "file": "reconciliation_runs.parquet",
        "format": "parquet",
        "sql": "DESCRIBE SELECT * FROM read_parquet(?)",
    },
    "reconciliation_results": {
        "file": "reconciliation_results.parquet",
        "format": "parquet",
        "sql": "DESCRIBE SELECT * FROM read_parquet(?)",
    },
    "enterprise_company": {
        "file": "enterprise_company.parquet",
        "format": "parquet",
        "sql": "DESCRIBE SELECT * FROM read_parquet(?)",
    },
    "settlement_paysettler": {
        "file": "settlement_paysettler.csv",
        "format": "csv",
        "sql": "DESCRIBE SELECT * FROM read_csv_auto(?, header = true)",
    },
}


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    try:
        for name, source in SOURCES.items():
            file_path = DATA_DIR / source["file"]

            schema = con.execute(
                source["sql"], [str(file_path)]
            ).fetchall()

            document = {
                "source_name": name,
                "file": f"sample-data/{source['file']}",
                "format": source["format"],
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "columns": [
                    {"name": row[0], "type": row[1]}
                    for row in schema
                ],
            }

            output_path = OUTPUT_DIR / f"{name}.json"
            output_path.write_text(
                json.dumps(document, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )

            print(f"Schema documentado: {output_path.relative_to(ROOT)}")
            for column in document["columns"]:
                print(f"  {column['name']}: {column['type']}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
