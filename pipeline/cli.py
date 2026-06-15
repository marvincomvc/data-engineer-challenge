"""Command-line entrypoint: `python -m pipeline.cli run|status`."""
from __future__ import annotations

import argparse
import os

from pipeline import control, db, runner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="process a reference date (+ 7-day window)")
    run_p.add_argument("--reference-date", required=True, help="YYYY-MM-DD")
    run_p.add_argument("--full", action="store_true", help="reprocess every completed run")
    run_p.add_argument("--db", default="warehouse.duckdb")
    run_p.add_argument("--data", default=os.environ.get("DATA_PATH", "docs/sample-data"))

    status_p = sub.add_parser("status", help="show recent pipeline runs")
    status_p.add_argument("--db", default="warehouse.duckdb")

    args = parser.parse_args(argv)
    con = db.connect(args.db)
    try:
        if args.cmd == "run":
            runner.run(con, args.reference_date, args.data, args.full)
            print(f"[pipeline] run for {args.reference_date} finished. Recent runs:")
        else:
            control.ensure_control_table(con)
        for row in control.get_status(con)[:10]:
            print("  ", row)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
