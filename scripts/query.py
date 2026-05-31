"""Run a SQL query against the warehouse and print results as a formatted table."""
import sys
import duckdb

MAX_COL_WIDTH = 50


def truncate(s: str, max_w: int = MAX_COL_WIDTH) -> str:
    if len(s) <= max_w:
        return s
    return s[: max_w - 1] + "…"


def main() -> None:
    sql = sys.stdin.read()
    if not sql.strip():
        print("No SQL provided via stdin.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect("/data/output/warehouse.duckdb", read_only=True)
    result = con.execute(sql)
    cols = [d[0] for d in result.description]
    rows = [cols] + [list(r) for r in result.fetchall()]
    con.close()

    # Convert all values to strings
    str_rows = [[truncate(str(v) if v is not None else "NULL") for v in r] for r in rows]

    # Compute column widths (capped at MAX_COL_WIDTH)
    widths = [min(max(len(r[i]) for r in str_rows), MAX_COL_WIDTH) for i in range(len(cols))]

    # Print formatted table
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    sep = "-+-".join("-" * w for w in widths)

    print(fmt.format(*str_rows[0]))
    print(sep)
    for row in str_rows[1:]:
        print(fmt.format(*row))

    print(f"\n({len(str_rows) - 1} rows)")


if __name__ == "__main__":
    main()
