"""Unit tests for CDC deduplication SQL helper.

These tests execute the generated SQL against a real (in-process) DuckDB
connection to verify the window-function logic — no file I/O needed.
"""
from __future__ import annotations

from pipeline.transform.cdc import dedup_latest


def _create_cdc_table(conn, table_name: str) -> None:
    conn.execute(f"""
        CREATE TEMP TABLE {table_name} (
            id        INTEGER,
            name      VARCHAR,
            op        VARCHAR,
            _timestamp VARCHAR
        )
    """)


class TestDedupLatest:
    def test_keeps_latest_row_for_key(self, tmp_duckdb):
        """When a key has I then U, only the U (later) row survives."""
        _create_cdc_table(tmp_duckdb, "cdc_basic")
        tmp_duckdb.execute("""
            INSERT INTO cdc_basic VALUES
                (1, 'initial',  'I', '2025-03-04T10:00:00'),
                (1, 'updated',  'U', '2025-03-04T11:00:00')
        """)
        sql = dedup_latest("cdc_basic", "id")
        rows = tmp_duckdb.execute(f"SELECT name FROM ({sql})").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "updated"

    def test_multiple_keys_one_row_each(self, tmp_duckdb):
        """Each unique key gets exactly one row — its latest state."""
        _create_cdc_table(tmp_duckdb, "cdc_multi")
        tmp_duckdb.execute("""
            INSERT INTO cdc_multi VALUES
                (1, 'a_v1', 'I', '2025-03-04T09:00:00'),
                (1, 'a_v2', 'U', '2025-03-04T10:00:00'),
                (2, 'b_v1', 'I', '2025-03-04T09:30:00'),
                (2, 'b_v2', 'U', '2025-03-04T10:30:00')
        """)
        sql = dedup_latest("cdc_multi", "id")
        rows = tmp_duckdb.execute(f"SELECT id, name FROM ({sql}) ORDER BY id").fetchall()
        assert rows == [(1, "a_v2"), (2, "b_v2")]

    def test_single_row_passes_through(self, tmp_duckdb):
        """A table with one row per key is unchanged by dedup_latest."""
        _create_cdc_table(tmp_duckdb, "cdc_single")
        tmp_duckdb.execute("INSERT INTO cdc_single VALUES (42, 'only', 'I', '2025-03-04T10:00:00')")
        sql = dedup_latest("cdc_single", "id")
        rows = tmp_duckdb.execute(f"SELECT id FROM ({sql})").fetchall()
        assert rows == [(42,)]

    def test_op_d_is_preserved(self, tmp_duckdb):
        """dedup_latest keeps Op='D' rows — filtering is the silver layer's job."""
        _create_cdc_table(tmp_duckdb, "cdc_delete")
        tmp_duckdb.execute("""
            INSERT INTO cdc_delete VALUES
                (1, 'alive',   'I', '2025-03-04T10:00:00'),
                (1, 'deleted', 'D', '2025-03-04T11:00:00')
        """)
        sql = dedup_latest("cdc_delete", "id")
        rows = tmp_duckdb.execute(f"SELECT op FROM ({sql})").fetchall()
        assert rows == [("D",)]

    def test_rn_column_excluded_from_output(self, tmp_duckdb):
        """The internal _rn column must not appear in the result set."""
        _create_cdc_table(tmp_duckdb, "cdc_rn")
        tmp_duckdb.execute("INSERT INTO cdc_rn VALUES (1, 'x', 'I', '2025-03-04T10:00:00')")
        sql = dedup_latest("cdc_rn", "id")
        cols = [
            desc[0]
            for desc in tmp_duckdb.execute(f"DESCRIBE ({sql})").fetchall()
        ]
        assert "_rn" not in cols

    def test_empty_table_returns_empty(self, tmp_duckdb):
        """An empty source table produces an empty result — no errors."""
        _create_cdc_table(tmp_duckdb, "cdc_empty")
        sql = dedup_latest("cdc_empty", "id")
        rows = tmp_duckdb.execute(f"SELECT * FROM ({sql})").fetchall()
        assert rows == []

    def test_three_updates_keeps_latest(self, tmp_duckdb):
        """Even with three CDC events, only the single latest survives."""
        _create_cdc_table(tmp_duckdb, "cdc_three")
        tmp_duckdb.execute("""
            INSERT INTO cdc_three VALUES
                (1, 'v1', 'I', '2025-03-04T08:00:00'),
                (1, 'v2', 'U', '2025-03-04T09:00:00'),
                (1, 'v3', 'U', '2025-03-04T10:00:00')
        """)
        sql = dedup_latest("cdc_three", "id")
        rows = tmp_duckdb.execute(f"SELECT name FROM ({sql})").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "v3"
