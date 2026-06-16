"""Helpers de conexao e transacao do DuckDB."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb


def connect(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Abre uma conexao com o DuckDB. Por padrao usa um banco em memoria (usado nos testes)."""
    return duckdb.connect(database)


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Executa um bloco dentro de uma unica transacao.

    Da commit se tudo der certo; faz rollback e relanca a excecao em caso de erro. E isso
    que torna o replace de particao (DELETE + INSERT) idempotente e seguro contra falhas: se
    quebrar no meio da escrita, a tabela de destino fica intacta em vez de meio apagada.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        yield
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
