
from pathlib import Path
import csv
import hashlib
import json

import duckdb


RAIZ = Path("/app")
CAMINHO_BANCO = RAIZ / "data" / "analytics.duckdb"
DIRETORIO_CSV = RAIZ / "data" / "dataset_teste" / "paysettler"

TABELA_DESTINO = "bronze_paysettler_arquivo"

COLUNAS = [
    "transaction_id",
    "merchant_id",
    "amount",
    "currency",
    "settled_at",
    "processor_reference",
    "status",
]


def hash_arquivo(caminho: Path) -> str:
    calculador = hashlib.sha256()

    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            calculador.update(bloco)

    return calculador.hexdigest()


def hash_registro(registro: dict) -> str:
    conteudo = json.dumps(
        [registro.get(coluna) for coluna in COLUNAS],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(conteudo.encode("utf-8")).hexdigest()


def criar_tabela(con):
    definicao_colunas = ",\n".join(
        f'"{coluna}" VARCHAR' for coluna in COLUNAS
    )

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABELA_DESTINO} (
            {definicao_colunas},
            row_hash VARCHAR,
            ingested_at TIMESTAMPTZ,
            arquivo_origem VARCHAR,
            hash_arquivo VARCHAR
        )
        """
    )


def ingerir_arquivo(con, caminho: Path) -> int:
    hash_do_arquivo = hash_arquivo(caminho)
    registros = []

    with caminho.open("r", encoding="utf-8-sig", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)

        if leitor.fieldnames is None:
            raise ValueError(f"Arquivo sem cabeçalho: {caminho.name}")

        leitor.fieldnames = [
            coluna.strip() for coluna in leitor.fieldnames
        ]

        faltantes = set(COLUNAS) - set(leitor.fieldnames)
        if faltantes:
            raise ValueError(
                f"{caminho.name}: colunas ausentes: {sorted(faltantes)}"
            )

        for numero_linha, linha in enumerate(leitor, start=2):
            if None in linha:
                raise ValueError(
                    f"{caminho.name}, linha {numero_linha}: "
                    "quantidade de campos maior que o cabeçalho."
                )

            registro = {
                coluna: (
                    linha.get(coluna).strip()
                    if linha.get(coluna) is not None
                    else None
                )
                for coluna in COLUNAS
            }

            registro["row_hash"] = hash_registro(registro)
            registros.append(registro)

    if not registros:
        print(f"[AVISO] {caminho.name}: arquivo sem registros.")
        return 0

    total_antes = con.execute(
        f"SELECT COUNT(*) FROM {TABELA_DESTINO}"
    ).fetchone()[0]

    con.execute("BEGIN TRANSACTION")

    try:
        con.executemany(
            f"""
            INSERT INTO {TABELA_DESTINO} (
                {", ".join(f'"{c}"' for c in COLUNAS)},
                row_hash,
                ingested_at,
                arquivo_origem,
                hash_arquivo
            )
            SELECT
                {", ".join("?" for _ in COLUNAS)},
                ?, CURRENT_TIMESTAMP, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM {TABELA_DESTINO} existente
                WHERE existente.row_hash = ?
            )
            """,
            [
                [
                    *[registro[c] for c in COLUNAS],
                    registro["row_hash"],
                    caminho.name,
                    hash_do_arquivo,
                    registro["row_hash"],
                ]
                for registro in registros
            ],
        )

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise

    total_depois = con.execute(
        f"SELECT COUNT(*) FROM {TABELA_DESTINO}"
    ).fetchone()[0]

    return total_depois - total_antes


def main():
    if not DIRETORIO_CSV.exists():
        raise FileNotFoundError(
            f"Diretório não encontrado: {DIRETORIO_CSV}"
        )

    arquivos = sorted(DIRETORIO_CSV.glob("*.csv"))

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum CSV encontrado em {DIRETORIO_CSV}"
        )

    CAMINHO_BANCO.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(CAMINHO_BANCO))

    try:
        criar_tabela(con)

        total_inseridos = 0

        for caminho in arquivos:
            inseridos = ingerir_arquivo(con, caminho)
            total_inseridos += inseridos

            print(
                f"[OK] {caminho.name}: "
                f"{inseridos} registros inseridos"
            )

        total_bronze = con.execute(
            f"SELECT COUNT(*) FROM {TABELA_DESTINO}"
        ).fetchone()[0]

        total_arquivos = con.execute(
            f"""
            SELECT COUNT(DISTINCT arquivo_origem)
            FROM {TABELA_DESTINO}
            """
        ).fetchone()[0]

        print(f"\n[OK] Arquivos encontrados: {len(arquivos)}")
        print(f"[OK] Registros inseridos nesta execução: {total_inseridos}")
        print(f"[OK] Total de registros na Bronze: {total_bronze}")
        print(f"[OK] Arquivos representados na Bronze: {total_arquivos}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
