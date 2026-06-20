"""
Configuração do pipeline de reconciliação.

Centraliza constantes, caminhos e parâmetros de configuração
para todos os módulos do pipeline.
"""

from pathlib import Path

# ---- Caminhos padrão ----
DEFAULT_DATA_PATH = Path("docs/sample-data")
DEFAULT_OUTPUT_PATH = Path("data/analytics.db")
DEFAULT_BRONZE_PATH = Path("bronze")
DEFAULT_LOG_LEVEL = "INFO"

# ---- Nomes de arquivos de entrada ----
FILE_TRANSACTIONS_BATCH_1 = "transactions_batch_1.parquet"
FILE_TRANSACTIONS_BATCH_2 = "transactions_batch_2.parquet"
FILE_RECONCILIATION_RUNS = "reconciliation_runs.parquet"
FILE_RECONCILIATION_RESULTS = "reconciliation_results.parquet"
FILE_ENTERPRISE_COMPANY = "enterprise_company.parquet"
FILE_PAYSETTLER_CSV = "settlement_paysettler.csv"

# ---- Colunas esperadas em cada fonte (validação de schema) ----
EXPECTED_COLUMNS_TRANSACTIONS = {
    "id", "transaction_id", "merchant_id", "amount", "currency",
    "status", "description", "created_at", "updated_at",
}

EXPECTED_COLUMNS_RECONCILIATION_RUNS = {
    "id", "reference_date", "file_name", "status",
    "total_transactions", "started_at", "completed_at", "created_at",
}

EXPECTED_COLUMNS_RECONCILIATION_RESULTS = {
    "id", "run_id", "transaction_id", "merchant_id", "category",
    "internal_amount", "processor_amount", "difference", "created_at",
}

EXPECTED_COLUMNS_ENTERPRISE_COMPANY = {
    "id", "merchant_id", "legal_name", "trade_name", "document",
    "primary_cnae", "created_at", "updated_at",
}

EXPECTED_COLUMNS_PAYSETTLER = {
    "transaction_id", "merchant_id", "amount", "currency",
    "settled_at", "processor_reference", "status",
}

# ---- Constantes de negócio ----
TOLERANCIA_REAIS = 0.01        # Tolerância de R$ 0,01 para matching
JANELA_DIAS = 7                # Janela de 7 dias para transações internas

# ---- Categorias de reconciliação ----
CATEGORIA_MATCHED = "MATCHED"
CATEGORIA_MISMATCHED = "MISMATCHED"
CATEGORIA_UNRECONCILED_PROCESSOR = "UNRECONCILED_PROCESSOR"
CATEGORIA_UNRECONCILED_INTERNAL = "UNRECONCILED_INTERNAL"

CATEGORIAS_FIXAS = [
    CATEGORIA_MATCHED,
    CATEGORIA_MISMATCHED,
    CATEGORIA_UNRECONCILED_PROCESSOR,
    CATEGORIA_UNRECONCILED_INTERNAL,
]

# ---- Descrições das categorias (para dim_category) ----
CATEGORIA_DESCRICOES = {
    CATEGORIA_MATCHED: "Transação casada com sucesso entre interno e processador",
    CATEGORIA_MISMATCHED: "Transação com divergência de valores entre interno e processador",
    CATEGORIA_UNRECONCILED_PROCESSOR: "Transação encontrada no processador mas ausente no sistema interno",
    CATEGORIA_UNRECONCILED_INTERNAL: "Transação encontrada no sistema interno mas ausente no processador",
}

# ---- Tipos de problema de qualidade (para bronze) ----
QUALIDADE_NULL_TRANSACTION_ID = "NULL_TRANSACTION_ID"
QUALIDADE_NULL_MERCHANT_ID = "NULL_MERCHANT_ID"
QUALIDADE_NULL_AMOUNT = "NULL_AMOUNT"
QUALIDADE_DUPLICATA_DESCARTADA = "DUPLICATA_DESCARTADA"
QUALIDADE_TOLERANCIA_APLICADA = "TOLERANCIA_APLICADA"
QUALIDADE_DIFERENCA_RECALCULADA = "DIFERENCA_RECALCULADA"
QUALIDADE_DEDUP_CDC = "DEDUP_CDC_DESCARTA_ANTERIOR"