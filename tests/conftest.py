"""
Fixtures compartilhados para os testes do pipeline de reconciliação.

Fornece:
- Conexão DuckDB em memória
- Dados sintéticos pequenos para testes unitários e de integração
"""

import duckdb
import pytest


@pytest.fixture
def con_memoria():
    """
    Retorna uma conexão DuckDB em memória limpa.

    Cada teste recebe uma conexão isolada.
    """
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def dados_sinteticos():
    """
    Retorna um dicionário com dados sintéticos representando
    as fontes de entrada em escala reduzida.

    Estrutura:
        transacoes: lista de dicionários (transactions)
        results: lista de dicionários (reconciliation_results)
        runs: lista de dicionários (reconciliation_runs)
        merchants: lista de dicionários (enterprise_company)
        paysettler: lista de dicionários (CSV PaySettler)
    """
    return {
        "transacoes": [
            {
                "id": 1,
                "transaction_id": "txn-001",
                "merchant_id": "MERCH_0001",
                "amount": 150.00,
                "currency": "BRL",
                "status": "COMPLETED",
                "description": "Pagamento 1",
                "created_at": "2025-03-10T10:00:00Z",
                "updated_at": "2025-03-10T10:05:00Z",
            },
            {
                "id": 2,
                "transaction_id": "txn-002",
                "merchant_id": "MERCH_0002",
                "amount": 200.00,
                "currency": "BRL",
                "status": "COMPLETED",
                "description": "Pagamento 2",
                "created_at": "2025-03-12T12:00:00Z",
                "updated_at": "2025-03-12T12:05:00Z",
            },
            {
                "id": 3,
                "transaction_id": "txn-003",
                "merchant_id": "MERCH_0001",
                "amount": 75.50,
                "currency": "BRL",
                "status": "PENDING",
                "description": "Pagamento 3",
                "created_at": "2025-03-14T09:00:00Z",
                "updated_at": "2025-03-14T09:05:00Z",
            },
            {
                "id": 4,
                "transaction_id": "txn-004",
                "merchant_id": "MERCH_0003",
                "amount": 500.00,
                "currency": "BRL",
                "status": "COMPLETED",
                "description": "Pagamento 4",
                "created_at": "2025-03-16T00:00:01Z",  # Fora da janela para ref 2025-03-15
                "updated_at": "2025-03-16T00:05:00Z",
            },
            {
                "id": 5,
                "transaction_id": "txn-005",
                "merchant_id": "MERCH_0002",
                "amount": 300.00,
                "currency": "BRL",
                "status": "COMPLETED",
                "description": "Pagamento 5",
                "created_at": "2025-03-08T08:00:00Z",  # Dentro da janela (7 dias antes de 03-15)
                "updated_at": "2025-03-08T08:05:00Z",
            },
        ],
        "results": [
            {
                "id": 1,
                "run_id": 1,
                "transaction_id": "txn-001",
                "merchant_id": "MERCH_0001",
                "category": "MATCHED",
                "internal_amount": 150.00,
                "processor_amount": 150.00,
                "difference": 0.0,
                "created_at": "2025-03-10T10:30:00Z",
                "Op": "I",
                "_timestamp": "2025-03-10T10:30:00Z",
            },
            {
                "id": 2,
                "run_id": 1,
                "transaction_id": "txn-002",
                "merchant_id": "MERCH_0002",
                "category": "MISMATCHED",
                "internal_amount": 200.00,
                "processor_amount": 200.01,  # Diferença de 0.01 → tolerância deve corrigir
                "difference": 0.01,
                "created_at": "2025-03-12T12:30:00Z",
                "Op": "I",
                "_timestamp": "2025-03-12T12:30:00Z",
            },
            {
                "id": 3,
                "run_id": 1,
                "transaction_id": "txn-003",
                "merchant_id": "MERCH_0001",
                "category": "UNRECONCILED_INTERNAL",
                "internal_amount": 75.50,
                "processor_amount": None,
                "difference": None,
                "created_at": "2025-03-14T09:30:00Z",
                "Op": "I",
                "_timestamp": "2025-03-14T09:30:00Z",
            },
        ],
        "runs": [
            {
                "id": 1,
                "reference_date": "2025-03-15",
                "file_name": "settlement_2025-03-15.csv",
                "status": "COMPLETED",
                "total_transactions": 3,
                "started_at": "2025-03-15T02:00:00Z",
                "completed_at": "2025-03-15T02:30:00Z",
                "created_at": "2025-03-15T02:00:00Z",
                "Op": "U",
                "_timestamp": "2025-03-15T02:30:00Z",
            },
        ],
        "merchants": [
            {
                "id": 1,
                "merchant_id": "MERCH_0001",
                "legal_name": "Loja Um LTDA",
                "trade_name": "Loja 1",
                "document": "12345678000101",
                "primary_cnae": "4712100",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-06-01T00:00:00Z",
                "Op": "I",
                "_timestamp": "2024-01-01T00:00:00Z",
            },
            {
                "id": 2,
                "merchant_id": "MERCH_0002",
                "legal_name": "Loja Dois LTDA",
                "trade_name": "Loja 2",
                "document": "98765432000102",
                "primary_cnae": "5611203",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-06-01T00:00:00Z",
                "Op": "I",
                "_timestamp": "2024-01-01T00:00:00Z",
            },
        ],
        "paysettler": [
            {
                "transaction_id": "txn-001",
                "merchant_id": "MERCH_0001",
                "amount": 150.00,
                "currency": "BRL",
                "settled_at": "2025-03-10T14:00:00Z",
                "processor_reference": "PS-001",
                "status": "SETTLED",
            },
            {
                "transaction_id": "txn-002",
                "merchant_id": "MERCH_0002",
                "amount": 200.01,
                "currency": "BRL",
                "settled_at": "2025-03-12T14:00:00Z",
                "processor_reference": "PS-002",
                "status": "SETTLED",
            },
            {
                "transaction_id": "txn-999",
                "merchant_id": "MERCH_0001",
                "amount": 50.00,
                "currency": "BRL",
                "settled_at": "2025-03-14T18:00:00Z",
                "processor_reference": "PS-999",
                "status": "SETTLED",
            },
        ],
    }