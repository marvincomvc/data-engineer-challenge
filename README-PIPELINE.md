# Pipeline de Reconciliação de Liquidações

Solução de engenharia de dados para transformar dados transacionais de reconciliação de liquidações em um modelo analítico dimensional (estrela), pronto para consumo por operações, CFO e compliance.

## Stack

- **Python 3.12+** — funções procedurais puras
- **DuckDB** — processamento SQL, leitura de parquet/CSV e armazenamento
- **Docker + Docker Compose** — ambiente conteinerizado
- **pytest** — testes unitários e de integração
- **logging** padrão — observabilidade sem dependências extras

## Estrutura do Projeto

```
.
├── src/
│   ├── cli.py            # Interface de linha de comando (argparse)
│   ├── config.py         # Constantes, caminhos e esquemas esperados
│   ├── quality.py        # Validação de qualidade + gravação camada bronze
│   ├── transform.py      # Dedup CDC, janela 7 dias, tolerância R$ 0,01
│   ├── model.py          # DDL/DML do modelo dimensional
│   └── pipeline.py       # Orquestrador principal
├── tests/
│   ├── conftest.py            # Fixtures: DuckDB em memória, dados sintéticos
│   ├── test_tolerance.py      # Testes unitários da tolerância R$ 0,01
│   ├── test_date_window.py    # Testes unitários da janela de 7 dias
│   └── test_integration.py    # Integração end-to-end + idempotência
├── docs/sample-data/          # Dados de entrada (parquet + CSV)
├── docker-compose-fred.yml    # Docker Compose do pipeline
├── Dockerfile                 # Build da imagem
├── pyproject.toml             # Dependências: duckdb, pyarrow, pytest
└── README-PIPELINE.md         # Este arquivo
```

## Como Rodar

### Via Docker (recomendado)

```bash
# Build da imagem e execução do pipeline
docker compose -f docker-compose-fred.yml up --build

# Ou para rodar com data específica:
docker compose -f docker-compose-fred.yml run --rm pipeline \
  python -m src.cli --reference-date 2025-03-15

# Executar testes
docker compose -f docker-compose-fred.yml run --rm pipeline \
  pytest tests/ -v
```

### Local (sem Docker)

```bash
# Instalar dependências
pip install -e ".[dev]"

# Executar pipeline
python -m src.cli --reference-date 2025-03-15

# Executar testes
pytest tests/ -v
```

### Parâmetros da CLI

| Parâmetro | Obrigatório | Default | Descrição |
|-----------|-------------|---------|-----------|
| `--reference-date` | Sim | — | Data de referência (YYYY-MM-DD) |
| `--data-path` | Não | `docs/sample-data` | Pasta com os arquivos de entrada |
| `--output-path` | Não | `data/analytics.db` | Caminho do banco DuckDB de saída |
| `--bronze-path` | Não | `bronze` | Pasta para registros de qualidade |
| `--log-level` | Não | `INFO` | DEBUG, INFO, WARNING, ERROR |

## Saídas do Pipeline

### Banco Analítico (`data/analytics.db`)

**Fato:**
- `fct_reconciliation` — cada transação reconciliada, com valores internos, do processador, diferenças recalculadas e categoria final (após tolerância)

**Dimensões:**
- `dim_date` — calendário (year, month, day, quarter, day_of_week, is_weekend)
- `dim_category` — 4 categorias: MATCHED, MISMATCHED, UNRECONCILED_PROCESSOR, UNRECONCILED_INTERNAL
- `dim_merchant` — dados cadastrais dos merchants (SCD Tipo 1)
- `dim_reconciliation_run` — execuções de reconciliação

**Agregações:**
- `agg_daily_reconciliation` — volume diário por merchant e categoria
- `agg_monthly_financial` — volume mensal (visão CFO)

### Camada Bronze (`bronze/`)

Arquivos `.parquet` com registros que sofreram alguma ação de qualidade:
- `*_null_transaction_id.parquet` — registros rejeitados por transaction_id nulo
- `*_null_merchant_id.parquet` — registros com merchant_id nulo
- `*_duplicatas_descartadas.parquet` — registros descartados por duplicação
- `tolerancia_aplicada.parquet` — registros que mudaram de MISMATCHED para MATCHED
- `diferenca_recalculada.parquet` — registros com diferença recalculada

## Fluxo do Pipeline

```
1. LEITURA
   ├── transactions_batch_1.parquet + batch_2.parquet
   ├── reconciliation_results.parquet
   ├── reconciliation_runs.parquet
   ├── enterprise_company.parquet
   └── settlement_paysettler.csv

2. QUALIDADE + BRONZE
   ├── Validação de schemas (crítico: FAIL se inválido)
   ├── Verificação de arquivos vazios (crítico: FAIL)
   ├── Detecção de NULLs → WARN + bronze
   ├── Deduplicação de transaction_id → WARN + bronze
   └── Deduplicação CDC (Op/_timestamp) → bronze

3. TRANSFORMAÇÃO
   ├── União dos batches de transações (UNION ALL)
   ├── Join: reconciliation_results ↔ runs (reference_date)
   ├── Janela de 7 dias: [reference_date - 7, reference_date]
   ├── Recálculo de difference_abs e difference_signed
   └── Tolerância R$ 0,01: MISMATCHED → MATCHED se diff <= 0.01

4. CARGA (Idempotente)
   ├── DELETE da reference_date (fato + agregações)
   ├── UPSERT dim_date, dim_merchant, dim_category, dim_reconciliation_run
   ├── INSERT fct_reconciliation
   └── INSERT agg_daily + agg_monthly
```

## Decisões e Premissas

1. **Janela de 7 dias aplicada via reference_date do run:** A janela filtra os reconciliation_runs cuja `reference_date` está entre `[data_informada - 7, data_informada]`. Isso garante que apenas resultados de runs dentro da janela sejam considerados.

2. **Tolerância corrige MISMATCHED → MATCHED:** Quando a diferença absoluta calculada é <= R$ 0,01, a categoria é sobrescrita para MATCHED independentemente do valor original. Registros MATCHED que já estão corretos não são alterados.

3. **Transações sem correspondência:** UNRECONCILED_PROCESSOR tem `internal_amount = NULL` e `processor_amount` preenchido. UNRECONCILED_INTERNAL tem o inverso. Esses NULLs são preservados na fato.

4. **Deduplicação CDC:** Os dados de exemplo usam padrão Debezium (colunas `Op`/`_timestamp`). Mantemos apenas a versão mais recente de cada `id`.

5. **Idempotência por reference_date:** DELETE antes de INSERT na fato e agregações. Execuções repetidas para a mesma data não geram duplicatas.

6. **Dependências mínimas:** Apenas `duckdb`, `pyarrow` e `pytest`. Sem pandas, SQLAlchemy, Airflow ou qualquer framework pesado.

## Testes

```bash
pytest tests/ -v
```

- **12 testes de tolerância** — cobrindo zero, 0.01, 0.02, valores nulos, valores negativos
- **11 testes de janela** — bordas (7 dias, 8 dias), datas dentro/fora, diferentes referências
- **9 testes de integração** — contagens, tolerância aplicada, dimensões, agregações, idempotência

## Perguntas que o Modelo Responde

### Operações
- Quantas transações foram reconciliadas por dia? Qual a taxa de MATCHED vs MISMATCHED?
- Quais merchants têm mais divergências?
- Qual o total de diferenças por categoria/dia?

### CFO
- Qual o volume financeiro processado por mês (interno vs processador)?
- Qual a diferença acumulada entre valores internos e do processador?
- Tendência mensal das divergências

### Compliance
- Rastreabilidade completa: cada transação, seu run de origem, datas e valores
- Histórico de alterações de categoria (via bronze)
- Dados de merchant para auditoria (CNPJ, razão social, CNAE)

## Extensibilidade

Para adicionar uma segunda fonte de liquidação:

1. **Novo arquivo:** `src/fontes.py` — nova função `ler_fonte_y()` com schema e validações
2. **Alterar `pipeline.py`:** +~10 linhas para ler a nova fonte e fazer UNION com os resultados
3. **Alterar `config.py`:** +~3 constantes (nome do arquivo, colunas esperadas)
4. **Novos testes:** +1 arquivo `tests/test_fonte_y.py`

Total estimado: ~5 arquivos tocados, ~50 linhas novas.