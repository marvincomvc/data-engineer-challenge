# 🏦 Settlement Reconciliation Platform

> Plataforma de dados para processamento, reconciliação financeira e disponibilização de indicadores executivos para o time financeiro (CFO).

![Status](https://img.shields.io/badge/status-ativo-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![DuckDB](https://img.shields.io/badge/engine-DuckDB-yellow)
![License](https://img.shields.io/badge/license-internal-lightgrey)

---

## 📖 Visão Geral

Este projeto implementa um pipeline de dados construído sobre o conceito de **Medallion Architecture**, separando responsabilidades entre ingestão, tratamento, modelagem analítica e consumo executivo.

O objetivo final é entregar, de forma confiável e auditável, indicadores de reconciliação financeira para tomada de decisão do CFO através de um dashboard executivo.

---

## 🏗️ Arquitetura

```
                    ┌──────────────────────────┐
                    │      Fonte de Dados       │
                    │    docs/sample-data       │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │      🟤  RAW              │
                    │      Arquivos originais   │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │      🥉  BRONZE           │
                    │      DuckDB                │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │      🥈  SILVER           │
                    │      Tratamento /          │
                    │      Deduplicação          │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │      🥇  GOLD             │
                    │      Modelo Dimensional    │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │      💎  DIAMOND          │
                    │      Camada CFO/Executiva  │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │   🖥️  FastAPI Dashboard   │
                    └──────────────────────────┘
```

O fluxo é estritamente unidirecional: cada camada consome exclusivamente a camada anterior, garantindo rastreabilidade e reprodutibilidade de ponta a ponta.

---

## 🧰 Stack Tecnológica

| Categoria | Tecnologias |
|---|---|
| **Processamento** | Python 3.12 · DuckDB · PyArrow · FastAPI · Docker / Docker Compose |
| **Qualidade** | Ruff · Pytest · Data Quality Checks executados via pipeline |
| **Visualização** | HTML · Chart.js · FastAPI (dashboard + API) |

---

## 🧭 Decisões Arquiteturais

### Por que DuckDB como engine principal?

| Motivo | Justificativa |
|---|---|
| Volume compatível | O volume do case é adequado a processamento OLAP local |
| Performance | Consultas SQL rápidas diretamente sobre arquivos Parquet |
| Simplicidade | Elimina a dependência de clusters distribuídos |
| Evolutividade | Mantém caminho aberto para engines maiores no futuro |

**Estrutura física:**

```
/app/duckdb-data
└── warehouse.duckdb
```

**Schemas do banco:**

```
bronze   silver   gold   diamond   quality
```

### Por que não Spark?

O projeto iniciou com uma arquitetura baseada em **Spark Cluster**, mas após avaliação técnica optou-se por não seguir esse caminho:

- ❌ O processamento não justificava a complexidade operacional de um cluster.
- ✅ O DuckDB entrega performance suficiente para o volume atual.
- 🎯 O foco do projeto é **modelagem e qualidade dos dados**, não escala distribuída.

> A decisão sobre o Spark foi mantida documentada como registro arquitetural (ADR).

**Gatilhos para reavaliar Spark no futuro:**

- 📈 Aumento significativo de volume
- 🔀 Necessidade real de processamento distribuído
- 👥 Múltiplos consumidores concorrentes do pipeline

---

## 🪜 Camadas de Dados

### 🟤 RAW — Camada de Ingestão

**Responsabilidade:** preservar os dados originais recebidos, sem qualquer alteração, servindo como camada de auditoria.

```
raw/
├── transactions_batch_1.parquet
├── transactions_batch_2.parquet
├── reconciliation_runs.parquet
├── reconciliation_results.parquet
├── enterprise_company.parquet
└── settlement_paysettler.csv
```

---

### 🥉 BRONZE — Primeira Camada no DuckDB

**Responsabilidade:** representar os dados originais dentro do DuckDB, com controle de ingestão.

**Metadados adicionados:**

| Campo | Descrição |
|---|---|
| `ingestion_timestamp` | Momento exato da ingestão |
| `ingestion_date` | Data da ingestão |
| `date_partition` | Partição lógica para consultas |

**Tabelas exemplo:**

```
bronze.transactions
bronze.reconciliation_results
bronze.enterprise_company
```

---

### 🥈 SILVER — Camada Confiável

**Responsabilidade:** entregar dados tratados e confiáveis para consumo analítico.

#### Deduplicação

Quando a fonte possui múltiplas versões do mesmo registro, aplica-se:

```sql
PARTITION BY chave_primaria
ORDER BY updated_at DESC
```

Sempre mantendo o **registro mais atualizado**.

**Exemplo prático:**

| `transaction_id` | Versão | `updated_at` | Resultado na Silver |
|---|---|---|---|
| 123 | 1 | 10:00 | — |
| 123 | 2 | 12:00 | ✅ Mantida |

#### Tipagem

| Campo | Antes | Depois |
|---|---|---|
| `amount` | `VARCHAR` → `"100,50"` | `DOUBLE` → `100.50` |
| Datas | `VARCHAR` | `DATE` / `TIMESTAMP` |

---

### 🥇 GOLD — Modelo Dimensional

**Responsabilidade:** estruturar o modelo dimensional para consumo analítico (star schema).

**Dimensões:**

```
gold.dim_company
gold.dim_date
```
> Dados mestres: empresas, merchants, calendário.

**Fatos:**

```
gold.fact_reconciliation
```
Contém: transações reconciliadas, valores internos, valores externos, diferenças e status.

**Modelo dimensional:**

```
   dim_company                    dim_date
        │                              │
        │                              │
        └────────────┬─────────────────┘
                      │
                      ▼
             fact_reconciliation
```

---

### 💎 DIAMOND — Camada Executiva

**Objetivo:** não expor tabelas complexas diretamente ao dashboard — entrega dados agregados, prontos para visualização e com regras de negócio já aplicadas.

| Tabela | Conteúdo |
|---|---|
| `diamond.cfo_daily_metrics` | Volume de transações, valor interno, valor processado, diferença financeira |
| `diamond.operations_reconciliation_health` | Quantidade de transações, quantidade reconciliada, quantidade divergente, % de divergência |
| `diamond.compliance_audit_history` | Histórico de execução, arquivos processados, status, valores conciliados |

**Exemplo de esquema — `cfo_daily_metrics`:**

| Coluna | Descrição |
|---|---|
| `reference_date` | Data de referência |
| `transaction_volume` | Volume de transações |
| `total_internal_amount` | Valor total interno |
| `total_processor_amount` | Valor total processado |
| `total_difference` | Diferença financeira |

---

## ✅ Data Quality

Todas as validações ficam isoladas no módulo:

```
app/quality
```

**Responsabilidades:**

- 🔍 Validar existência das tabelas
- 📊 Validar volume
- 🔑 Validar chaves
- 💰 Validar regras financeiras

**Checks implementados:**

| Categoria | Regra |
|---|---|
| Integridade | `transaction_id` não pode ser `NULL` |
| Duplicidade | Não podem existir `transaction_id` duplicados na Silver |
| Valores financeiros | `amount > 0` |
| Relacionamentos | Toda transação deve possuir merchant válido |
| Reconciliação | Taxa de `MATCH` deve estar dentro do esperado |

**Resultado consolidado em:**

```
quality.results
```

| Campo | Descrição |
|---|---|
| `check_name` | Nome da verificação |
| `status` | Resultado (pass/fail) |
| `message` | Detalhe do resultado |
| `rows` | Linhas afetadas |
| `checked_at` | Timestamp da checagem |

---

## 📊 Dashboard

```
   Browser
      │
      │  HTTP
      ▼
   localhost:8000
      │
      ▼
   FastAPI
      │
      ▼
   /api/dashboard
      │
      ▼
   DuckDB
      │
      ▼
   diamond tables
```

### Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/` | Entrega o dashboard executivo em HTML |
| `GET` | `/api/dashboard` | Retorna os indicadores em JSON |

**Payload de resposta — `/api/dashboard`:**

```json
{
  "kpis": {},
  "daily": {},
  "status": {},
  "merchants": []
}
```

---

## 🚀 Execução

```bash
docker compose up
```

A aplicação executa automaticamente, em sequência:

1. Load RAW
2. Construção Bronze
3. Construção Silver
4. Construção Gold
5. Construção Diamond
6. Quality Checks
7. Disponibilização do Dashboard

---

## 📂 Estrutura do Projeto

```
app/
├── raw/
│   └── load_raw.py
├── bronze/
│   └── build.py
├── silver/
│   └── build.py
├── gold/
│   └── build.py
├── diamond/
│   └── build.py
├── quality/
│   ├── checks.py
│   ├── models.py
│   └── run.py
├── dashboard/
│   ├── api.py
│   ├── service.py
│   └── index.html
└── config.py
```

---

## 🔭 Próximas Evoluções

- [ ] **Airflow** para orquestração de pipelines
- [ ] **dbt** para gerenciamento dos modelos SQL
- [ ] **CI/CD** com GitHub Actions
- [ ] Testes automatizados de contrato
- [ ] Autenticação no dashboard
- [ ] Migração para ambiente cloud (AWS / Databricks)

---

<p align="center"><sub>Documentação técnica — Settlement Reconciliation Platform</sub></p>