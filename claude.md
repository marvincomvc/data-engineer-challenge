# CLAUDE.md — Agente de Implementação: Settlement Reconciliation Data Platform

## Visão Geral da Missão

Você está implementando uma plataforma de dados para um Settlement Reconciliation Service de uma fintech. O objetivo é transformar dados transacionais brutos em produtos de dados consumíveis pelo negócio (Operações, CFO, Compliance).

Trabalhe de forma incremental, validando cada etapa antes de avançar. Documente decisões e tradeoffs ao longo do caminho.

---

## Contexto do Domínio

**Fluxo de negócio:**
1. PaySettler envia CSV diário com transações liquidadas das últimas 24h
2. O serviço compara com registros internos (`settlement_db`)
3. Cada transação é categorizada e persistida

**Categorias de reconciliação:**
- `MATCHED` — transação encontrada e valores batem
- `MISMATCHED` — encontrada, mas com divergência de valor
- `UNRECONCILED_PROCESSOR` — existe no PaySettler, não existe internamente
- `UNRECONCILED_INTERNAL` — existe internamente, não existe no PaySettler

**Schema das fontes** (PostgreSQL `settlement_db`):
- `transactions` — registros internos (id, transaction_id, merchant_id, amount, currency, status, created_at, updated_at)
- `reconciliation_runs` — execuções (id, reference_date, file_name, status, total_transactions, started_at, completed_at)
- `reconciliation_results` — resultados (id, run_id, transaction_id, merchant_id, category, internal_amount, processor_amount, difference)
- `enterprise_company` — cadastro de merchants (merchant_id, legal_name, trade_name, document/CNPJ, primary_cnae)

**CSV do PaySettler:** transaction_id, merchant_id, amount, currency, settled_at (ISO 8601 UTC), processor_reference, status (`SETTLED` | `REVERSED`)

**Dados de exemplo:** `docs/sample-data/` contém arquivos `.parquet` e o CSV do PaySettler. Os arquivos parquet de transactions têm dois batches (`transactions_batch_1.parquet`, `transactions_batch_2.parquet`) com possível schema drift entre eles — trate isso.

---

## Stack Obrigatória

- **Python 3.12+**
- **DuckDB** para todo processamento analítico (não use pandas como motor principal)
- **Docker + Docker Compose** — o ambiente deve subir com `docker-compose up`
- **Filesystem local** como storage (sem S3, sem bancos externos além do DuckDB)
- Armazenamento final em **Parquet** particionado ou **DuckDB persistent file**

---

## Parte 1 — Modelagem Analítica

### Tabelas a implementar

Projete e implemente as seguintes tabelas analíticas. O schema exato é decisão sua, mas documente o raciocínio.

**Para Operações** — saúde diária das reconciliações:
- Taxa de discrepância por dia (`MISMATCHED / total`)
- Volume de não-reconciliados por categoria e dia
- Tempo médio de processamento por run
- Runs com falha ou em progresso há mais de X horas

**Para o CFO** — visão financeira consolidada:
- Volume transacionado por dia (BRL)
- Volume liquidado pelo processador por dia
- Diferença acumulada entre valor interno e processado
- Breakdown por merchant (top N por volume)

**Para Compliance** — rastreabilidade e auditoria:
- Histórico completo de todas as runs com metadados
- Todas as transações com divergência (MISMATCHED), com valores de ambos os lados
- Transações UNRECONCILED com merchant e data de referência
- Imutabilidade: registros históricos não devem ser sobrescritos

### Entregáveis da Parte 1

1. DDL ou código de criação das tabelas analíticas
2. Arquivo `docs/analytics/data_model.md` documentando:
   - Perguntas que cada tabela responde (e quais deliberadamente não responde)
   - Decisões de modelagem (granularidade, particionamento, surrogate keys, etc.)
3. Arquivo `docs/analytics/sample_queries.sql` com queries funcionais para cada consumidor
4. As queries devem rodar contra os dados de `docs/sample-data/`

---

## Parte 2 — Pipeline de Dados

### Estrutura esperada do projeto

```
pipeline/
  __init__.py
  cli.py                  # entrypoint CLI
  ingest/
    parquet_reader.py     # leitura dos .parquet com tratamento de schema drift
    csv_reader.py         # leitura do CSV do PaySettler com normalização de datas
  transform/
    bronze.py             # camada bronze: ingestão raw sem transformações de negócio
    silver.py             # camada silver: limpeza, deduplicação, joins
    gold.py               # camada gold: agregações e tabelas para consumo final
    reconciliation.py     # lógica de categorização (MATCHED, MISMATCHED, etc.)
    transactions.py
    merchants.py
  load/
    writer.py             # escrita idempotente (staging → promote)
  quality/
    checks.py             # checks HARD e SOFT com resultados estruturados
  observability/
    logging.py
    metrics.py
  config.py

data/
  warehouse.duckdb        # arquivo DuckDB persistente (gerado em runtime)

tests/
  conftest.py
  test_bronze.py
  test_silver.py
  test_gold.py
  test_checks.py
  test_idempotency.py
  test_generate_sample_data.py   # NÃO MODIFICAR — teste existente do gerador
```

### Arquitetura Medallion no DuckDB

Todas as camadas vivem dentro do arquivo `data/warehouse.duckdb`. Use schemas do DuckDB para separar as camadas:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;  -- pipeline_runs e metadados
```

**Camada Bronze (`bronze.*`)** — dados brutos, sem transformação de negócio:
- Ingestão direta dos Parquet e CSV exatamente como vieram da fonte
- Colunas extras dos batches são mantidas (não descartadas)
- Adicione apenas colunas de metadados de ingestão: `_ingested_at`, `_source_file`, `_reference_date`
- Nunca aplique regras de negócio aqui — se o dado veio errado, ele entra errado no bronze
- Tabelas: `bronze.transactions_raw`, `bronze.reconciliation_runs_raw`, `bronze.reconciliation_results_raw`, `bronze.paysettler_raw`, `bronze.enterprise_company_raw`

**Camada Silver (`silver.*`)** — dados limpos e integrados:
- Deduplicação de `transaction_id` (manter mais recente por `updated_at`)
- Normalização de tipos (datas para UTC, decimais consistentes)
- Resolução de schema drift entre batches (union com coalesce de colunas divergentes)
- Join de `reconciliation_results` com `reconciliation_runs` para adicionar `reference_date`
- Join com `enterprise_company` para enriquecer com `legal_name`, `trade_name`, `document`
- Checks de qualidade HARD são aplicados aqui — registros inválidos são rejeitados com log
- Tabelas: `silver.transactions`, `silver.reconciliation_results`, `silver.paysettler_settlements`, `silver.merchants`

**Camada Gold (`gold.*`)** — tabelas prontas para consumo pelo negócio:
- Agregações pré-computadas alinhadas aos consumidores (Operações, CFO, Compliance)
- Nunca contém dados brutos — sempre derivado do silver
- Cada tabela gold deve ter um owner declarado em comentário: `-- consumer: cfo`
- Tabelas (exemplos orientativos — o schema exato é decisão sua):
  - `gold.daily_reconciliation_health` — para Operações
  - `gold.daily_financial_volume` — para CFO
  - `gold.merchant_volume_summary` — para CFO
  - `gold.audit_discrepancies` — para Compliance
  - `gold.audit_unreconciled` — para Compliance

**Tabela de controle (`control.pipeline_runs`)** — fora das camadas medallion:
- Registra cada execução com `reference_date`, `status`, `started_at`, `completed_at`, `source_file_hash`
- É o primeiro artefato a checar em incidentes (ver Runbook)
- Nunca deletar registros desta tabela — apenas atualizar status

### CLI obrigatória

O pipeline deve ser acionável via CLI com ao menos:

```bash
# Executa para uma data de referência específica
python -m pipeline.cli run --reference-date 2024-01-15

# Re-executa (deve ser idempotente)
python -m pipeline.cli run --reference-date 2024-01-15 --force

# Verifica status de uma run
python -m pipeline.cli status --reference-date 2024-01-15
```

### Idempotência — regras estritas

- Executar para a mesma `reference_date` duas vezes **nunca** deve duplicar registros
- Use uma tabela/arquivo de controle de runs (`pipeline_runs`) com status e hash do arquivo processado
- Se uma run está `IN_PROGRESS` há mais de 1h, trate como falha e permita re-execução com `--force`
- Em caso de falha no meio da execução, o estado anterior deve ser preservado (write to temp, then atomic rename/replace)
- Estratégia recomendada: escreva em staging, valide, depois promova para produção com DELETE+INSERT ou REPLACE partition

### Qualidade de dados

Implemente checks explícitos. Classifique cada check como **HARD** (para o pipeline) ou **SOFT** (loga e continua):

| Check | Tipo sugerido |
|-------|--------------|
| `transaction_id` nulo em reconciliation_results | HARD |
| `amount` negativo em transactions | HARD |
| Taxa de MISMATCHED > 20% na run | SOFT (alerta) |
| `merchant_id` sem cadastro em enterprise_company | SOFT |
| Arquivo CSV vazio ou com zero linhas | HARD |
| Duplicatas de `transaction_id` dentro da mesma run | HARD |
| Schema drift entre batches de parquet | SOFT com log detalhado |

Implemente em `quality/checks.py` como funções testáveis que recebem um DuckDB relation e retornam resultados estruturados.

### Observabilidade

Cada execução deve gerar:

1. **Log estruturado** (JSON) com ao menos:
   - `run_id`, `reference_date`, `status`, `started_at`, `completed_at`
   - Contagens: total processado, matched, mismatched, unreconciled
   - Erros e warnings com contexto

2. **Tabela de controle** `pipeline_runs` persistida com histórico de execuções

3. **Métricas de qualidade** por run: taxa de cada categoria, volume financeiro, checks que falharam

### Tratamento de dados sujos (esperados nos arquivos de exemplo)

- Schema drift entre `transactions_batch_1.parquet` e `transactions_batch_2.parquet` (colunas extras, tipos ligeiramente diferentes) — normalize antes de unir
- Valores nulos em colunas que deveriam ser NOT NULL — trate com política explícita (rejeitar linha vs. substituir default)
- Duplicatas de `transaction_id` — deduplicar mantendo o registro mais recente por `updated_at`
- Datas em formatos mistos no CSV — normalize para UTC

---

## Parte 3 — Arquitetura

### 3.1 Diagrama de Arquitetura

Crie `docs/architecture/architecture.md` com:

- Diagrama ASCII ou Mermaid do fluxo completo: fonte → ingestão → transformação → camadas analíticas → consumo
- Descrição de cada componente e sua responsabilidade
- Como o pipeline se integraria em produção com o PostgreSQL transacional (CDC vs. batch export)
- Onde ficaria o orquestrador (ex: Airflow, Prefect) e como o pipeline atual se conectaria a ele

### 3.2 Runbook de Troubleshooting

Crie `docs/runbook/monday_incident.md` com o passo a passo para o cenário:

> Segunda-feira de manhã. Dashboards sem dados desde sexta à noite.

O runbook deve cobrir:
1. Primeiro artefato a checar (tabela `pipeline_runs` — qual query rodar)
2. Como distinguir falha de pipeline vs. falha de ingestão vs. dado ausente da fonte
3. Checks de saúde do ambiente Docker
4. Quando e como acionar o time de Engenharia vs. time de Dados vs. fornecedor (PaySettler)
5. Procedimento de re-execução segura para datas perdidas

### 3.3 Plano de Escalabilidade

Crie `docs/architecture/scalability.md` cobrindo:

- Onde o design atual quebra primeiro ao atingir ~5M transações/dia
- Como migrar de DuckDB local para uma solução distribuída sem reescrever a lógica de negócio
- Estratégia de particionamento para `reconciliation_results` em escala (1,8B rows/ano)
- O que mudaria na estratégia de idempotência com múltiplos workers paralelos

---

## Ambiente Docker

O `docker-compose.yml` deve:

- Definir um serviço `pipeline` com Python 3.12+
- Montar `docs/sample-data/` como volume read-only em `/data/input`
- Montar um volume de output em `/data/output` para os artefatos gerados
- Expor o comando padrão como `python -m pipeline.cli --help`

O `Makefile` deve ter ao menos:

```makefile
make up          # docker-compose up --build
make run         # executa pipeline para hoje
make test        # roda testes
make generate    # gera 10k linhas de dados sintéticos
make generate-large  # gera 1M linhas
make help        # lista targets
```

---

## Testes

Implemente testes em `tests/` cobrindo ao menos:

- **Unit tests** para cada check de qualidade de dados
- **Unit tests** para as transformações principais (deduplicação, categorização, cálculo de diferença)
- **Integration test** de idempotência: executar o pipeline duas vezes para a mesma data e verificar que o count de registros não muda
- **Fixture** baseada nos dados de `docs/sample-data/` para testes determinísticos

Use `pytest`. Configure `testpaths = ["tests/"]` em `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests/"]
```

O `make test` deve rodar sem dependências externas além do Docker.

---

## Geração de Dados Sintéticos

O gerador está disponível em `scripts/generate_sample_data.py` — **não modifique este arquivo**. Use-o dentro do container para validar o pipeline em escala:

```bash
# Dataset médio para smoke em escala (1M transações, 90 dias)
python scripts/generate_sample_data.py --rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/medium

# Dataset alinhado à projeção de ~5M transações/mês
python scripts/generate_sample_data.py --rows 5000000 --days 30 --out /tmp/month
```

Flags principais:

| Flag | Descrição | Default |
|------|-----------|---------|
| `--rows N` | Total de transações internas a gerar | `1000000` |
| `--days N` | Janela temporal em dias | `90` |
| `--merchants N` | Número de merchants | `500` |
| `--seed N` | Semente determinística | `42` |
| `--out PATH` | Diretório de saída | `docs/sample-data` |

O gerador produz o mesmo schema do fixture com CDC (`Op`/`_timestamp`), schema drift intencional entre batches e padrões realistas de dados sujos. Os atalhos `make generate` (10k linhas) e `make generate-large` (1M linhas) estão disponíveis.

---

## O que NÃO Modificar

Os arquivos abaixo são parte da infraestrutura do desafio. **Não altere, delete ou renomeie nenhum deles:**

| Arquivo / Diretório | Motivo |
|---------------------|--------|
| `pyproject.toml` | Dependências core definidas (`duckdb`, `pyarrow`) — adicione dependências, não substitua |
| `Dockerfile` | Base image e estrutura de build já definidas |
| `docker-compose.yml` | Serviço `pipeline` e volumes já configurados |
| `scripts/generate_sample_data.py` | Gerador fornecido pelo desafio |
| `tests/test_generate_sample_data.py` | Teste existente do gerador — deve continuar passando |
| `docs/sample-data/` | Dados de exemplo montados como read-only no container (`/data/input`) |

Se precisar adicionar dependências Python, faça em `pyproject.toml` na seção correta sem remover as existentes. Se precisar estender o Makefile ou o `docker-compose.yml`, adicione apenas — não substitua serviços ou targets existentes.

---

## Entregáveis Finais (checklist)

### Código
- [ ] `pipeline/` — módulo Python completo e funcional
- [ ] `tests/` — suíte de testes com `make test` passando
- [ ] `docker-compose.yml` — ambiente funcional
- [ ] `Makefile` com targets documentados

### Documentação
- [ ] `README.md` — como rodar, premissas, arquitetura resumida
- [ ] `docs/analytics/data_model.md` — modelagem e raciocínio por consumidor
- [ ] `docs/analytics/sample_queries.sql` — queries funcionais
- [ ] `docs/architecture/architecture.md` — diagrama e descrição
- [ ] `docs/architecture/scalability.md` — plano de escala
- [ ] `docs/runbook/monday_incident.md` — runbook de troubleshooting

### Seções do README (PR)
- [ ] Como rodar
- [ ] Premissas e decisões
- [ ] Visão geral da arquitetura
- [ ] Extensibilidade (segunda fonte de liquidação: quantos arquivos mudam?)
- [ ] Limites do desenho (2–3 exemplos concretos e específicos)
- [ ] O que faria diferente em produção
- [ ] Ferramentas de IA utilizadas

---

## Restrições e Princípios de Implementação

**Faça:**
- Use DuckDB para todo processamento — aproveite SQL analítico, window functions, COPY, leitura direta de Parquet
- Escreva funções pequenas e testáveis
- Prefira clareza a cleverness — o código será lido em uma conversa técnica
- Documente decisões não-óbvias com comentários curtos no código
- Trate erros explicitamente; evite `except: pass`

**Não faça:**
- Não use pandas como motor de transformação principal (DuckDB primeiro)
- Não conecte a serviços externos além do filesystem local
- Não use ORMs para as tabelas analíticas — SQL direto via DuckDB
- Não inclua segredos ou chaves hardcoded

**Tradeoffs explícitos:**
- Priorize correção e idempotência sobre performance
- Priorize legibilidade sobre over-engineering
- Se o tempo estiver curto, reduza escopo (ex.: menos checks de qualidade), não qualidade do que foi feito
- Documente o que foi deixado de fora e por quê

---

## Ordem de Implementação Sugerida

1. Leia `docs/domain-glossary.md` antes de qualquer código
2. Explore os dados de `docs/sample-data/` com DuckDB interativo — entenda o schema real, não só o documentado
3. Implemente a modelagem analítica (Parte 1) primeiro — define o contrato de dados
4. Implemente o pipeline (Parte 2) para popular essas tabelas
5. Valide idempotência manualmente antes de escrever o teste automatizado
6. Escreva a documentação de arquitetura (Parte 3) com base no que foi implementado, não antes
7. Revise o README com olhos de quem vai fazer code review no PR