# Case Técnico — Engenheiro(a) de Dados Sênior / Especialista

## Contexto de Negócio

Você está entrando no time de dados de uma fintech que processa liquidações de pagamentos para merchants. A empresa opera o **Settlement Reconciliation Service** — um serviço em produção que:

1. Recebe diariamente um **arquivo CSV** do processador de pagamentos externo (**PaySettler**) com as transações liquidadas nas últimas 24h
2. Compara essas transações com os **registros internos** do sistema
3. Categoriza cada transação e persiste os resultados em um **PostgreSQL** (`settlement_db`)

O serviço funciona bem, mas **todos os dados vivem apenas no banco transacional**. O time de operações precisa saber se a taxa de discrepância está subindo. O CFO quer o volume transacionado por dia. Compliance precisa de histórico de auditoria. **Ninguém tem acesso estruturado a nada disso hoje.**

### Sua Missão

Construir uma solução que transforme esses dados em **produtos de dados** consumíveis pelo negócio. Cabe a você decidir como.

---

## Domínio: Reconciliação de Liquidações

> O glossário completo está em [`docs/domain-glossary.md`](docs/domain-glossary.md). Leia antes de começar.

### Tabelas Fonte (`settlement_db`)

**`transactions`** — Transações internas do sistema

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `transaction_id` | uuid | Identificador único da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal | Valor da transação (BRL) |
| `currency` | varchar | Moeda (ISO 4217) |
| `status` | varchar | `COMPLETED`, `PENDING`, `FAILED` |
| `description` | varchar | Descrição da transação |
| `created_at` | timestamp | Data de criação |
| `updated_at` | timestamp | Última atualização |

**`reconciliation_runs`** — Execuções de reconciliação

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `reference_date` | date | Data de referência do arquivo (dia de negócio) |
| `file_name` | varchar | Nome do arquivo processado |
| `status` | varchar | `IN_PROGRESS`, `COMPLETED`, `FAILED` |
| `total_transactions` | integer | Total de transações no arquivo |
| `started_at` | timestamp | Início do processamento |
| `completed_at` | timestamp | Fim do processamento |
| `created_at` | timestamp | Data de criação do registro |

**`reconciliation_results`** — Resultados da reconciliação

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `run_id` | bigint | FK → `reconciliation_runs.id` |
| `transaction_id` | uuid | Identificador da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `category` | varchar | `MATCHED`, `MISMATCHED`, `UNRECONCILED_PROCESSOR`, `UNRECONCILED_INTERNAL` |
| `internal_amount` | decimal | Valor no sistema interno (null se unreconciled_processor) |
| `processor_amount` | decimal | Valor no PaySettler (null se unreconciled_internal) |
| `difference` | decimal | Diferença absoluta entre valores |
| `created_at` | timestamp | Data de criação |

**`enterprise_company`** — Dados cadastrais dos merchants

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | bigint | PK |
| `merchant_id` | varchar | Código do merchant |
| `legal_name` | varchar | Razão social |
| `trade_name` | varchar | Nome fantasia |
| `document` | varchar | CNPJ |
| `primary_cnae` | varchar | CNAE principal |
| `created_at` | timestamp | Data de criação |
| `updated_at` | timestamp | Última atualização |

### Arquivo CSV do PaySettler

O processador externo envia diariamente um CSV com as transações liquidadas.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `transaction_id` | uuid | Identificador da transação |
| `merchant_id` | varchar | Identificador do merchant |
| `amount` | decimal | Valor liquidado |
| `currency` | varchar | Moeda (ISO 4217) |
| `settled_at` | datetime (ISO 8601, UTC) | Data/hora da liquidação |
| `processor_reference` | varchar | Referência interna do processador |
| `status` | varchar | `SETTLED` ou `REVERSED` |

---

## Stack

- **Linguagem:** Python 3.12+
- **Processamento:** DuckDB
- **Conteinerização:** Docker + Docker Compose
- **Armazenamento:** Filesystem local

A solução deve **rodar localmente** com `docker-compose up`.

---

## Dados de Exemplo

Na pasta `docs/sample-data/` você encontrará:

- `transactions_batch_1.parquet` — Transações internas (lote 1)
- `transactions_batch_2.parquet` — Transações internas (lote 2)
- `reconciliation_runs.parquet` — Execuções de reconciliação
- `reconciliation_results.parquet` — Resultados categorizados
- `settlement_paysettler.csv` — Arquivo CSV do PaySettler
- `enterprise_company.parquet` — Dados cadastrais de merchants

---

## Como Começar

1. **Faça um fork** deste repositório para sua conta pessoal do GitHub
2. Clone o seu fork e trabalhe nele normalmente
3. Suba o ambiente:

```bash
docker compose up -d --build
# ou: make up
```

Um `Makefile` no repositório expõe atalhos para as operações mais comuns (`make help` lista os alvos disponíveis). O uso é opcional.

---

## Requisitos

### Parte 1 — Modelagem e Analytics

Usando os dados disponíveis, **modele e implemente tabelas analíticas** que atendam os seguintes consumidores:

- **Operações:** precisa acompanhar a saúde das reconciliações no dia a dia
- **CFO:** precisa de visão consolidada do volume financeiro
- **Compliance:** precisa de rastreabilidade e histórico para auditorias

**A forma e o conteúdo das tabelas são decisão sua.** Traduza as dores acima em métricas e tabelas analíticas (fatos e dimensões) que você defenderia em produção. Documente, para cada consumidor:

- **Quais perguntas** seu modelo consegue responder (e quais deliberadamente não)
- **Por que** essas e não outras (priorização, tradeoffs de escopo vs tempo)

Entregue também exemplos de queries rodando contra suas tabelas que respondem as perguntas que você propôs. Queremos ver seu raciocínio de produto, não só SQL.

---

### Parte 2 — Pipeline de Dados

Implemente o pipeline que processa as fontes de dados disponíveis e prepara o insumo para as tabelas analíticas da Parte 1.

O volume em `docs/sample-data/` é pequeno de propósito — assuma **~5M transações/mês em produção** e projete para crescer. Se quiser validar na prática como seu pipeline se comporta em escala, use o gerador descrito na seção _"Dados de Exemplo e Geração de Volume"_.

Critérios de qualidade (como você os atende é decisão sua — documente suas escolhas):

- **Correção:** a saída do pipeline bate com o contrato de dados que você propôs
- **Idempotência:** executar o pipeline para a mesma data de referência duas vezes não pode duplicar registros nem corromper as tabelas. Considere como isso se manifesta na sua CLI, nas tabelas finais e em falhas no meio da execução
- **Observabilidade:** em produção, quando algo quebrar, que sinais você precisaria para diagnosticar rapidamente?
- **Qualidade de dados:** quais checks você adicionaria para proteger os consumidores de downstream de dados incorretos? O que faria o pipeline parar versus apenas alertar?
- **Testabilidade:** como você valida automaticamente que uma mudança no código não quebra o comportamento?

Stack obrigatória: **DuckDB** para processamento, **Python 3.12+**, **Docker Compose** para subir o ambiente.

---

### Parte 3 — Arquitetura

#### 3.1 Desenho de Arquitetura

**Explique e/ou desenhe** como você levaria esses dados do banco transacional até o consumo pelo negócio em produção.

#### 3.2 Troubleshooting

Segunda-feira de manhã. Ops abre um chamado: os dashboards de reconciliação estão sem dados desde sexta-feira à noite. Você é o primeiro a chegar.

Como você investigaria? Que artefatos do próprio pipeline você consultaria? Até onde iria antes de acionar mais alguém?

#### 3.3 Escalabilidade

Hoje o volume é pequeno, mas o negócio projeta chegar em **~5M transações/dia** em 18 meses (≈1,8B/ano em `reconciliation_results`). Como você prepararia a plataforma para esse crescimento? Onde o desenho atual quebra primeiro?

---

## Dados de Exemplo e Geração de Volume

O repositório oferece duas fontes de dado. **A escolha de volume e de quais fontes usar é sua.**

### 1. Fixture pequeno — `docs/sample-data/`

Cerca de 1.000 transações internas e ~500 liquidações. Pensado para exploração, desenvolvimento local e como insumo default dos exemplos de queries da Parte 1.

### 2. Gerador sintético — `scripts/generate_sample_data.py`

Produz o mesmo schema do fixture (CDC `Op`/`_timestamp`, schema drift entre batches, padrões realistas de dados sujos) em volume arbitrário. Útil para validar seu pipeline em escala — por exemplo, ao redor dos ~5M transações/mês assumidos na Parte 2.

**Você define o volume e as características.** O script é parametrizável:

```bash
# dataset médio para smoke em escala (1M transações, 90 dias)
python scripts/generate_sample_data.py --rows 1000000 --days 90 --out /tmp/medium

# dataset alinhado à projeção de ~5M transações/mês
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

Rode `python scripts/generate_sample_data.py --help` para a lista completa. Dentro do container, `make generate` (10k linhas) e `make generate-large` (1M linhas) são atalhos prontos.

---

## Uso de IA

Fique à vontade para usar ferramentas de IA durante o desafio. Se usar, documente no item _"Ferramentas de IA utilizadas"_ da entrega quais ferramentas e para quê. Se mantiver prompts, configs (`CLAUDE.md`, `.cursorrules`, etc.) ou transcrições no repositório, melhor — ajuda a entender seu processo.

---

## Entrega

1. Ao finalizar, **abra um Pull Request do seu fork para o repositório original** (branch `main`)
2. Preencha as seções abaixo no README do seu PR:

### Como rodar

**Pré-requisito:** Docker + Docker Compose instalados.

```bash
# 1. Clonar e entrar no diretório
git clone <url-do-fork>
cd data-engineer-challenge

# 2. Build + execução do pipeline contra os dados de exemplo (2025-03-04)
docker compose up --build
# Container sai com código 0 ao terminar. Logs em JSON estruturado no stdout.

# 3. Rodar para uma data específica
make pipeline-date DATE=2025-03-16

# 4. Executar todos os testes (91 testes)
make test

# 5. Abrir shell interativo dentro do container
make shell

# 6. Smoke test com dataset em escala (~1M transações)
# Os dados gerados precisam estar no volume persistente (/data/),
# não em /tmp/ — que é efêmero e some ao encerrar o container.
make shell
# dentro do container:
python scripts/generate_sample_data.py \
  --rows 1000000 --days 90 --merchants 500 --seed 42 \
  --out /data/generated-large

python -m pipeline.main --reference-date 2025-03-04 \
  --data-path /data/generated-large
# sair do container (exit) e limpar: make clean
```

O arquivo DuckDB é persistido em `./data/warehouse.duckdb` (criado automaticamente). Para limpar e recomeçar: `make clean`.

### Consultando o warehouse após a execução

Após `make up` ou `make pipeline`, o warehouse está disponível em `./data/warehouse.duckdb`:

```bash
make shell
python
```

```python
import duckdb
c = duckdb.connect('/data/warehouse.duckdb')

# Saúde da reconciliação do dia — pergunta do time de Operações
c.sql("""
    SELECT reference_date, total_transactions, matched_count,
           match_rate_pct, total_discrepancy_amount
    FROM gold.mart_operations
""").show()

# Top merchants por volume liquidado — pergunta do CFO
c.sql("""
    SELECT merchant_legal_name, total_settled_amount,
           matched_count, mismatched_count
    FROM gold.mart_cfo
    ORDER BY total_settled_amount DESC
    LIMIT 5
""").show()
```

Queries prontas por consumidor estão em `sql/queries/`:

| Arquivo | Consumer | Perguntas respondidas |
| --- | --- | --- |
| `ops_daily_health.sql` | Operações | Taxa de match, runs abaixo de 95%, ranking de discrepância por dia |
| `cfo_volume_by_merchant.sql` | CFO | Volume liquidado por merchant, exposição financeira por mismatch |
| `compliance_audit_trail.sql` | Compliance | Histórico de uma transação, distribuição de categorias, UNRECONCILED_PROCESSOR |

```bash
# Exemplo de uso dentro do container
make shell
python -c "
import duckdb
c = duckdb.connect('/data/warehouse.duckdb')
c.sql(open('/app/sql/queries/ops_daily_health.sql').read()).show()
"
```

`make help` lista todos os alvos disponíveis.

---

### Premissas e decisões

**Estrutura do pacote.** O `pyproject.toml` usa hatchling com `bypass-selection = true`, que não detecta automaticamente o layout `src/`. O pacote foi mantido em `pipeline/` na raiz do projeto — sem modificar `pyproject.toml`.

**Ingestão do CSV como `all_varchar = true`.** O PaySettler envia valores como `"R$ 23.696,77"` (prefixo BRL + separador de milhar com ponto + decimal com vírgula). Ler como string na camada bronze preserva o dado original para auditoria e transfere a responsabilidade de normalização para a camada silver, onde é testável de forma isolada. Cinco formatos são suportados: `"13266.42"`, `"13266,42"`, `"23.696,77"`, `"R$ 23.696,77"`, `"R$ 873,16"`.

**Idempotência via DELETE + INSERT com transação explícita por etapa.** DuckDB 1.x não tem MERGE confiável. A estratégia `DELETE WHERE reference_date = ? / _source_file = ?` seguida de `INSERT` é envolvida em `BEGIN/COMMIT/ROLLBACK` explícito em cada builder — se o processo cair no meio de uma etapa, o rollback automático garante que a tabela não fique parcialmente apagada. A atomicidade é **por etapa** (ingest, silver, mart), não do pipeline inteiro: uma falha após `mart_operations` e antes de `mart_compliance` deixa os dois marts em estados de datas diferentes. Re-executar o pipeline para a mesma data corrige isso. A tabela `gold.pipeline_runs` cresce intencionalmente — é um log de auditoria, não um mart.

**CDC e schema drift.** O lote 2 de transações adiciona a coluna `payment_method` ausente no lote 1. `read_parquet(..., union_by_name=True)` resolve isso nativamente — linhas do lote 1 recebem `NULL` nessa coluna, o que é semanticamente correto. Linhas com `Op='D'` são preservadas no silver com a flag `is_deleted=true` em vez de serem descartadas — downstream pode escolher excluí-las ou não.

**Lookup do CSV de liquidação.** O pipeline tenta primeiro `paysettler/settlement_{date}.csv` (layout de dados gerados) e recua para `settlement_paysettler.csv` (layout dos dados de exemplo do desafio), logando um WARNING quando usa o fallback. Isso permite usar os mesmos dados de amostra sem configuração extra.

**Granularidade dos marts.** `mart_operations` agrega por `(reference_date, run_id)` — um run pode ter múltiplos dias na janela de 7 dias, mas cada arquivo do PaySettler pertence a um único `reference_date`. `mart_cfo` agrega por `(reference_date, merchant_id)`. `mart_compliance` mantém granularidade de transação para rastreabilidade total.

**STOP vs WARN.** Checks de STOP rodam sobre o bronze (antes da transformação) e abortam o pipeline — protegem dados estruturalmente inválidos de contaminar o silver/gold. Checks de WARN rodam sobre o silver (após transformação) e apenas logam — sinalizam anomalias de negócio que o time de Ops deve investigar sem interromper o fluxo.

---

### Visão geral da arquitetura

O pipeline segue arquitetura medallion (bronze → silver → gold) sobre um único arquivo DuckDB. Todos os consumidores leem do schema `gold`.

```text
PostgreSQL (interno)          PaySettler (externo)
     │                              │
     │ CDC Debezium                 │ CSV diário
     ▼                              ▼
  Parquet files                 settlement_YYYY-MM-DD.csv
  (por tabela)
     │                              │
     └──────────────┬───────────────┘
                    ▼
          ┌─────────────────┐
          │  Python Pipeline│  python -m pipeline.main --reference-date YYYY-MM-DD
          │                 │
          │  ① Ingest       │──► bronze.*   (CDC bruto, amounts como VARCHAR)
          │  ② STOP checks  │──► abort se violação de integridade
          │  ③ Silver build │──► silver.*   (dedup CDC, tipos normalizados)
          │  ④ WARN checks  │──► log warnings, nunca aborta
          │  ⑤ Gold marts   │──► gold.*     (agregações por consumidor)
          └─────────────────┘
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
   gold.mart_operations   gold.mart_cfo
   (Operações: match rate, (CFO: volume liquidado
    discrepâncias por run)  por merchant e dia)
          │
          ▼
   gold.mart_compliance
   (Compliance: lineage
    completo por transação)
```

O diagrama Mermaid completo com sequência de execução e decisões arquiteturais está em [`docs/architecture/diagram.md`](docs/architecture/diagram.md).

**Estrutura de pastas relevante:**

```text
pipeline/
├── config.py              # env vars: DATA_PATH, DB_PATH, LOG_LEVEL
├── db.py                  # factory DuckDB + bootstrap de schema
├── main.py                # CLI + orquestrador dos 10 steps
├── ingest/                # leitores por fonte (parquet, CSV)
├── transform/             # cdc.py (dedup), normalize.py, silver.py
├── quality/               # checks.py (STOP e WARN)
├── marts/                 # operations.py, cfo.py, compliance.py
└── observability/         # logger.py (JSON formatter + timed_step)
sql/schema/                # DDL: bronze.sql, silver.sql, gold.sql
tests/
├── unit/                  # normalize, cdc, quality_checks (sem I/O)
└── integration/           # idempotency, e2e, marts (dados sintéticos)
docs/architecture/         # diagram.md, troubleshooting.md, scalability.md
```

---

### Extensibilidade

**Cenário:** adicionar um segundo processador de liquidação, "AcquirerXYZ", com schema parecido mas não idêntico (ex.: coluna `settled_date` em vez de `settled_at`, `gross_amount` em vez de `amount`).

**O que muda:**

| Arquivo | Mudança | Linhas estimadas |
| ------- | ------- | ---------------- |
| `pipeline/ingest/settlement.py` | Adicionar `find_acquirerxyz_file()` (ou tornar `find_settlement_file` parametrizável por `source`) + `_EXPECTED_COLUMNS_XYZ` | ~25 linhas |
| `pipeline/transform/silver.py` | `build_silver_settlement()` já opera por `_source_file` — adicionar mapeamento de colunas (`settled_date → settled_at`, `gross_amount → amount`) no SELECT para a nova fonte | ~15 linhas |
| `pipeline/main.py` | Uma linha adicional: `ingest_acquirerxyz(conn, data_path, reference_date)` na sequência de ingestão | 1 linha |
| `tests/unit/test_normalize.py` | Adicionar casos para formatos de amount específicos do novo processador, se diferentes | ~10 linhas |
| `tests/integration/` | Fixture com CSV de amostra da nova fonte + 1 teste de smoke | ~30 linhas |

**O que não muda:** schema do bronze.settlement (já tem `_source_file` como campo de partição), silver.settlement (normaliza para o mesmo schema canônico), todos os marts gold (consomem apenas a camada silver normalizada).

**Total: 4 arquivos, ~80 linhas adicionadas, zero linhas removidas.**

O ponto de extensão central é `pipeline/ingest/settlement.py` + o `SELECT` de mapeamento em `build_silver_settlement()`. A separação bronze (dado bruto por fonte) / silver (dado canônico) foi desenhada exatamente para esse caso.

---

### Limites do desenho

**1. Sem suporte a re-reconciliação parcial intraday.**
O modelo de `mart_operations` tem PK `(reference_date, run_id)`, o que funciona bem quando o PaySettler envia um único arquivo por dia. Se o processador re-enviar um arquivo corrigido no mesmo dia (ou enviar dois arquivos parciais para o mesmo `reference_date`), o pipeline sobrescreve o mart corretamente — mas os dois arquivos são reconciliados juntos como se fossem um só. A arquitetura não distingue "re-envio de correção" de "envio inicial": o modelo de auditoria perde a rastreabilidade do que mudou entre os envios.

**2. Bronze com replace full, não incremental.**
`ingest_transactions()` faz `DELETE FROM bronze.transactions` e relê todos os parquets a cada execução. Isso funciona bem hoje, mas com anos de histórico e CDC incremental, a leitura de todo o histórico a cada run se torna o gargalo dominante. Não há particionamento por data nem leitura apenas do delta diário. O `scalability.md` descreve a correção (Hive partitioning + ingest incremental por date shard), mas ela requer mudança no layout dos arquivos na origem.

**3. DuckDB sem controle de acesso nem concorrência.**
O arquivo `warehouse.duckdb` é acessado por um único processo de cada vez. Duas execuções simultâneas (ex.: backfill manual enquanto o cron roda) causam erro de lock. Em produção, qualquer usuário de BI que conecte ao arquivo DuckDB diretamente pode bloquear o pipeline — ou vice-versa. A versão de produção precisaria de MotherDuck, um proxy de query (Metabase com conexão read-only), ou mover os marts para um banco que suporte concorrência (BigQuery, Redshift).

---

### O que faria diferente em produção

A arquitetura abaixo reflete o que eu aplicaria com base em experiência real em pipelines de dados sobre AWS com volume de produção.

**CDC via DMS → S3, particionado por data.** Em vez de receber parquet já consolidado, o PostgreSQL alimentaria o AWS DMS que entregaria os eventos CDC em S3 particionados por `ano/mês/dia`. O ingest leria apenas a partição do dia (`transactions/date=2025-03-04/`), reduzindo o I/O de um scan completo para segundos e permitindo backfill paralelo de múltiplas datas sem interferência entre runs.

**Formato de armazenamento: Apache Iceberg.** Em produção eu utilizei Parquet e Hudi. Hoje recomendaria Iceberg pelo suporte nativo a time-travel, evolução de schema sem rewrite de partições e compaction automática — crítico para uma tabela de CDC que acumula deletes e updates continuamente.

**Processamento distribuído com EMR/Glue.** Para volumes de 5M+ transações/mês, o DuckDB single-process não escala. A camada bronze → silver seria processada com PySpark no EMR ou Glue, mantendo o mesmo padrão de dedup por `ROW_NUMBER()` e normalização de tipos, mas executando em cluster com paralelismo por partição de data.

**Transformação silver → gold com dbt + Athena.** A camada de transformação (o equivalente ao `transform/silver.py` e `marts/`) seria modelada em dbt com o engine do Athena. Cada mart vira um modelo dbt com testes nativos de unicidade e not-null, e a linhagem fica documentada automaticamente no dbt docs. O BI consome apenas a camada gold — nunca bronze ou silver diretamente.

**Orquestração com Airflow.** Os 10 steps de `main.py` virariam tasks de um DAG no Airflow, com retry por step, SLA monitoring e suporte a dois modos de reprocessamento: reprocessamento do dia (só a partição afetada) e reprocessamento full (todas as datas em paralelo com concorrência controlada).

**Qualidade de dados com Great Expectations.** Os checks STOP/WARN do `quality/checks.py` são o embrião certo, mas em produção seriam evoluídos para Great Expectations com expectations versionadas por fonte, relatórios de Data Docs publicados automaticamente e integração com o Airflow para bloquear o DAG quando a suite falhar. O CSV do PaySettler teria uma suite própria com detecção de schema drift antes de chegar ao ingest.

**Secrets via AWS Secrets Manager.** `DATA_PATH`, credenciais de S3 e conexões de banco sairiam do `.env` e seriam injetados pelo orquestrador via Secrets Manager — rotação automática sem redeploy do pipeline.

---

### Ferramentas de IA utilizadas

**Claude Code (claude-sonnet-4-6) — uso extensivo em toda a implementação.**

O desafio foi desenvolvido em sessões de pair programming com o Claude Code CLI, com revisão humana explícita a cada fase antes de prosseguir. O processo foi estruturado em fases aprovadas sequencialmente:

- **Fase 0 — Fundação:** geração do `config.py`, `db.py`, `observability/logger.py`, DDLs SQL e fixtures de teste
- **Fase 1 — Ingestão:** leitores de parquet/CSV com `union_by_name`, validação de header, fallback de arquivo
- **Fase 2 — Transformação silver:** dedup CDC, normalização de amounts BRL, coerção de timestamps
- **Fase 3 — Qualidade:** checks STOP/WARN com `QualityError`, SQL expressions espelhadas em silver e checks
- **Fase 4 — Marts gold:** agregações por consumidor, `NULLIF` para match_rate seguro, dedup de PK em compliance
- **Fase 5 — Orquestrador:** `main.py` com 10 steps, `docker-compose.override.yml`, Makefile
- **Fase 6 — Testes:** 91 testes unitários e de integração (normalize, cdc, quality_checks, idempotency, e2e, marts)
- **Fase 7 — Documentação:** diagrama Mermaid, runbook de troubleshooting, guia de escalabilidade

**Como o Claude foi usado na prática:** proposta de estrutura de pastas antes de qualquer código, escrita de código revisado e aprovado antes de cada fase, diagnóstico de bugs (TIMESTAMPTZ/pytz sem pandas, PK violation em mart_compliance por dedup faltante, `module not found` por `pytest` vs `python -m pytest`), e redação da documentação de arquitetura.

O arquivo `CLAUDE.md` na raiz do repositório contém as instruções de contexto usadas durante o desenvolvimento, conforme encorajado pelo enunciado.

---

## Prazo

Você tem **3 a 4 dias** para completar o desafio.

Valorizamos uma solução **completa, limpa e bem documentada** mais do que rica em features. Se o tempo estiver curto, reduza escopo, não qualidade.

---

## Observações

- **Não inclua código malicioso no projeto.** Caso identificado, o projeto será desconsiderado.
- Após a entrega, faremos uma **conversa técnica de ~45 minutos** sobre sua implementação e decisões.

*Boa sorte! Qualquer dúvida sobre o enunciado, entre em contato antes de assumir premissas.*
