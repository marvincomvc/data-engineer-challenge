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

---

### Como rodar

```bash
# Subir o ambiente
make up
# ou: docker compose up -d --build

# Rodar o pipeline para uma data específica | dados default (docs/sample-data/)
make run REF_DATE=2025-03-16
# ou diretamente:
docker compose exec pipeline python -m pipeline.cli run --reference-date 2025-03-16 --force

# Verificar status
make status REF_DATE=2025-03-16

# Com sample de dados gerados (10k linhas)
make generate
make run REF_DATE=2025-03-16 DATA=/tmp/generated

# Com dados grandes (1M linhas)
make generate-large
make run REF_DATE=2025-03-16 DATA=/tmp/generated-large

# Sem DATA=, usa o default (/data/input = docs/sample-data/)
make run REF_DATE=2025-03-16

# Rodar os testes
make test

# Rodar queries no DuckDB
# Com o arquivo padrão (queries/example.sql)
make query

# Com outro arquivo SQL
make query SQL=queries/my_query.sql


```

O pipeline gera o arquivo `data/warehouse.duckdb` com as camadas bronze, silver e gold, além da tabela de controle `control.pipeline_runs`.

**Pré-requisitos:** Docker e Docker Compose. Todo processamento roda dentro do container — não é necessário instalar Python ou DuckDB localmente.

---

### Premissas e decisões

1. **Arquitetura Medallion com DuckDB:** Escolhi separar as camadas (bronze/silver/gold) usando schemas do DuckDB dentro de um único arquivo persistente (`warehouse.duckdb`). Isso simplifica o deploy local e mantém claro o progresso dos dados.

2. **Deduplicação por CDC timestamp:** Os dados fonte vêm no formato CDC (com `Op` e `_timestamp`). Faço dedup mantendo o registro com `_timestamp` mais recente por chave natural (ex: `transaction_id`). Isso reflete como um CDC real funciona em produção.

3. **Reconciliação como parte da camada Silver:** A lógica de categorização (MATCHED, MISMATCHED, etc.) roda no silver porque é uma transformação de negócio que precisa dos dados limpos. Os resultados ficam na mesma tabela `silver.reconciliation_results` com a coluna `reference_date` para idempotência.

4. **Checks de qualidade SOFT vs HARD:** O check de `duplicate_transaction_id_in_run` é SOFT (não bloqueante) porque dados CDC da fonte já podem conter duplicatas legítimas (intencionalmente plantadas pelo gerador). Checks HARD (null transaction_id, CSV vazio, amount negativo) bloqueiam o pipeline.

5. **Normalização no CSV reader:** O PaySettler CSV pode conter sujeira (vírgula decimal, falta de 'Z' no timestamp, símbolos de moeda, case inconsistente). Toda normalização acontece antes de entrar no bronze.

6. **Schema drift coalesced:** Batch 1 e 2 têm colunas diferentes (payment_method só no batch 2). Uso UNION ALL com NULL para colunas ausentes, preservando todos os dados.

7. **Idempotência via DELETE+INSERT:** Para a reference_date sendo reprocessada, a reconcialição deleta os resultados existentes antes de re-inserir. O controle de runs registra cada execução separadamente.

---

### Visão geral da arquitetura

```
┌─────────────────────┐     ┌──────────────────────┐
│  Parquet (CDC)      │     │  CSV (PaySettler)    │
│  transactions (×2)  │     │  settlement_YYYY.csv  │
│  reconciliation_*   │     └──────────┬───────────┘
│  enterprise_company │                │
└─────────┬───────────┘                │
          │                            │
          ▼                            ▼
┌─────────────────────────────────────────────────────┐
│                  BRONZE LAYER                        │
│  Raw data + metadata (_ingested_at, _source_file)   │
│  No business rules applied                          │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                  SILVER LAYER                        │
│  Deduplicated, normalized, typed                     │
│  CDC dedup by _timestamp                             │
│  Schema drift resolved (coalesce NULLs)             │
│  Reconciliation logic (categorization)               │
│  Quality checks (HARD/SOFT)                         │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                  GOLD LAYER                          │
│  ┌─────────────────────────────────────────────┐    │
│  │ daily_reconciliation_health (Operations)    │    │
│  │ daily_financial_volume (CFO)                │    │
│  │ merchant_volume_summary (CFO)               │    │
│  │ audit_discrepancies (Compliance)            │    │
│  │ audit_unreconciled (Compliance)             │    │
│  │ audit_reconciliation_runs_history (Compl.)  │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│              CONTROL LAYER                           │
│  pipeline_runs — tracks every execution              │
└─────────────────────────────────────────────────────┘
```

O pipeline é acionado via CLI: `python -m pipeline.cli run --reference-date YYYY-MM-DD`. Tudo roda sobre DuckDB (sem pandas como motor de processamento). Consulte [`docs/architecture/architecture.md`](docs/architecture/architecture.md) para o diagrama detalhado.

---

### Extensibilidade

Se uma segunda fonte de liquidação (ex.: "QuickSettle") precisasse ser adicionada com schema parecido mas não idêntico:

1. **`pipeline/ingest/csv_reader.py`** — adicionar um novo reader (ou generalizar o existente com configuração de schema). ~50 linhas.
2. **`pipeline/transform/bronze.py`** — adicionar `load_quicksettle()`. ~15 linhas.
3. **`pipeline/transform/silver.py`** — adicionar `build_silver_quicksettle_settlements()` e ajustar `reconcile()` para JOIN com múltiplas fontes. ~30 linhas.
4. **`pipeline/config.py`** — adicionar caminho para o novo CSV. ~5 linhas.
5. **`tests/`** — adicionar testes para o novo reader e a silver transform. ~80 linhas.

**Total: ~4-5 arquivos, ~180 linhas.** A lógica gold e as queries analíticas não mudam — elas consomem da silver.Já a reconciliação precisaria ser estendida para considerar múltiplas fontes de processador (hoje faz JOIN apenas com PaySettler).

---

### Limites do desenho

1. **Sem particionamento temporal no DuckDB:** As tabelas silver/gold usam `CREATE OR REPLACE TABLE`, o que significa que a cada execução o warehouse inteiro é reprocessado. Em escala, isso não funciona — precisaria de particionamento por data (porquet files particionados + views no DuckDB).

2. **Sem concorrência:** O pipeline usa um único arquivo DuckDB e não suporta execuções paralelas para datas diferentes. Não há locking ou coordenação — múltiplos workers escreveriam no mesmo arquivo e corromperiam dados.

3. **Tabelas são totalmente recriadas:** As tabelas usam `CREATE OR REPLACE`, o que significa que todo o histórico é recalculado a cada run. Isso viola o requisito de imutabilidade do Compliance em produção — precisaria ser append-only com datas de validade (SCD Type 2 ou event-sourced).

---

### O que faria diferente em produção

1. **Armazenamento distribuído:** Trocaria o DuckDB local por um data lake (S3 + Iceberg/Delta) com um warehouse como Trino ou Spark para consultas. O DuckDB serviria apenas como cache local ou para desenvolvimento.

2. **Orquestração:** Adicionaria um orquestrador (Airflow/Prefect) para agendar execuções diárias, gerenciar dependências, retries e alertas. O CLI atual seria o operador chamado pelo orquestrador.

3. **CDC real:** Em vez de ler arquivos Parquet de export batch, usaria Debezium + Kafka para capturar mudanças do PostgreSQL em tempo real. Isso eliminaria a latência de 24h e permitiria reconciliação intraday.

4. **Monitoramento:** Adicionaria métricas Prometheus/Grafana para contagens de categorias, duração das runs, taxa de falhas dos checks, e alertas automáticos (ex.: envio de mensagem em canal do Slack/Teams quando a run falha ou a discrepancy rate > threshold).

5. **Schema registry:** Para evitar acoplamento frágil com o schema dos Parquet/CSV, usaria um schema registry (ou pelo menos validação com Pydantic/Great Expectations) na ingestão.

---

### Ferramentas de IA utilizadas

- **Claude Code (Anthropic)** — Utilizado como par de programação para:
  - Geração da estrutura do projeto e boilerplate do pipeline
  - Implementação das camadas bronze/silver/gold com DuckDB
  - Criação da suíte de testes (conftest, unit tests, integration tests)
  - Documentação (data model, architecture, runbook, scalability plan)
  - O arquivo `claude.md` contém as instruções completas usadas como prompt

---

## Prazo

Você tem **3 a 4 dias** para completar o desafio.

Valorizamos uma solução **completa, limpa e bem documentada** mais do que rica em features. Se o tempo estiver curto, reduza escopo, não qualidade.

---

## Observações

- **Não inclua código malicioso no projeto.** Caso identificado, o projeto será desconsiderado.
- Após a entrega, faremos uma **conversa técnica de ~45 minutos** sobre sua implementação e decisões.

*Boa sorte! Qualquer dúvida sobre o enunciado, entre em contato antes de assumir premissas.*
