# Design — Settlement Reconciliation Analytics

> Documento de design (spec) do desafio de Engenharia de Dados.
> Data: 2026-06-14 · Stack: Python 3.12 + DuckDB 1.5 + Docker Compose.
> Este documento registra **o que** vamos construir e **por quê** — incluindo as
> ambiguidades encontradas e as decisões tomadas.

> ⚠️ **Nota (as-built):** este é o design PRÉ-implementação. Durante a construção, a
> inspeção dos dados reais e uma revisão de código levaram a evoluções — ver
> [`SOLUTION.md`](../../SOLUTION.md) como referência do que foi efetivamente entregue.
> Principais deltas: **3 fatos** em vez de 2 (`fact_reconciliation_daily` por `reference_date`
> p/ Ops + `fact_settlement_daily` por `settled_at` p/ CFO, evitando fan-out cross-date);
> chaves surrogate **determinísticas**; **quarentena** de results órfãos/nulos (D12);
> merchant **SCD2 as-was** no fato de auditoria; atomicidade **por-partição** (não run-wide).

---

## 1. Contexto e missão

A fintech opera o **Settlement Reconciliation Service**: diariamente recebe um CSV do
processador externo **PaySettler**, compara com os registros internos, categoriza cada
transação e persiste o resultado no PostgreSQL `settlement_db`. O serviço funciona, mas
**os dados só vivem no banco transacional** — Operações, CFO e Compliance não têm acesso
analítico.

**Missão:** transformar esses dados em **produtos de dados** consumíveis pelos três times.

---

## 2. Premissas e decisões (as ambiguidades que resolvemos)

Todas as premissas abaixo foram **validadas inspecionando os dados reais** em
`docs/sample-data/` (ver Apêndice A com os números).

| # | Ambiguidade | Decisão | Justificativa |
|---|---|---|---|
| D1 | O pipeline **refaz** a reconciliação ou **consome** o resultado pronto? | **Consome** `reconciliation_results`. Não reimplementa o matching. | O enunciado diz que o serviço já reconcilia e persiste. Nosso produto é analytics sobre o resultado, não a reconciliação. |
| D2 | Qual o papel do CSV do PaySettler, se o resultado já existe? | É o **feed canônico de liquidação**: enriquece com `settled_at` e `status` (SETTLED/REVERSED), serve de **cross-check de qualidade** (orphans, valores) e é o **ponto de extensão** para um 2º processador. | `reconciliation_results` não tem `settled_at` nem o status de reversão — só o CSV tem. |
| D3 | Como tratar o CDC (`Op`/`_timestamp`)? | Dedup para a **última versão por chave natural** e **excluir registros cujo último `Op` = `D`**. | Os dados reais têm `U` (update) e `D` (delete), embora o gerador do repo só emita `I`. Projetar pela verdade dos dados. |
| D4 | Schema drift entre `batch_1` e `batch_2` (coluna `payment_method`)? | Ler com `union_by_name=true` (unifica por nome; ausente vira NULL). | Mecanismo nativo do DuckDB para evolução de schema. |
| D5 | Histórico de atributos do merchant (SCD)? | **SCD Type 2** em `dim_merchant` (atributos `legal_name`, `primary_cnae`). | Compliance precisa de auditoria "as-was": uma transação liquidada sob a razão social antiga deve continuar ligada a ela. Kimball chama Type 2 de "o tipo de mudança mais importante". |
| D6 | Granularidade temporal | **Batch diário** por `reference_date`, com **janela deslizante de 7 dias** de reprocessamento. | A janela de reconciliação do domínio é `[reference_date - 7d, reference_date]`. |
| D7 | Tolerância de valor | `difference <= R$ 0,01` ⇒ dentro da tolerância. | Regra do glossário. |
| D8 | Run upstream com status `FAILED` (ex.: 2025-03-22) | A data correspondente **não é processada** (alerta + marca no controle); não geramos analytics de uma reconciliação que não completou. | Não propagar resultado parcial de uma run incompleta. |

---

## 3. Arquitetura da solução (medalhão + adapters)

Camadas com responsabilidade única, orquestradas por uma CLI Python. Tudo roda num único
arquivo DuckDB (`warehouse.duckdb`).

```
Fontes (docs/sample-data/)                 Camadas DuckDB                Consumidores
┌───────────────────────────┐
│ transactions_batch_*.parquet │ (CDC)   ┌──────────┐   ┌──────────┐   ┌─────────────────────┐
│ reconciliation_results.parquet│ (CDC)  │  BRONZE  │   │  SILVER  │   │  GOLD (star schema) │
│ reconciliation_runs.parquet │ (CDC) ─▶ │ ingestão │─▶ │ limpeza  │─▶ │ fatos + dims + marts│─▶ Ops / CFO / Compliance
│ enterprise_company.parquet  │ (CDC)    │ crua     │   │ +CDC dedup│  │                     │
│ settlement_paysettler.csv   │ (raw) ──▶│ (as-is)  │   │ +DQ +canon│  └─────────────────────┘
└───────────────────────────┘           └──────────┘   └──────────┘
        │                                                     ▲
        └── adapter (PaySettler → schema canônico) ───────────┘
```

- **Bronze** — ingestão crua de cada fonte, *as-is*, preservando `Op`/`_timestamp`. Nada é limpo aqui. Serve de "raw landing" auditável.
- **Silver** — conformação: CDC dedup (última versão, exclui `D`), `union_by_name` para o drift, normalização (vírgula decimal, timezone, caixa do `merchant_id`), aplicação dos **checks de qualidade**, e mapeamento do feed de liquidação para o **schema canônico** via adapter. Saída: entidades limpas.
- **Gold** — modelo dimensional (star schema) + marts por consumidor.

### 3.1 CLI

```bash
python -m pipeline run --reference-date 2025-03-16        # processa 1 dia (+ janela de 7d)
python -m pipeline run --reference-date 2025-03-16 --full # reprocessa tudo
python -m pipeline status                                  # lê a tabela de controle
```

### 3.2 Estrutura de módulos

```
pipeline/
  __init__.py
  cli.py            # entrypoint argparse: run / status
  config.py         # registro de fontes (PaySettler e futuros processadores)
  db.py             # conexão DuckDB, helpers de transação
  control.py        # tabela de controle (watermark/observabilidade)
  quality.py        # checks de DQ (fix / stop / quarantine)
  adapters/
    base.py         # interface SettlementAdapter -> schema canônico
    paysettler.py   # adapter do CSV do PaySettler
  bronze.py         # ingestão crua
  silver.py         # CDC dedup, normalização, canônico
  gold.py           # fatos, dimensões, marts
sql/                # modelos SQL versionados (1 arquivo por tabela gold)
tests/
docs/
  design/           # este documento
  sample-data/      # fixtures
```

---

## 4. Modelo dimensional (camada Gold)

Star schema Kimball: **2 fatos + 3 dimensões + 3 marts**, ligados por **dimensões conformadas**
(bus architecture) — Ops, CFO e Compliance batem entre si (single version of truth).

### 4.1 Fatos

**`fact_reconciliation_result`** — *transaction fact* · grão = **1 linha por transação por run**
Append-only, imutável. É a **trilha de auditoria** (Compliance).

| Coluna | Papel |
|---|---|
| `transaction_id` | dimensão **degenerada** (id sem atributos) |
| `reconciliation_run_id` | dimensão degenerada |
| `processor_reference` | dimensão degenerada (do feed) |
| `merchant_key` | FK → `dim_merchant` (versão SCD2 válida na data — "as-was") |
| `reference_date_key` | FK → `dim_date` |
| `settlement_date_key` | FK → `dim_date` (role-playing; do `settled_at`, nullable) |
| `transaction_profile_key` | FK → `dim_transaction_profile` (junk) |
| `internal_amount`, `processor_amount`, `settled_amount`, `difference` | medidas |
| `is_matched`, `is_mismatched`, `is_unreconciled` | flags (1/0) |
| `loaded_at` | metadado |

**`fact_settlement_daily`** — *periodic snapshot* · grão = **1 linha por merchant × moeda × dia**
Linhas **densas** (todo dia ativo tem linha) → tendências sem buracos. Agregada a partir do
fato atômico. Serve **Operações** (taxas) **e CFO** (volume) com uma tabela só.

| Coluna | Papel |
|---|---|
| `reference_date_key` | FK → `dim_date` |
| `merchant_key` | FK → `dim_merchant` (chave durável p/ roll-up atual) |
| `currency` | grão |
| `total_transactions`, `matched_count`, `mismatched_count`, `unreconciled_processor_count`, `unreconciled_internal_count` | medidas |
| `discrepancy_rate` | (mismatched + unreconciled) / total |
| `total_settled_amount`, `total_reversed_amount`, `net_settled_amount` | medidas (CFO) |
| `total_difference`, `avg_ticket` | medidas |

### 4.2 Dimensões

- **`dim_merchant`** (**SCD Type 2**): `merchant_key` (surrogate), `merchant_id` (chave natural/durável), `legal_name`, `trade_name`, `document` (CNPJ), `primary_cnae`, `cnae_sector` (derivado), `valid_from`, `valid_to`, `is_current`.
- **`dim_date`** (conformada, **role-playing** como `reference_date` e `settlement_date`): `date_key` (YYYYMMDD), `date`, `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week`, `is_weekend`, `iso_week`.
- **`dim_transaction_profile`** (**junk**): `profile_key`, `category` (MATCHED/MISMATCHED/UNRECONCILED_PROCESSOR/UNRECONCILED_INTERNAL), `within_tolerance` (bool), `settlement_status` (SETTLED/REVERSED/N/A), `has_internal_record`, `has_processor_record` — só as combinações que ocorrem (não o produto cartesiano).

### 4.3 Marts (1 por consumidor)

| Mart | Consumidor | Fonte | Entrega |
|---|---|---|---|
| `mart_ops_reconciliation_health` | Operações | `fact_settlement_daily` + `control_pipeline_run` | taxa de discrepância por dia, contagens por categoria, runs que falharam/demoraram |
| `mart_cfo_settled_volume` | CFO | `fact_settlement_daily` | volume liquidado por dia × moeda × merchant/setor, valor revertido, ticket médio |
| `mart_compliance_audit_trail` | Compliance | `fact_reconciliation_result` + `dim_merchant` (as-was) | trilha completa por transação (merchant/CNPJ, categoria, valores, run, arquivo, `settled_at`) |

---

## 5. Perguntas que o modelo responde (e as que deliberadamente não)

**Operações** ✅ "A taxa de discrepância subiu nos últimos 7/30 dias?" · "Quantas transações ficaram sem conciliar ontem, por categoria?" · "Alguma run falhou ou demorou demais?"
❌ *Não:* visão intradiária / tempo real — é batch diário.

**CFO** ✅ "Volume liquidado por dia e por moeda?" · "Ticket médio por merchant/setor (CNAE)?" · "Quanto foi revertido (REVERSED) no período?"
❌ *Não:* receita/taxas e P&L — **não há dado de fee nas fontes**, só volume liquidado.

**Compliance** ✅ "Histórico completo da transação X?" · "Quais transações ficaram unreconciled no período?" · "Qual CNPJ teve mais mismatches?"
❌ *Não:* relatório regulatório formatado para órgão (regras externas fora do escopo).

Exemplos de queries em `docs/queries/` (a entregar na Parte 1).

---

## 6. Pipeline — mecânica detalhada

### 6.1 CDC dedup (silver)
```sql
-- última versão de cada chave natural, excluindo deletes
SELECT * EXCLUDE (rn)
FROM (
  SELECT *, row_number() OVER (PARTITION BY transaction_id
                               ORDER BY _timestamp DESC, id DESC) AS rn
  FROM read_parquet(['…batch_1.parquet','…batch_2.parquet'], union_by_name=true)
)
WHERE rn = 1 AND Op <> 'D';
```

### 6.2 Qualidade de dados — política em 3 camadas

| Camada | Gatilho | Ação | Exemplos reais no fixture |
|---|---|---|---|
| **FIX** (conserta na silver) | dado recuperável | normaliza | vírgula decimal (22), `settled_at` sem `Z` (49), `merchant_id` em caixa errada |
| **STOP** (circuit breaker) | ameaça integridade | aborta a run (transação reverte) + alerta | `transaction_id` nulo; PK duplicada após dedup; queda de volume vs `runs.total_transactions` além de X%; run upstream `FAILED` (ex.: 2025-03-22); valor não parseável |
| **QUARANTINE** (isola + alerta) | linha ruim mas não corrompe | grava em `quarantine_settlement` com motivo, segue | orphan `REVERSED` sem par interno; liquidação tardia (fora da janela de 7d); `merchant_id` nulo; moeda divergente entre interno e processador |

Leitura tolerante do CSV: `read_csv(..., decimal_separator=',', nullstr=['',' '], store_rejects=true)`.

### 6.3 Idempotência
- Reprocessa a **janela `[ref_date-7d, ref_date]`** substituindo cada partição (nunca acumula).
- Padrão **atômico** por partição, num **único arquivo `.duckdb`** (ATTACH cross-database **não** é atômico):
  ```sql
  BEGIN;
    DELETE FROM fact_x WHERE reference_date = :d;
    INSERT INTO fact_x SELECT … ;
    UPDATE control_pipeline_run SET status='SUCCESS', rows_written=… WHERE run_id=:r;
  COMMIT;   -- crash no meio = rollback; partição nunca fica pela metade
  ```
- Rodar 2× a mesma data ⇒ contagens idênticas (teste automatizado garante).

### 6.4 Observabilidade — `control_pipeline_run`
`run_id, reference_date, source_name, status (PENDING/RUNNING/SUCCESS/FAILED), rows_read, rows_written, rows_rejected, started_at, finished_at, error_message, code_version`.
Atualizada **na mesma transação** que escreve os dados → o status nunca diverge do dado.
É a primeira fonte que se olha no troubleshooting (Parte 3.2).

### 6.5 Testabilidade (pytest)
- **Unit** nas transformações puras: CDC dedup (inclui delete), normalização de valor/timezone, tolerância R$0,01, categorização.
- **1 teste por check de DQ** (entra linha suja → comportamento esperado: fix/stop/quarantine).
- **Golden/contract test**: roda no fixture pequeno e compara a saída com snapshot esperado.
- **Teste de idempotência**: roda 2× a mesma data → contagens iguais, sem duplicação.

---

## 7. Extensibilidade — plugar um 2º processador

Schema **canônico de liquidação** (saída de todo adapter):
`transaction_id, merchant_id, settled_amount, currency, settled_at (UTC), processor_reference, status, source_name, reference_date`.

Cada processador = **1 adapter** (`adapters/<nome>.py`, implementa `SettlementAdapter`:
lê o arquivo bruto → devolve o canônico) + **1 entrada em `config.py`** (glob do arquivo,
mapa de colunas, formato decimal/tz, regras específicas).

> **Resposta concreta à pergunta do PR:** plugar um 2º processador toca **2 arquivos**
> (`adapters/<novo>.py` + 1 bloco em `config.py`) e adiciona testes do adapter.
> **Silver e Gold não mudam**, porque operam sobre o schema canônico. Nenhum módulo de
> transformação a jusante é tocado.

---

## 8. Limites deliberados do desenho (o que produção precisaria)

1. **Sem backfill de chegada tardia além da janela de 7 dias.** Uma liquidação que chega com 10 dias de atraso cai na quarentena e **não** atualiza a partição já "fechada" — produção precisaria de um processo de replay/backfill sob demanda com reabertura controlada de partição.
2. **Não é fonte de verdade da categorização.** Consome `reconciliation_results` do upstream; se a lógica de matching do serviço estiver errada, o erro se propaga. Produção desacoplaria isso com testes de contrato contra o upstream.
3. **DuckDB de um único processo escritor.** Não suporta múltiplos processos escrevendo nem serving concorrente de muitos usuários de BI no mesmo arquivo — o que quebra primeiro ao escalar não é o volume, é a concorrência (ver Parte 3.3).
4. **Sem reconciliação intradiária / streaming.** É batch diário; um sinal que precise de latência de minutos (ex.: fraude) não é atendido.

---

## 9. Parte 3 — Arquitetura de produção (esboço, a detalhar)

**3.1 Desenho** — Debezium lê o WAL do Postgres → Kafka (Avro + schema registry) →
Kafka Connect S3 sink grava parquet particionado (bronze) → tabela aberta **Apache Iceberg**
→ transformações (este pipeline / dbt) → DW/BI. Orquestração **Dagster** (asset-aware),
linhagem via **OpenLineage**. O core de transformação local (DuckDB) é o mesmo que roda no
nó de produção — trocável.

**3.2 Troubleshooting** ("dashboards sem dados desde sexta") — ordem de investigação, do
sintoma à origem: (1) `control_pipeline_run` — última run `SUCCESS`? alguma `FAILED`/`RUNNING`
travada? (2) histórico do orquestrador — a run de sexta rodou? (3) logs por `run_id`;
(4) frescor das fontes; (5) só então CDC/Debezium — lag do slot via
`pg_replication_slots` (distância de LSN). **Não** assumir CDC antes de checar o orquestrador.

**3.3 Escalabilidade (→ 5M/dia ≈ 1,8B linhas/ano)** — o gargalo **não é volume** (cabe num
nó; DuckDB faz out-of-core e já rodou TPC-H em escala de TB). Quebra primeiro em
**concorrência** (1 processo escritor) e **serving multiusuário**. Caminho: particionar por
data; separar ingestão de serving; migrar o armazenamento de `.duckdb` único para **Iceberg**
+ engine distribuída (Spark/Trino) ou DW (BigQuery/Snowflake/ClickHouse) **quando a
concorrência/multiusuário exigir** — mantendo DuckDB para dev/embedded.

---

## Apêndice A — Números reais do fixture (provados, não assumidos)

| Fonte | Linhas | Observações |
|---|---|---|
| `transactions_batch_1` | 742 | todas `Op=I` |
| `transactions_batch_2` | 318 | `Op`: I=259, U=53, **D=6** (CDC real) + coluna `payment_method` (drift) |
| união (dedup→latest) | 1000 distintas | latest op: I=947, U=48, **D=5 (excluir)** ⇒ 995 ativas; 60 linhas duplicadas no bruto |
| `reconciliation_results` | 837 | MATCHED 726, MISMATCHED 62, UNRECONCILED_PROCESSOR 28 (internal NULL), UNRECONCILED_INTERNAL 21 (processor NULL) |
| `reconciliation_runs` | 19 linhas / 10 runs | CDC I+U; **1 FAILED** (2025-03-22); runs a cada 3 dias (03-04→03-31) |
| `enterprise_company` | 70 | merchants |
| `settlement_paysettler.csv` | 510 | **1 dia (2025-03-16)**; SETTLED 483 / REVERSED 27; 22 vírgula-decimal; 49 sem `Z` |

## Apêndice B — Alavancas técnicas verificadas (e onde NÃO exagerar)

- **Idempotência:** DuckDB **1.4 LTS+** tem `MERGE INTO` (upsert sem PK). Instalado aqui: **1.5.3**.
- **CDC dedup:** `QUALIFY row_number() OVER (PARTITION BY chave ORDER BY _timestamp DESC) = 1` + filtrar `Op <> 'D'`.
- **Schema drift:** `read_parquet(glob, union_by_name=true)` (unifica por nome; ausente → NULL).
- **CSV sujo:** `read_csv(..., decimal_separator=',', nullstr=[...], store_rejects=true)`.
- ⚠️ **Não** citar "número mágico de GB" para sair do DuckDB — defender o gatilho **qualitativo** (concorrência / multi-writer / multiusuário). O que quebra primeiro **não é volume**.
- ⚠️ Dizer **"processo"** escritor único (dentro do processo há multithread via MVCC).
- ⚠️ `delete+insert` só é atômico no DuckDB **se** envolvido em `BEGIN; … COMMIT;`.
- ⚠️ SCD/idempotência: layouts são **síntese** de técnicas Kimball (não há case Kimball de reconciliação publicado); as **técnicas** (tipos de fato, SCD, degenerada/junk, bus) são fundamentadas.
```
