# Modelo de Dados Analitico — Camada Gold

Este documento descreve o modelo analitico da plataforma de conciliacao de liquidacoes, implementado na camada Gold do data warehouse (DuckDB). Cada tabela Gold foi projetada para atender um consumidor especifico — Operacoes, CFO ou Compliance — com granularidade, particionamento e colunas deliberateamente escolhidos para responder perguntas de negocio bem definidas.

---

## Arquitetura Geral

```
Bronze (raw + _ingested_at, _source_file, _reference_date)
  │
  ├─ silver.transactions            (internas deduplicadas por transaction_id, CDC latest-wins)
  ├─ silver.reconciliation_results  (transacoes categorizadas com reference_date)
  ├─ silver.paysettler_settlements  (liquidacoes PaySettler limpas)
  ├─ silver.merchants               (enterprise_company deduplicada por merchant_id, CDC latest-wins)
  └─ silver.reconciliation_runs     (execucoes de conciliacao deduplicadas por id, CDC latest-wins)
       │
       └─ Gold (6 tabelas orientadas a consumidor)
            ├─ Operacoes:  gold.daily_reconciliation_health
            ├─ CFO:        gold.daily_financial_volume
            │              gold.merchant_volume_summary
            └─ Compliance: gold.audit_discrepancies
                           gold.audit_unreconciled
                           gold.audit_reconciliation_runs_history
```

### Tabela de controle

- `control.pipeline_runs` — registro imutavel de cada execucao do pipeline (run_id, reference_date, status, metricas de qualidade). Nao e uma tabela analytica; serve como auditoria interna do processo de ETL.

---

## Decisoes de Modelagem Transversais

### Granularidade

A granularidade de cada tabela Gold e definida pela combinacao minimal de colunas que garante unicidade de cada linha. Isso e explicitado na secao de cada tabela. Nenhuma tabela Gold contém duplicatas por design — a deduplicacao ocorre na camada Silver (CDC latest-wins via `_timestamp DESC` + `ROW_NUMBER`).

### Particionamento

O armazem utiliza DuckDB em arquivo unico (`data/warehouse.duckdb`). O DuckDB nao suporta particionamento fisico de tabelas no mesmo sentido que um lakehouse (Parquet particionado por diretorio). Em troca, adotamos:

- **Filtro natural por `reference_date`** — todas as tabelas Gold que possuem data de negocio sao ordenadas (`ORDER BY reference_date`) e as consultas devem filtrar por essa coluna para acessar o minimo de paginas possivel.
- **Ordenacao fisica** — as tabelas sao criadas com `CREATE OR REPLACE TABLE ... ORDER BY`, garantindo que dados da mesma data fiquem localizados proximos no arquivo.
- Se no futuro o armazem migrar para Parquet particionado em S3/ADLS, `reference_date` sera a chave de particao natural para todas as tabelas exceto `merchant_volume_summary`.

### Chaves surrogate

A plataforma opera com identificadores de negocio que ja sao estaveis:

- `transaction_id` (UUID) — identificador nativo de transacao, compartilhado entre sistema interno e PaySettler.
- `merchant_id` (string, formato `MERCH_NNNN`) — identificador nativo do estabelecimento.
- `run_id` (inteiro) — identificador da execucao de conciliacao.

**Nao introduzimos chaves surrogate artificiais** (e.g., sequencias numericas) nas tabelas Gold porque:

1. Os identificadores de negocio ja sao unicos e imutaveis.
2. Consumers consultam diretamente por `merchant_id`, `transaction_id` ou `reference_date`; um SK adicionaria uma camada de indirecao sem ganho de performance no DuckDB.
3. A tabela `merchant_volume_summary` usa `merchant_id` comoPK natural — a granularidade e um merchant por linha, que ja e univocamente identificado.

### Janela temporal de conciliacao

A conciliacao compara o arquivo do processador PaySettler com transacoes internas criadas em uma janela de **7 dias** ancorada na `reference_date`: `[reference_date - 7 dias, reference_date]` (inclusivo). Isso significa que uma transacao interna pode ser conciliada com um arquivo de liquidacao de ate 7 dias depois da sua criacao.

### Tolerancia de divergencia

Diferencas de ate **R$ 0,01** (um centavo) entre valor interno e valor do processador sao consideradas `MATCHED`, nao `MISMATCHED`. Isso reflete arredondamentos inerentes ao processamento de pagamentos. O parametro `mismatch_tolerance` esta centralizado em `PipelineConfig`.

---

## Tabelas Gold — Detalhamento

---

### 1. `gold.daily_reconciliation_health`

**Consumidor:** Operacoes

**Granularidade:** Uma linha por `reference_date` (uma data de conciliacao por linha).

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `reference_date` | VARCHAR | Data de referencia da conciliacao (YYYY-MM-DD) |
| `total_results` | BIGINT | Total de transacoes conciliadas na data |
| `matched_count` | BIGINT | Transacoes com category = MATCHED |
| `mismatched_count` | BIGINT | Transacoes com category = MISMATCHED |
| `unreconciled_processor_count` | BIGINT | Transacoes presentes no PaySettler mas ausentes internamente |
| `unreconciled_internal_count` | BIGINT | Transacoes presentes internamente mas ausentes no PaySettler |
| `discrepancy_rate` | DOUBLE | `mismatched_count / total_results` (0.0 a 1.0) |
| `run_status` | VARCHAR | Status da execucao de conciliacao (COMPLETED) |
| `processing_time_minutes` | DOUBLE | Duracao da execucao em minutos |
| `started_at` | TIMESTAMPTZ | Inicio da execucao |
| `completed_at` | TIMESTAMPTZ | Fim da execucao |

**Perguntas que responde:**

- Qual a taxa de discrepancia por dia de conciliacao?
- Quantas transacoes ficaram sem conciliacao (por lado) em cada data?
- O tempo de processamento do run esta dentro do SLA?
- A execucao do dia foram concluida com sucesso?
- Ha tendencias de piora na taxa de discrepancia ao longo dos dias?

**Perguntas que NAO responde (deliberadamente):**

- Quais transacoes especificas estao mismatched? — Tabela `audit_discrepancies`.
- Qual o volume financeiro por dia? — Tabela `daily_financial_volume`.
- Quais merchants sao os mais problematicos? — Tabela `merchant_volume_summary`.
- Historico de runs com status FAILED? — A tabela filtra `WHERE status = 'COMPLETED'` no join com `silver.reconciliation_runs`. Runs que falharam aparecem em `audit_reconciliation_runs_history`, nao aqui.

**Decisoes de design:**

- O LEFT JOIN com `silver.reconciliation_runs` (filtrando COMPLETED) garante que dias sem run concluido ainda aparecem com metricas de resultado, mas com colunas de execucao nulas. Isso evita que um run falho oculte os dados de resultado ja calculados.
- `discrepancy_rate` e calculada como proporcao de mismatched sobre o total, nao como proporcao de nao-matched. Isso isola o sinal de erro de valor do sinal de ausencia de contraparte.
- Nao particionamos por merchant nesta tabela porque o consumidor (Operacoes) precisa de uma visao consolidada por dia, nao por merchant.

---

### 2. `gold.daily_financial_volume`

**Consumidor:** CFO

**Granularidade:** Uma linha por `reference_date` (volume diario agregado de todas as transacoes conciliadas).

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `reference_date` | VARCHAR | Data de referencia da conciliacao |
| `internal_volume_brl` | DOUBLE | Soma de `internal_amount` (valor registrado internamente) |
| `processor_volume_brl` | DOUBLE | Soma de `processor_amount` (valor liquidado pelo PaySettler) |
| `daily_difference` | DOUBLE | `internal_volume_brl - processor_volume_brl` (diferenca liquida do dia) |
| `total_abs_difference` | DOUBLE | Soma de `difference` (soma das discrepancias absolutas por transacao) |
| `transaction_count` | BIGINT | Numero total de transacoes conciliadas na data |

**Perguntas que responde:**

- Qual o volume financeiro diario (interno vs. processador)?
- Qual a diferenca liquida diaria entre o que registramos e o que o PaySettler liquidou?
- Qual o impacto financeiro absoluto das discrepancias por dia?
- Ha tendencias de desvio acumulado ao longo do tempo?

**Perguntas que NAO responde (deliberadamente):**

- Qual merchant contribui mais para a diferenca? — Tabela `merchant_volume_summary`.
- Detalhe transacao-a-transacao das discrepancias? — Tabela `audit_discrepancies`.
- Volume por moeda diferente de BRL? — A tabela agrega tudo em BRL sem conversao. Cerca de 95% das transacoes sao BRL (ver `CURRENCY_MIX` no gerador); as 4-5% restantes (USD, EUR) sao somadas como-estao. Essa e uma limitacao deliberada: o pipeline nao implementa taxas de cambio, e misturar moedas no mesmo total e aceitavel apenas porque a parcela nao-BRL e residual. Se o cenario mudar, uma evolucao seria adicionar colunas por moeda ou uma tabela de conversao.

**Decisoes de design:**

- `COALESCE(internal_amount, 0)` e `COALESCE(processor_amount, 0)` tratam os nulos das categorias UNRECONCILED, onde apenas um lado tem valor. Sem isso, a soma seria NULL para qualquer data que contivesse unreconciled.
- `daily_difference` (soma interna - soma processador) e diferente de `total_abs_difference` (soma das diferencas absolutas por transacao). A primeira indica desvio direcional (sistematico?); a segunda indica ruido agregado (pode cancelar em liquido mas ser risco alto).
- A granularidade e por dia, nao por (dia, merchant), porque o CFO precisa da visao consolidada. Drill-down por merchant fica na tabela `merchant_volume_summary`.

---

### 3. `gold.merchant_volume_summary`

**Consumidor:** CFO

**Granularidade:** Uma linha por `merchant_id` (volume acumulado de todo o historico por merchant).

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `merchant_id` | VARCHAR | Identificador do merchant (PK natural) |
| `legal_name` | VARCHAR | Razao social (da silver.merchants) |
| `trade_name` | VARCHAR | Nome fantasia |
| `document` | VARCHAR | CNPJ do merchant |
| `total_transactions` | BIGINT | Total de transacoes conciliadas (todas as categorias) |
| `total_internal_volume` | DOUBLE | Soma de internal_amount |
| `total_processor_volume` | DOUBLE | Soma de processor_amount |
| `total_difference` | DOUBLE | Soma de difference |
| `mismatched_count` | BIGINT | Quantidade de transacoes MISMATCHED |

**Perguntas que responde:**

- Quais sao os merchants com maior volume financeiro (top-N)?
- Quais merchants tem as maiores diferencas acumuladas entre interno e processador?
- Qual a proporcao de mismatched por merchant?
- Qual merchant deveria ser priorizado para investigacao de discrepancias?

**Perguntas que NAO responde (deliberadamente):**

- Volume por dia por merchant? — A tabela e acumulada (cross-date). Para analise temporal por merchant, cruze `silver.reconciliation_results` com `silver.merchants` filtrando `reference_date`.
- Detalhe transacao-a-transacao? — Tabela `audit_discrepancies`.
- Merchants que nunca aparecem em conciliacao? — A tabela inclui apenas merchants presentes em `reconciliation_results`. Merchants cadastrados mas sem transacao nao apareceriam. Se necessario, use `silver.merchants LEFT JOIN silver.reconciliation_results`.
- Evolucao temporal do mismatched_count? — Nao particionada por data. Use `audit_discrepancies` filtrada por merchant.

**Decisoes de design:**

- **Denormalizacao com merchants:** Os campos `legal_name`, `trade_name`, `document` sao trazidos via LEFT JOIN com `silver.merchants`. Isso evita que o consumidor CFO precise fazer um join adicional — a tabela e self-contained para relatorio. A denormalizacao e aceitavel porque a dimensao merchant e small-cardinality (centenas de merchants) e muda raramente.
- **Ordenacao por volume:** `ORDER BY total_internal_volume DESC` coloca os maiores merchants primeiro, alinhado com o caso de uso de top-N.
- **Acumulacao cross-date:** A tabela nao quebra por `reference_date`. Isso e deliberado: o CFO quer saber "qual merchant acumulou mais discrepancia no historico", nao "qual merchant teve mais discrepancia no dia X". Se o cenario mudar, a expansao natural e uma granularity (merchant_id, reference_date) — porem isso triplicaria o numero de linhas e prejudicaria a simplicidade de consumo.

---

### 4. `gold.audit_discrepancies`

**Consumidor:** Compliance

**Granularidade:** Uma linha por `transaction_id` onde `category = 'MISMATCHED'`.

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `reference_date` | VARCHAR | Data de referencia da conciliacao |
| `transaction_id` | VARCHAR | UUID da transacao |
| `merchant_id` | VARCHAR | Identificador do merchant |
| `legal_name` | VARCHAR | Razao social |
| `trade_name` | VARCHAR | Nome fantasia |
| `document` | VARCHAR | CNPJ |
| `internal_amount` | DOUBLE | Valor registrado internamente |
| `processor_amount` | DOUBLE | Valor liquidado pelo PaySettler |
| `difference` | DOUBLE | Diferenca absoluta entre os lados |
| `created_at` | TIMESTAMPTZ | Data/hora de criacao da transacao |

**Perguntas que responde:**

- Quais transacoes tiveram discrepancia de valor entre os dois lados?
- Qual o valor de cada lado (interno vs. processador) para cada discrepancia?
- Qual merchant teve a discrepancia de maior valor?
- Em quais datas as discrepancias ocorreram?
- Qual o detalhe completo (com dados cadastrais) para investigacao de cada caso?

**Perguntas que NAO responde (deliberadamente):**

- Transacoes que nao foram conciliadas (unreconciled)? — Tabela `audit_unreconciled`.
- Resumo agregado por dia? — Tabela `daily_reconciliation_health`.
- Historico de execucoes do pipeline? — Tabela `audit_reconciliation_runs_history`.
- Transacoes com diferenca dentro da tolerancia (<= R$ 0,01)? — Essas sao `MATCHED` e nao aparecem aqui.

**Decisoes de design:**

- **Filtro por MISMATCHED:** A tabela contem apenas transacoes com `category = 'MISMATCHED'`, isto e, transacoes presentes em ambos os lados com diferenca acima de R$ 0,01. Isso e o que o time de Compliance precisa investigar — cada linha e um caso aberto.
- **Denormalizacao com merchants:** Pela mesma razao que `merchant_volume_summary`: a tabela e self-contained para o consumidor Compliance, que precisa do CNPJ e razao social para abrir um caso.
- **Ordenacao por `difference DESC`:** As discrepancias de maior impacto financeiro aparecem primeiro.
- **`difference` e absoluto:** O campo `difference` vem de `ABS(internal_amount - processor_amount)` (calculado na conciliacao). Para saber a direcao (interno > processador ou vice-versa), compare `internal_amount` e `processor_amount` diretamente.

---

### 5. `gold.audit_unreconciled`

**Consumidor:** Compliance

**Granularidade:** Uma linha por `transaction_id` onde `category IN ('UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')`.

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `reference_date` | VARCHAR | Data de referencia da conciliacao |
| `transaction_id` | VARCHAR | UUID da transacao |
| `merchant_id` | VARCHAR | Identificador do merchant |
| `legal_name` | VARCHAR | Razao social |
| `trade_name` | VARCHAR | Nome fantasia |
| `document` | VARCHAR | CNPJ |
| `category` | VARCHAR | `UNRECONCILED_PROCESSOR` ou `UNRECONCILED_INTERNAL` |
| `amount` | DOUBLE | Valor disponivel (interno ou processador, o que existir) |
| `created_at` | TIMESTAMPTZ | Data/hora de criacao da transacao |

**Perguntas que responde:**

- Quais transacoes estao sem contraparte na conciliacao?
- A transacao falta no lado interno (registrada pelo PaySettler mas nao por nos) ou no lado do processador?
- Qual o valor em risco por transacoes nao conciliadas?
- Qual merchant tem mais transacoes nao conciliadas?

**Perguntas que NAO responde (deliberadamente):**

- Transacoes com discrepancia de valor (mismatched)? — Tabela `audit_discrepancies`.
- Resumo diario de quantidades? — Tabela `daily_reconciliation_health`.
- Motivo pelo qual a transacao nao foi conciliada? — O pipeline classifica a ausencia (PROCESSOR vs. INTERNAL) mas nao determina a causa raiz. Isso requer investigacao manual.

**Decisoes de design:**

- **Campo `amount` unificado:** Para UNRECONCILED, apenas um lado tem valor. Em vez de trazer `internal_amount` e `processor_amount` (onde um seria NULL), a tabela expoe `COALESCE(internal_amount, processor_amount) AS amount`. Isso simplifica o consumo e e semanticamente correto: so ha um valor conhecido.
- **Categoria retida:** A coluna `category` distingue `UNRECONCILED_PROCESSOR` (transacao so no PaySettler) de `UNRECONCILED_INTERNAL` (transacao so no sistema interno). Essa distincao e fundamental para a investigacao: o primeiro sugere falha de registro interno; o segundo sugere falha de liquidacao.
- **Ordenacao por (reference_date, category):** Facilita a leitura cronologica com agrupamento por tipo de unreconciled.
- **UnreconciledProcessor vs. UnreconciledInternal no contexto da janela:** Uma transacao `UNRECONCILED_INTERNAL` pode ser conciliada em um run futuro se o PaySettler liquidar posteriormente (dentro da janela de 7 dias). Ja uma `UNRECONCILED_PROCESSOR` indica que o PaySettler reportou algo que nao existe nos nossos registros — potencialmente mais grave.

---

### 6. `gold.audit_reconciliation_runs_history`

**Consumidor:** Compliance

**Granularidade:** Uma linha por `run_id` (uma execucao de conciliacao por linha).

**Colunas:**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `run_id` | BIGINT | Identificador da execucao |
| `reference_date` | VARCHAR | Data de referencia |
| `file_name` | VARCHAR | Nome do arquivo PaySettler processado |
| `status` | VARCHAR | Status do run (IN_PROGRESS, COMPLETED, FAILED) |
| `total_transactions` | BIGINT | Total de transacoes no arquivo |
| `started_at` | TIMESTAMPTZ | Inicio da execucao |
| `completed_at` | TIMESTAMPTZ | Fim da execucao |
| `processing_time_minutes` | DOUBLE | Duracao em minutos |

**Perguntas que responde:**

- Quantas execucoes de conciliacao foram realizadas?
- Quais runs falharam ou ficaram em progresso?
- Qual o tempo de processamento por run? Ha outliers?
- Qual arquivo foi processado em cada run?
- Houve reprocessamento de uma mesma data?

**Perguntas que NAO responde (deliberadamente):**

- Resultados detalhados de cada run (quantos matched, mismatched etc.)? — Tabela `daily_reconciliation_health` (para runs COMPLETED).
- Detalhe por transacao? — Tabelas `audit_discrepancies` e `audit_unreconciled`.
- Metricas de qualidade do pipeline (HARD/SOFT checks)? — Tabela `control.pipeline_runs`.

**Decisoes de design:**

- **Tabela de historico puro:** Diferente de `daily_reconciliation_health`, esta tabela inclui runs com status `IN_PROGRESS` e `FAILED`, nao apenas `COMPLETED`. Isso e essencial para auditoria: Compliance precisa saber se houve tentativas fracassadas.
- **Sem join com resultados:** A tabela nao agrega metricas de resultado (matched, mismatched etc.) porque: (a) essas metricas ja estao em `daily_reconciliation_health`; (b) runs que falharam nao teriam resultados, e o left join introduziria nulos que poluiriam a leitura.
- **`processing_time_minutes` recalculado:** O campo e derivado de `EXTRACT(EPOCH FROM (completed_at - started_at)) / 60.0`. Runs em `IN_PROGRESS` terao `completed_at` NULL e portanto `processing_time_minutes` NULL — o que e semanticamente correto.
- **Ordenacao por `reference_date`:** Facilita leitura cronologica.

---

## Mapa de Perguntas x Tabelas

| Pergunta | Tabela Gold |
|----------|-------------|
| Taxa de discrepancia por dia? | `daily_reconciliation_health` |
| Transacoes sem contraparte por dia? | `daily_reconciliation_health` |
| Tempo de processamento do run? | `daily_reconciliation_health` / `audit_reconciliation_runs_history` |
| Volume financeiro diario? | `daily_financial_volume` |
| Diferenca liquida diaria? | `daily_financial_volume` |
| Impacto absoluto das discrepancias por dia? | `daily_financial_volume` |
| Top merchants por volume? | `merchant_volume_summary` |
| Top merchants por discrepancia? | `merchant_volume_summary` |
| Detalhe de cada discrepancia? | `audit_discrepancies` |
| Detalhe de cada unreconciled? | `audit_unreconciled` |
| Historico de runs (incluindo falhas)? | `audit_reconciliation_runs_history` |
| Runs com status FAILED? | `audit_reconciliation_runs_history` |
| Volume por merchant por dia? | *Nenhuma tabela Gold* — cruzar silver diretamente |
| Motivo raiz de unreconciled? | *Nenhuma tabela Gold* — investigacao manual |
| Conversao de moeda? | *Nenhuma tabela Gold* — nao implementado |

---

## Linhas de Evolucao

1. **Particionamento por `reference_date`** — Ao migrar para um lakehouse (Parquet em S3/ADLS), todas as tabelas exceto `merchant_volume_summary` serao particionadas por `reference_date`. A `merchant_volume_summary` poderia ser particionada por faixa de volume ou redistribuida para (merchant_id, reference_date).

2. **Conversao de moeda** — Se a parcela nao-BRL crescer, adicionar uma tabela de taxas de cambio e derivar `volume_brl` a partir de conversao na `daily_financial_volume`.

3. **Granularidade (merchant, date)** — `merchant_volume_summary` poderia ser expandida para (merchant_id, reference_date) se o CFO precisar de analise temporal por merchant. Isso tornaria a tabela ~90x maior (um mes de dados) mas ainda perfeitamente viavel no DuckDB.

4. **SCD Type 2 para merchants** — Hoje `silver.merchants` usa CDC latest-wins (SCD Type 1). Se Compliance necessitar do historico de razao social/CNPJ, seria necessario evoluir para SCD Type 2 com validade (valid_from, valid_to) e propagar a versao correta para as tabelas Gold de auditoria.
