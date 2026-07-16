# 🏦 Settlement Reconciliation Platform

> Plataforma de dados para processamento, reconciliação financeira e disponibilização de indicadores para Operações, CFO e Compliance.

![Status](https://img.shields.io/badge/status-ativo-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![DuckDB](https://img.shields.io/badge/engine-DuckDB-yellow)

---

## 🚀 Como Rodar

```bash
docker compose up -d --build
# ou: make up
```

A aplicação executa, em sequência: Load Raw → Bronze → Silver → Gold → Diamond → Quality Checks → Dashboard (`http://localhost:8000`).

---

## 🧭 Premissas e Decisões

- **Medallion Architecture** (Raw → Bronze → Silver → Gold → Diamond) para separar auditoria, tratamento, modelagem e consumo.
- **DuckDB local + Parquet** como engine principal — volume do case não justifica cluster distribuído; Spark foi avaliado e descartado nesta fase (ver "Escalabilidade" abaixo).
- **Silver** aplica deduplicação (`ROW_NUMBER() PARTITION BY pk ORDER BY updated_at DESC`) e tipagem, entregando dado confiável para as camadas seguintes.
- **Gold** consome exclusivamente de **Silver** (nunca de Bronze), garantindo que o modelo dimensional só seja construído sobre dado já tratado.
- **Diamond** existe para não expor o modelo dimensional (Gold) diretamente ao consumo final — uma tabela por persona, com regras de negócio já aplicadas.
- **Dashboard web (`localhost:8000`) cobre apenas a persona CFO.** Ops e Compliance são atendidos pelas tabelas `diamond.operations_reconciliation_health` e `diamond.compliance_audit_history`, consumíveis via SQL direto — decisão consciente de escopo (tempo do case), não uma lacuna não percebida.
- **O pipeline lê o dataset completo a cada execução (full read), sem parametrização por `reference_date`.** Adequado ao volume de exemplo (~1k transações); em produção, com a fonte já particionada por data (como a Bronze já faz), o ideal seria parametrizar a leitura para D-1 ou uma data específica via CLI/config. Idempotência de **resultado** (rodar N vezes não duplica dados) está garantida pelo dedup na Silver; idempotência de **execução** (rodar N vezes não reprocessa tudo) é a lacuna deliberadamente não resolvida aqui.
- **Status `REVERSED` do CSV do PaySettler não é tratado como caso especial.** O pipeline reconcilia a transação normalmente, sem distinguir uma liquidação revertida de uma liquidação válida. Ambiguidade do enunciado não resolvida por falta de tempo — o correto seria sinalizar/anular reversões antes da reconciliação.
- **Moeda única assumida (BRL).** `cfo_daily_metrics` soma `internal_amount`/`processor_amount` sem `GROUP BY currency`. Se surgir mais de uma moeda nos dados, a métrica financeira fica incorreta.
- **Testabilidade: nenhum teste automatizado foi escrito.** Decisão consciente, não esquecimento — testes unitários "dummy" (ex.: mockar uma tabela DuckDB isolada) pareceram menos representativos do comportamento real do pipeline do que um teste de fato end-to-end (rodar o pipeline completo contra um fixture e validar contagens/regras nas camadas finais). Como o tempo do case não permitiu montar esse teste E2E com qualidade, optou-se por **não** entregar um teste que desse falsa sensação de cobertura. Em produção, o teste certo seria E2E: subir o pipeline contra um dataset fixo conhecido e validar contagens por camada, resultado do dedup, e as regras de negócio (`has_difference`, `is_reconciled`) contra valores esperados.
- **Qualidade de dados como principal rede de segurança do pipeline.** Como não há testes automatizados, a camada `app/quality` assume esse papel: qualquer desvio inesperado nos dados (linha nula onde não deveria, duplicidade, valor negativo, merchant órfão) é validado a cada execução e vira falha explícita — hoje todos os checks são bloqueantes (`FAIL` interrompe o pipeline), não há ainda a distinção entre check bloqueante e check apenas de alerta (`WARN`).

---

## 🏗️ Visão Geral da Arquitetura

```
Raw (arquivos) → Bronze (DuckDB, partição por dia) → Silver (dedup + tipagem)
→ Gold (dim/fato) → Diamond (visões por persona) → Dashboard (CFO) / SQL direto (Ops, Compliance)
```

- **Raw:** arquivos originais (Parquet/CSV), sem alteração — camada de auditoria.
- **Bronze:** primeira camada no DuckDB, com metadados de ingestão e partição por `created_at DATE`.
- **Silver:** deduplicação via `ROW_NUMBER()` e cast de tipos, uma tabela por fonte.
- **Gold:** modelo dimensional (`dim_merchant`, `fact_reconciliation`).
- **Diamond:** tabelas agregadas por persona (`cfo_daily_metrics`, `operations_reconciliation_health`, `compliance_audit_history`).
- **Consumo:** dashboard FastAPI para CFO; Ops e Compliance consultam as tabelas Diamond diretamente via SQL.

### Arquitetura em produção

Em produção, a mesma sequência de camadas se manteria, mas o ponto de entrada mudaria: em vez de arquivos lidos do filesystem local, a ingestão viria de um storage durável (ex.: S3) alimentado por um job agendado (orquestrador tipo Airflow), com o warehouse DuckDB local substituído por um engine que suporte múltiplos writers/consumidores concorrentes (ver "Escalabilidade"). O dashboard e as tabelas Diamond passariam a ser servidos a partir desse warehouse compartilhado, não de um arquivo `.duckdb` local ao container do pipeline.

### Troubleshooting (dashboards sem dados desde sexta)

Ordem de investigação, do mais rápido ao mais profundo:

1. **`quality.results`** — o pipeline rodou e algum check bloqueante falhou (`FAIL`), interrompendo a execução antes da Diamond?
2. **Logs do pipeline** (`silver.build`, `gold.build`, `diamond.build`) — timestamp da última execução bem-sucedida; comparar com o horário esperado do cron/scheduler.
3. **`reconciliation_runs`** — existe um run com `status = FAILED` ou travado em `IN_PROGRESS` desde sexta?
4. **Contagens por camada** (Bronze → Silver → Gold → Diamond) — em qual camada o volume zera ou para de crescer? Isso localiza exatamente onde o pipeline parou.
5. Só a partir daqui, se nada acima explicar, eu acionaria o time responsável pela fonte (o PaySettler pode ter simplesmente não enviado o arquivo de sexta) antes de assumir bug no pipeline.

### Escalabilidade (~5M transações/dia, ~1,8B/ano)

O ponto onde o desenho atual quebra primeiro **não é processamento analítico** — DuckDB lê Parquet em OLAP local com boa performance mesmo em volume alto. Quebra primeiro em:

- **Concorrência de escrita:** DuckDB não é multi-writer. Múltiplos processos/consumidores escrevendo no mesmo arquivo `.duckdb` ao mesmo tempo não é suportado — isso vira gargalo antes do volume de dados em si.
- **Full read a cada execução:** sem parametrização por `reference_date` (ver "Premissas"), reprocessar o histórico inteiro a 5M/dia se torna inviável em tempo de execução.
- **Arquivo monolítico:** um único `.duckdb` dificulta backup, DR e paralelização de leitura — dados particionados fisicamente em object storage (S3 + Parquet particionado) resolveriam isso sem necessariamente trocar de engine.
- Só nesse ponto (volume real de produção, múltiplos consumidores, necessidade de processamento distribuído) migrar para **Spark** ou reforçar com engines maiores (ex.: Databricks) se justificaria — não antes disso.

---

## 🥇 Camada Gold — Modelo Dimensional

### `gold.dim_merchant`

Dimensão de merchants, construída a partir de `silver.enterprise_company`.

| Coluna | Descrição |
|---|---|
| `merchant_key` | Chave substituta (surrogate key) |
| `merchant_id` | Código do merchant |
| `legal_name` / `trade_name` | Razão social / nome fantasia |
| `document` | CNPJ |
| `primary_cnae` | CNAE principal |

> ⚠️ **Limitação conhecida:** a tabela é recriada a cada execução (`CREATE OR REPLACE`), sem histórico de mudanças cadastrais (sem SCD Type 2). Se o cadastro de um merchant mudar, a versão anterior é perdida.

### `gold.fact_reconciliation`

Fato de reconciliação, construído a partir de `silver.reconciliation_results`.

| Coluna | Descrição |
|---|---|
| `reconciliation_result_id`, `run_id`, `transaction_id`, `merchant_id` | Chaves de rastreabilidade |
| `category` | `MATCHED`, `MISMATCHED`, `UNRECONCILED_PROCESSOR`, `UNRECONCILED_INTERNAL` |
| `internal_amount`, `processor_amount`, `difference` | Valores comparados |
| `is_reconciled` | `TRUE` somente quando `category = 'MATCHED'` |
| `has_difference` | `TRUE` quando `difference > 0`; `NULL` quando `difference IS NULL` (transação não reconciliada de um dos lados) |

---

## 💎 Camada Diamond — Tabelas por Persona

### `diamond.operations_reconciliation_health` — Operações

**Responde:**
- Qual a taxa de discrepância diária?
- Como a saúde da reconciliação evoluiu nos últimos N dias?

```sql
-- Taxa de discrepância nos últimos 7 dias
SELECT reference_date, discrepancy_rate
FROM diamond.operations_reconciliation_health
ORDER BY reference_date DESC
LIMIT 7;

-- Pior dia do mês em taxa de discrepância
SELECT reference_date, discrepancy_rate
FROM diamond.operations_reconciliation_health
WHERE reference_date >= date_trunc('month', current_date)
ORDER BY discrepancy_rate DESC
LIMIT 1;
```

**Ainda não responde:**
- Qual o tipo de discrepância predominante? (`MISMATCHED` vs. `UNRECONCILED_*` estão agregados em um único número — precisa de breakdown por categoria)
- Quais merchants concentram mais discrepâncias? (tabela não tem granularidade por merchant)
- A métrica considera runs com `status != COMPLETED`? (não há filtro por status do run hoje)

---

### `diamond.cfo_daily_metrics` — CFO

**Responde:**
- Qual o volume financeiro total liquidado em um período?
- Qual a diferença acumulada entre valor interno e valor do processador?

```sql
-- Volume liquidado na última semana
SELECT SUM(total_processor_amount) AS volume_semana
FROM diamond.cfo_daily_metrics
WHERE reference_date >= current_date - INTERVAL 7 DAY;

-- Diferença acumulada no mês corrente
SELECT SUM(total_difference) AS diferenca_mes
FROM diamond.cfo_daily_metrics
WHERE date_trunc('month', reference_date) = date_trunc('month', current_date);
```

**Ainda não responde:**
- Qual o volume por merchant ou por CNAE? (tabela é agregada só por dia, sem granularidade)
- Multi-moeda: a soma assume que só existe BRL — se surgir outra moeda, o total fica incorreto (sem `GROUP BY currency`)
- Comparação mês a mês (MoM) — precisa ser calculada via query adicional, não está pré-agregada

---

### `diamond.compliance_audit_history` — Compliance

**Responde:**
- Qual o caminho completo de uma transação específica, do run até o resultado?
- Quantas transações foram processadas em um run específico?

```sql
-- Auditoria pontual de uma transação
SELECT *
FROM diamond.compliance_audit_history
WHERE transaction_id = '<uuid>';

-- Total de transações por run
SELECT run_id, COUNT(*) AS total
FROM diamond.compliance_audit_history
GROUP BY run_id
ORDER BY run_id DESC;
```

**Ainda não responde:**
- Razão social / CNPJ do merchant envolvido (tabela não faz join com `gold.dim_merchant`)
- Histórico de mudanças de categoria entre reprocessamentos — como a tabela é recriada com `CREATE OR REPLACE`, uma transação que mudou de `UNRECONCILED_PROCESSOR` para `MATCHED` num reprocessamento não deixa rastro da versão anterior (requisito central de auditoria ainda não coberto)

---

## ✅ Data Quality

Checks isolados em `app/quality`, resultado consolidado em `quality.results`. Cobre: integridade (`transaction_id` não nulo), duplicidade, valores financeiros positivos, e existência de merchant válido.

**Toda mudança inesperada nos dados vira erro direto.** Como não há suíte de testes automatizados cobrindo o código, os quality checks são a principal rede de proteção do pipeline: qualquer check que falhe (`FAIL`) interrompe a execução, evitando que dado incorreto chegue às camadas Gold/Diamond e, dali, ao dashboard ou às consultas de Ops/Compliance. Hoje todos os checks têm o mesmo peso (bloqueante); a evolução natural seria diferenciar checks estruturais (ex.: `transaction_id` nulo → sempre bloqueia) de checks de negócio mais sensíveis a variação natural (ex.: taxa de `MISMATCHED` um pouco acima do normal → deveria alertar, não necessariamente parar o pipeline).

---

## 🔌 Extensibilidade — Plugar uma 2ª Fonte de Liquidação

Assumindo uma nova fonte com schema parecido (mas não idêntico) ao do PaySettler, o caminho concreto seria:

1. **`app/raw/`** — novo loader (ex.: `load_raw_nova_fonte.py`) para ler o arquivo bruto da nova fonte e depositá-lo em Raw, sem transformação.
2. **`app/bronze/build.py`** — adicionar o mapeamento da nova fonte para uma tabela Bronze própria (`bronze.nova_fonte_settlements`), com os mesmos metadados de ingestão (`ingestion_timestamp`, `date_partition`).
3. **`app/silver/build.py`** — criar (ou parametrizar) a lógica de dedup/tipagem para o schema da nova fonte, convergindo para o **mesmo contrato canônico** já usado por `silver.reconciliation_results` (mesmas colunas de saída, mesmo grão), mesmo que os nomes de origem sejam diferentes.
4. **`app/gold/build.py`** — `gold.fact_reconciliation` passaria a receber um `UNION` (ou fonte adicional identificada por uma coluna `source_processor`) das duas origens já normalizadas pela Silver.
5. **`app/config.py`** — adicionar a nova fonte à configuração (nome do arquivo, schema esperado).
6. **`app/quality/checks.py`** — os checks já existentes (`transaction_id` não nulo, `amount > 0`, etc.) rodam automaticamente contra a nova fonte, desde que ela converja para o schema canônico na Silver; não precisam ser reescritos.

**Resumo:** ~4 arquivos tocados (`raw`, `bronze`, `silver`, `gold`) + 1 config, sem necessidade de reescrever Diamond ou Quality — desde que a nova fonte seja normalizada para o mesmo contrato na Silver. O ponto de atenção real é justamente a Silver: é onde a heterogeneidade de schema entre fontes precisa ser resolvida antes de chegar em Gold.

---

## 🚧 Limites do Desenho

O que a arquitetura **deliberadamente não suporta hoje**, mas uma versão de produção precisaria:

1. **Sem SCD Type 2 em `dim_merchant`.** Mudanças cadastrais (razão social, CNAE) sobrescrevem o registro anterior — uma transação reconciliada em janeiro, consultada em junho, mostraria o cadastro atual do merchant, não o vigente na época da reconciliação. Problema real de correção histórica para Compliance.
2. **Sem histórico em `compliance_audit_history`.** A tabela é reconstruída via `CREATE OR REPLACE` a cada execução — se uma transação mudar de categoria entre reprocessamentos, a versão anterior não fica registrada em lugar nenhum. Para uma persona cujo requisito central é "histórico para auditorias", essa é a lacuna mais séria do desenho atual.
3. **DuckDB não suporta múltiplos writers concorrentes.** O desenho atual assume um único processo escrevendo no `.duckdb` por vez — não há estratégia de concorrência para múltiplos pipelines/consumidores escrevendo simultaneamente, algo que se torna necessário assim que mais de uma fonte ou mais de um consumidor de escrita entrar em cena.

---

## 🔮 O Que Faria Diferente em Produção

**O que foi simplificado para o case:**
- Warehouse em arquivo único local (`.duckdb`) em vez de um storage compartilhado/distribuído.
- Leitura full a cada execução em vez de parametrizada por `reference_date`.
- Ausência de orquestrador (Airflow) — o pipeline roda como um script sequencial único via `docker compose up`.
- Sem testes automatizados (ver justificativa em "Premissas e Decisões").
- Sem autenticação no dashboard.

**O que a versão de produção precisaria:**
- Object storage (S3) + engine com suporte a múltiplos writers/consumidores no lugar do arquivo `.duckdb` local.
- Orquestração real (Airflow ou similar) com retries, alertas e parametrização por data.
- Suíte de testes E2E rodando contra um fixture fixo, validando contagens e regras de negócio por camada.
- Diferenciação entre quality checks bloqueantes e checks de alerta (hoje todos são bloqueantes).
- SCD Type 2 em `dim_merchant` e histórico append-only em `compliance_audit_history`.
- Autenticação e controle de acesso no dashboard, já que ele expõe dados financeiros sensíveis.

---

## 🔭 Próximas Evoluções

- [ ] Breakdown de discrepância por tipo (`MISMATCHED` vs. `UNRECONCILED_*`) em `operations_reconciliation_health`
- [ ] SCD Type 2 em `dim_merchant` e histórico append-only em `compliance_audit_history`
- [ ] Suporte multi-moeda em `cfo_daily_metrics`
- [ ] Distinção entre quality checks bloqueantes e checks de alerta
- [ ] Suíte de testes E2E contra fixture fixo
- [ ] Airflow, dbt, CI/CD

> **Nota sobre escala:** para o volume tratado neste case (até ~5M transações/mês), não faz sentido introduzir uma arquitetura distribuída — DuckDB local resolve com folga e mantém a solução simples de operar e depurar. Se o volume crescer de forma significativa e o processamento local deixar de performar bem, aí sim vale considerar Spark (para processamento distribuído em cluster) ou Pandas (para pipelines menores que ainda cabem em memória, mas exigem transformações mais flexíveis) como próximo passo, dependendo de qual gargalo aparecer primeiro — volume ou complexidade de transformação.

---

## 🤖 Ferramentas de IA Utilizadas

A implementação do pipeline, do modelo de dados e dos testes foi feita **sem uso de IA para geração de código**. O **Claude (versão free)** foi utilizado apenas para apoio na **redação e formatação da documentação** (este README).

Vale registrar uma limitação prática do ambiente de desenvolvimento: a máquina pessoal usada para o case é **antiga, com apenas 8GB de RAM, rodando Windows**, o que restringe o uso de ferramentas mais pesadas do mercado (containers múltiplos simultâneos, engines distribuídas, IDEs com IA integrada consumindo memória adicional, etc.). Isso influenciou diretamente a escolha por uma stack local leve (DuckDB + Parquet). Em um ambiente **AWS ou Databricks**, boa parte dessas restrições de recurso deixaria de ser um fator limitante — processamento, escala e ferramentas de IA embarcadas passariam a ser resolvidos de forma bem mais simples e direta, sem depender do hardware local.

---

<p align="center"><sub>Documentação técnica — Settlement Reconciliation Platform</sub></p>