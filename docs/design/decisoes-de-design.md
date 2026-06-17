# Design — Settlement Reconciliation Analytics

Notas de design do desafio: o que construímos e por quê. As decisões abaixo foram tomadas
inspecionando os dados reais em `docs/sample-data/`. O que ficou efetivamente entregue está
no [`SOLUTION.md`](../../SOLUTION.md); aqui fica o raciocínio por trás.

## Contexto

A fintech opera o Settlement Reconciliation Service: todo dia recebe um CSV do processador
PaySettler, compara com os registros internos, categoriza cada transação e grava o resultado
no PostgreSQL `settlement_db`. O serviço funciona, mas os dados só vivem no banco
transacional — Operações, CFO e Compliance não têm acesso analítico. A ideia é transformar
isso em produtos de dados para os três times.

## Decisões principais

| # | Ambiguidade | Decisão |
|---|---|---|
| D1 | Refazer a reconciliação ou consumir o resultado pronto? | Consome `reconciliation_results`. O serviço já reconcilia; nosso produto é analytics sobre o resultado, não o matching. |
| D2 | Qual o papel do CSV do PaySettler, se o resultado já existe? | É o feed canônico de liquidação: traz `settled_at` e o status SETTLED/REVERSED (que não existem em `reconciliation_results`), serve de cross-check de qualidade e é o ponto de extensão para um 2º processador. |
| D3 | Como tratar o CDC (`Op`/`_timestamp`)? | Dedup para a última versão por chave natural e exclui registros cujo último `Op` é `D`. Os dados reais têm update e delete. |
| D4 | Schema drift entre os lotes (`payment_method` só no 2º)? | Leitura com `union_by_name=true` (unifica por nome; coluna ausente vira NULL). |
| D5 | Histórico de atributos do merchant? | `dim_merchant` modelada como SCD Type 2 (colunas `valid_from`/`valid_to`/`is_current` e join as-was no fato de auditoria). Hoje a silver consome a última versão de cada merchant, então a dimensão guarda uma versão (a corrente) por merchant — reter histórico real é o passo que falta (ver Limites). A estrutura já está pronta para isso. |
| D6 | Granularidade temporal? | Batch diário por `reference_date`, com janela de reprocessamento de 7 dias (a janela de reconciliação do domínio). |
| D7 | Tolerância de valor? | `difference <= R$ 0,01` conta como casado (regra do glossário). |
| D8 | Run upstream com status FAILED (ex.: 2025-03-22)? | A data não é processada — não geramos analytics de uma reconciliação que não completou; fica sinalizado na tabela de controle. |

## Arquitetura

Medalhão (bronze → silver → gold) + adapters por fonte, orquestrado por uma CLI Python,
tudo num único arquivo DuckDB (`warehouse.duckdb`).

- **Bronze** — ingestão crua de cada fonte, do jeito que veio, preservando as colunas de CDC
  (`Op`/`_timestamp`). Nada é limpo aqui; serve de landing auditável.
- **Silver** — conformação: dedup de CDC (última versão, exclui deletes), `union_by_name`
  para o schema drift, normalização (vírgula decimal, timezone, caixa do `merchant_id`),
  os checks de qualidade, e o mapeamento do feed de liquidação para o schema canônico via
  adapter.
- **Gold** — star schema (fatos + dimensões) + um mart por consumidor.

```
python -m pipeline.cli run --reference-date 2025-03-16         # processa 1 dia (+ janela de 7 dias)
python -m pipeline.cli run --reference-date 2025-03-16 --full  # reprocessa todas as datas completas
python -m pipeline.cli status                                  # lê a tabela de controle
```

## Modelo dimensional (gold)

Star schema no estilo Kimball: 3 fatos, 3 dimensões e 3 marts, ligados por dimensões
conformadas — Ops, CFO e Compliance batem entre si.

**Fatos**

- `fact_reconciliation_result` — transaction fact, grão de 1 linha por `reconciliation_result`.
  É a trilha de auditoria imutável (Compliance). Liga ao merchant por um join as-was sobre
  `dim_merchant` (compara a data com `valid_from`/`valid_to`); como a dimensão guarda só a
  versão corrente hoje, na prática resolve para a versão atual — a mecânica as-was já fica
  pronta para quando houver histórico. `transaction_id`, `reconciliation_run_id` e
  `processor_reference` entram como dimensões degeneradas.
- `fact_reconciliation_daily` — periodic snapshot, grão de `reference_date` × merchant × moeda,
  colapsando por (transação, run) com a pior severidade. Taxas de discrepância (Operações).
- `fact_settlement_daily` — periodic snapshot, grão de `settled_at` × merchant × moeda, vindo
  do feed de liquidação (1 linha por transação, sem fan-out entre datas). Volume liquidado,
  revertido, líquido e ticket médio (CFO).

**Dimensões**

- `dim_merchant` — modelada como SCD Type 2 (chave surrogate determinística;
  `valid_from`/`valid_to`/`is_current`), guardando `legal_name`, `trade_name`, CNPJ e
  `primary_cnae`. Hoje guarda uma versão por merchant (a silver deduplica para a última);
  reter histórico real é o que falta — ver Limites.
- `dim_date` — role-playing, usada como `reference_date` e `settlement_date`.
- `dim_transaction_profile` — junk (categoria, dentro-da-tolerância, status de liquidação,
  flags de presença), só com as combinações que de fato ocorrem.

**Marts (um por consumidor)**

- `mart_ops_reconciliation_health` (Operações) — taxa de discrepância por dia, contagens por
  categoria, runs que falharam.
- `mart_cfo_settled_volume` (CFO) — volume liquidado por dia × moeda × setor (CNAE), valor
  revertido, líquido e ticket médio.
- `mart_compliance_audit_trail` (Compliance) — trilha completa por transação: merchant/CNPJ,
  categoria, valores interno/processador, a run e a `processor_reference` (referência do
  processador).

## Perguntas que o modelo responde (e as que não)

- **Operações** — responde: a taxa de discrepância subiu nos últimos dias? quantas ficaram
  sem conciliar, por categoria? alguma run falhou? Não responde: visão intradiária / tempo
  real — é batch diário.
- **CFO** — responde: volume liquidado por dia e moeda? ticket médio por setor? quanto foi
  revertido no período? Não responde: receita e taxas (P&L) — não há dado de fee nas fontes,
  só volume liquidado.
- **Compliance** — responde: histórico completo de uma transação? quais ficaram unreconciled?
  qual CNPJ teve mais mismatches? Não responde: relatório regulatório formatado para órgão —
  regras externas, fora do escopo.

Exemplos de queries em `docs/queries/`.

## Qualidade de dados — três camadas

- **FIX** (conserta na silver) — dado recuperável, normaliza. Ex.: vírgula decimal,
  `settled_at` sem `Z`, `merchant_id` em caixa errada.
- **STOP** (circuit breaker) — ameaça à integridade: aborta a run **antes de gerar os fatos**
  e sinaliza. Ex.: `transaction_id` nulo; run upstream com status FAILED.
- **QUARANTINE** (isola e segue) — linha ruim mas que não corrompe, vai para uma tabela de
  quarentena com o motivo. Ex.: resultado com `run_id` órfão, `merchant_id` nulo, estorno sem
  par interno, moeda divergente entre interno e processador.

Uma checagem de queda brusca de volume ficou como roadmap — não foi codada (exigiria um
baseline histórico que um único dia de CSV de exemplo não permite validar).

## Idempotência

Reprocessa a janela `[reference_date - 7d, reference_date]` substituindo cada partição (nunca
acumula). A troca é atômica por partição, dentro de uma transação:

```sql
BEGIN;
  DELETE FROM fato WHERE reference_date = :d;
  INSERT INTO fato SELECT ...;
COMMIT;   -- crash no meio = rollback; a partição nunca fica pela metade
```

Rodar a mesma data duas vezes dá contagens idênticas (tem teste automatizado). A atomicidade é
por partição, não da execução inteira.

## Observabilidade

Cada execução grava uma linha em `control_pipeline_run`: `RUNNING` no início, `SUCCESS` ou
`FAILED` no fim, com linhas lidas/escritas/rejeitadas, timestamps e mensagem de erro. Cada
partição de fato é gravada de forma transacional (atomicidade por partição); o status é
atualizado ao fim da execução. É o primeiro artefato a consultar num troubleshooting.

## Testes

O pytest cobre as transformações puras (dedup de CDC incluindo delete, normalização de
valor/timezone, tolerância, categorização), um teste por check de qualidade, e a idempotência
(roda 2× a mesma data e compara as contagens).

## Extensibilidade — plugar um 2º processador

O schema canônico de liquidação (saída de todo adapter) é:
`transaction_id, merchant_id, settled_amount, currency, settled_at, processor_reference,
status, source_name`.

Cada processador novo é 1 adapter (`adapters/<nome>.py`, lê o arquivo bruto e devolve o
canônico) mais 1 entrada em `config.py` (glob do arquivo, mapa de colunas, separador decimal,
timezone). Silver e Gold não mudam, porque trabalham só sobre o schema canônico. Na prática:
2 arquivos tocados, mais os testes do novo adapter.

## Limites deliberados

1. **Sem backfill de chegada tardia além da janela de 7 dias.** Uma reconciliação de uma data
   fora da janela não é reprocessada sem `--full`; não há um tratamento dedicado de chegada
   tardia. Produção precisaria de um replay/backfill sob demanda.
2. **Não é a fonte de verdade da categorização.** Consome `reconciliation_results` do
   upstream; se o matching do serviço estiver errado, o erro se propaga. Produção desacoplaria
   isso com testes de contrato contra o upstream.
3. **DuckDB de um único processo escritor.** Não suporta vários processos escrevendo nem
   serving concorrente de muitos usuários de BI no mesmo arquivo. Ao escalar, o que aperta
   primeiro é a concorrência, não o volume.
4. **Histórico de merchant não retido.** A dimensão e o join são SCD Type 2, mas a silver
   guarda só a última versão de cada merchant — então hoje há uma versão por merchant. Para
   auditoria "as-was" de verdade, a silver precisaria preservar as versões anteriores.

Arquitetura de produção, troubleshooting e escalabilidade estão na Parte 3 do
[`SOLUTION.md`](../../SOLUTION.md).

## Números do fixture (conferidos, não assumidos)

- `transactions`: 1060 linhas brutas em 2 lotes (com `Op` I/U/D e o drift de `payment_method`);
  dedup para a última versão e excluindo deletes → 995 ativas.
- `reconciliation_results`: 837 (MATCHED 726, MISMATCHED 62, UNRECONCILED_PROCESSOR 28,
  UNRECONCILED_INTERNAL 21).
- `reconciliation_runs`: 10 runs, uma delas FAILED (2025-03-22), aproximadamente a cada 3 dias.
- `enterprise_company`: 70 linhas / 50 merchants distintos (CDC: 50 inserts + 20 updates).
- `settlement_paysettler.csv`: 510 linhas de um único dia (2025-03-16); SETTLED 483 /
  REVERSED 27; 22 com vírgula decimal; 49 sem `Z` no timestamp.
