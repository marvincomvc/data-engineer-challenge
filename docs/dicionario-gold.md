# Dicionário de Dados — Camada Gold

## 1. Objetivo

A camada Gold disponibiliza dados organizados para análises de negócio, acompanhamento financeiro, avaliação dos resultados de conciliação e auditoria.

Neste projeto, a camada possui três tabelas:

- `gold_operacoes_conciliacao`: indicadores operacionais das execuções de conciliação.
- `gold_cfo_volume_financeiro`: indicadores de volume financeiro por data de referência, moeda e categoria.
- `gold_compliance_auditoria`: dados detalhados de conciliação e execução para rastreabilidade e auditoria.

A documentação descreve as colunas conforme o schema identificado no DuckDB. As regras exatas de cálculo devem ser confirmadas no código que gera as tabelas Gold.

## 2. Visão geral das tabelas

| Tabela | Finalidade |
|---|---|
| `gold_operacoes_conciliacao` | Apresenta indicadores operacionais e financeiros agrupados por execução, categoria e moedas. |
| `gold_cfo_volume_financeiro` | Apresenta volumes financeiros agregados por data de referência, moedas e categoria da conciliação. |
| `gold_compliance_auditoria` | Disponibiliza registros detalhados de resultados e execuções, incluindo metadados de CDC e ingestão. |

## 3. Dicionário por tabela

### 3.1 `gold_operacoes_conciliacao`

Tabela analítica voltada ao acompanhamento operacional das conciliações. Permite avaliar quantidades de resultados, diferenças financeiras e valores totais por execução.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `reference_date` | VARCHAR | Sim | Data de referência de negócio da conciliação. |
| `run_id` | BIGINT | Sim | Identificador da execução de conciliação. |
| `file_name` | VARCHAR | Sim | Nome do arquivo associado à execução. |
| `status_execucao` | VARCHAR | Sim | Situação da execução de conciliação. |
| `category` | VARCHAR | Sim | Categoria do resultado da conciliação. |
| `moeda_interna` | VARCHAR | Sim | Código da moeda associado aos valores internos. |
| `moeda_externa` | VARCHAR | Sim | Código da moeda associado aos valores do processador externo. |
| `quantidade_resultados` | BIGINT | Sim | Quantidade de resultados considerados no agrupamento. |
| `quantidade_sem_diferenca` | BIGINT | Sim | Quantidade de resultados classificados sem diferença, conforme a regra implementada. |
| `quantidade_diferenca_ate_001` | BIGINT | Sim | Quantidade de resultados com diferença de até 0,01, conforme a regra implementada. |
| `quantidade_diferenca_acima_001` | BIGINT | Sim | Quantidade de resultados com diferença superior a 0,01, conforme a regra implementada. |
| `valor_interno_total` | DOUBLE | Sim | Soma dos valores internos considerados no agrupamento. |
| `valor_externo_total` | DOUBLE | Sim | Soma dos valores externos considerados no agrupamento. |
| `diferenca_absoluta_total` | DOUBLE | Sim | Total das diferenças absolutas calculadas ou registradas para o agrupamento. |
| `inicio_execucao` | VARCHAR | Sim | Data e hora de início da execução. |
| `fim_execucao` | VARCHAR | Sim | Data e hora de término da execução, quando disponível. |

### 3.2 `gold_cfo_volume_financeiro`

Tabela analítica voltada à visão financeira agregada. Permite analisar volumes internos e externos, quantidade de resultados e diferenças financeiras por data de referência, moedas e categoria.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `reference_date` | VARCHAR | Sim | Data de referência de negócio da conciliação. |
| `moeda_interna` | VARCHAR | Sim | Código da moeda associado aos valores internos. |
| `moeda_externa` | VARCHAR | Sim | Código da moeda associado aos valores externos. |
| `category` | VARCHAR | Sim | Categoria dos resultados de conciliação considerados no agrupamento. |
| `quantidade_resultados` | BIGINT | Sim | Quantidade de resultados incluídos no agrupamento. |
| `transacoes_distintas` | BIGINT | Sim | Quantidade de transações distintas consideradas no agrupamento. |
| `volume_interno` | DOUBLE | Sim | Volume financeiro interno agregado. |
| `volume_processador` | DOUBLE | Sim | Volume financeiro agregado informado pelo processador. |
| `diferenca_absoluta_total` | DOUBLE | Sim | Soma das diferenças absolutas consideradas no agrupamento. |

### 3.3 `gold_compliance_auditoria`

Tabela detalhada voltada à rastreabilidade e auditoria. Reúne dados do resultado da conciliação e informações da execução relacionada, incluindo identificadores, valores, datas e metadados de CDC e ingestão.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `resultado_id` | BIGINT | Sim | Identificador do resultado de conciliação. |
| `run_id` | BIGINT | Sim | Identificador da execução de conciliação relacionada. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação associada ao resultado. |
| `merchant_id` | VARCHAR | Sim | Identificador do estabelecimento ou merchant. |
| `category` | VARCHAR | Sim | Categoria atribuída ao resultado da conciliação. |
| `internal_amount` | DOUBLE | Sim | Valor registrado no sistema interno. |
| `processor_amount` | DOUBLE | Sim | Valor informado pelo processador externo. |
| `difference` | DOUBLE | Sim | Diferença registrada entre os valores interno e externo. |
| `resultado_criado_em` | VARCHAR | Sim | Data e hora de criação do resultado na origem. |
| `operacao_cdc_resultado` | VARCHAR | Sim | Operação CDC associada ao registro do resultado, quando disponível. |
| `timestamp_cdc_resultado` | VARCHAR | Sim | Timestamp do evento CDC associado ao resultado, quando disponível. |
| `hash_resultado` | VARCHAR | Sim | Hash do registro do resultado, utilizado para rastreabilidade. |
| `resultado_ingestado_em` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão do resultado. |
| `reference_date` | VARCHAR | Sim | Data de referência de negócio da execução. |
| `file_name` | VARCHAR | Sim | Nome do arquivo associado à execução. |
| `status_execucao` | VARCHAR | Sim | Situação da execução de conciliação. |
| `total_transactions` | BIGINT | Sim | Quantidade total de transações informada para a execução. |
| `started_at` | VARCHAR | Sim | Data e hora de início da execução. |
| `completed_at` | VARCHAR | Sim | Data e hora de conclusão da execução, quando disponível. |
| `execucao_criada_em` | VARCHAR | Sim | Data e hora de criação do registro da execução na origem. |
| `operacao_cdc_execucao` | VARCHAR | Sim | Operação CDC associada ao registro da execução, quando disponível. |
| `timestamp_cdc_execucao` | VARCHAR | Sim | Timestamp do evento CDC associado à execução, quando disponível. |
| `hash_execucao` | VARCHAR | Sim | Hash do registro da execução, utilizado para rastreabilidade. |
| `execucao_ingestada_em` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão do registro da execução. |
