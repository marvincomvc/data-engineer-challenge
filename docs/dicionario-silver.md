# Dicionário de Dados — Camada Silver

## 1. Objetivo

A camada Silver organiza e padroniza os dados ingeridos na Bronze para facilitar o uso analítico. Neste projeto, ela contém dados internos consolidados, liquidações recebidas do PaySettler, resultados de conciliação enriquecidos com informações das execuções e uma tabela de verificações de qualidade.

A documentação abaixo reflete o schema identificado no DuckDB. As descrições de negócio são baseadas nos nomes das colunas e no contexto do desafio. Quando o schema não permite concluir uma regra exata, isso é indicado.

## 2. Visão geral das tabelas

| Tabela | Finalidade |
|---|---|
| `silver_transactions` | Transações internas consolidadas a partir dos batches de origem. |
| `silver_settlement_paysettler` | Liquidações do PaySettler em formato tipado e com metadados de arquivo. |
| `silver_transacoes_conciliacao` | Resultados de conciliação associados aos dados da execução correspondente. |
| `silver_reconciliation_results` | Resultados de conciliação preservados em estrutura próxima à origem. |
| `silver_reconciliation_runs` | Dados das execuções de conciliação preservados em estrutura próxima à origem. |
| `silver_enterprise_company` | Cadastro de empresas e estabelecimentos em estrutura próxima à origem. |
| `silver_qualidade_dados` | Contagens de verificações de qualidade por tabela. |

## 3. Dicionário por tabela

### 3.1 `silver_transactions`

Representa as transações internas consolidadas. A coluna `payment_method` existe na Silver, embora não esteja presente em todos os batches de origem.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador numérico do registro de origem. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação usado para relacionar registros. |
| `merchant_id` | VARCHAR | Sim | Identificador do estabelecimento ou merchant. |
| `amount` | DOUBLE | Sim | Valor monetário da transação interna. |
| `currency` | VARCHAR | Sim | Código da moeda da transação. |
| `status` | VARCHAR | Sim | Situação da transação interna. |
| `description` | VARCHAR | Sim | Descrição textual da transação, quando disponível. |
| `created_at` | VARCHAR | Sim | Data e hora de criação conforme recebidas da origem. |
| `updated_at` | VARCHAR | Sim | Data e hora da última atualização conforme recebidas da origem. |
| `Op` | VARCHAR | Sim | Campo de operação CDC recebido da origem, quando disponível. |
| `_timestamp` | VARCHAR | Sim | Timestamp associado ao evento CDC, quando disponível. |
| `row_hash` | VARCHAR | Sim | Hash usado para identificar o conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão do registro na camada. |
| `payment_method` | VARCHAR | Sim | Método de pagamento, disponível em parte dos dados de origem. |

### 3.2 `silver_settlement_paysettler`

Representa os registros de liquidação do PaySettler. O valor monetário é tipado como `DECIMAL(18,2)` e a data de liquidação como timestamp com fuso horário.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `transaction_id` | VARCHAR | Sim | Identificador da transação informado pelo processador. |
| `merchant_id` | VARCHAR | Sim | Identificador do estabelecimento ou merchant. |
| `amount` | DECIMAL(18,2) | Sim | Valor da liquidação com precisão decimal de duas casas. |
| `currency` | VARCHAR | Sim | Código da moeda informada pelo processador. |
| `settled_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora da liquidação, com informação de fuso horário. |
| `processor_reference` | VARCHAR | Sim | Referência da transação no processador. |
| `status` | VARCHAR | Sim | Situação informada pelo PaySettler. |
| `row_hash` | VARCHAR | Sim | Hash do conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão na camada. |
| `arquivo_origem` | VARCHAR | Sim | Nome ou identificação do arquivo de origem. |
| `hash_arquivo` | VARCHAR | Sim | Hash usado para identificar o arquivo recebido. |

### 3.3 `silver_transacoes_conciliacao`

Tabela de apoio analítico que associa o resultado da conciliação aos dados da execução. Os nomes das colunas estão em português, exceto os identificadores técnicos mantidos da origem.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `resultado_id` | BIGINT | Sim | Identificador do resultado de conciliação. |
| `run_id` | BIGINT | Sim | Identificador da execução de conciliação. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação relacionada ao resultado. |
| `category` | VARCHAR | Sim | Categoria ou classificação do resultado da conciliação. |
| `valor_interno` | DOUBLE | Sim | Valor registrado no sistema interno. |
| `valor_externo` | DOUBLE | Sim | Valor informado pelo processador externo. |
| `diferenca` | DOUBLE | Sim | Diferença entre os valores, conforme registrada no resultado. |
| `reference_date` | VARCHAR | Sim | Data de referência de negócio da execução. |
| `file_name` | VARCHAR | Sim | Nome do arquivo associado à execução. |
| `status_execucao` | VARCHAR | Sim | Situação da execução de conciliação. |
| `started_at` | VARCHAR | Sim | Data e hora de início da execução. |
| `completed_at` | VARCHAR | Sim | Data e hora de conclusão da execução, quando disponível. |

### 3.4 `silver_reconciliation_results`

Mantém os resultados de conciliação em estrutura próxima à tabela de origem.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador do resultado. |
| `run_id` | BIGINT | Sim | Identificador da execução relacionada. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação. |
| `merchant_id` | VARCHAR | Sim | Identificador do estabelecimento ou merchant. |
| `category` | VARCHAR | Sim | Classificação do resultado da conciliação. |
| `internal_amount` | DOUBLE | Sim | Valor registrado internamente. |
| `processor_amount` | DOUBLE | Sim | Valor registrado pelo processador. |
| `difference` | DOUBLE | Sim | Diferença registrada entre os valores. |
| `created_at` | VARCHAR | Sim | Data e hora de criação do resultado, conforme recebidas da origem. |
| `Op` | VARCHAR | Sim | Operação CDC, quando disponível. |
| `_timestamp` | VARCHAR | Sim | Timestamp do evento CDC, quando disponível. |
| `row_hash` | VARCHAR | Sim | Hash do conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão na camada. |

### 3.5 `silver_reconciliation_runs`

Mantém os metadados das execuções de conciliação em estrutura próxima à origem.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador da execução. |
| `reference_date` | VARCHAR | Sim | Data de referência de negócio da execução. |
| `file_name` | VARCHAR | Sim | Nome do arquivo processado. |
| `status` | VARCHAR | Sim | Situação da execução. |
| `total_transactions` | BIGINT | Sim | Quantidade total de transações informada na execução. |
| `started_at` | VARCHAR | Sim | Data e hora de início da execução. |
| `completed_at` | VARCHAR | Sim | Data e hora de conclusão da execução, quando disponível. |
| `created_at` | VARCHAR | Sim | Data e hora de criação do registro da execução. |
| `Op` | VARCHAR | Sim | Operação CDC, quando disponível. |
| `_timestamp` | VARCHAR | Sim | Timestamp do evento CDC, quando disponível. |
| `row_hash` | VARCHAR | Sim | Hash do conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão na camada. |

### 3.6 `silver_enterprise_company`

Cadastro de empresas e estabelecimentos, mantido em estrutura próxima à origem.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador numérico do cadastro. |
| `merchant_id` | VARCHAR | Sim | Identificador do estabelecimento ou merchant. |
| `legal_name` | VARCHAR | Sim | Razão social. |
| `trade_name` | VARCHAR | Sim | Nome fantasia. |
| `document` | VARCHAR | Sim | Documento cadastral da empresa. |
| `primary_cnae` | VARCHAR | Sim | CNAE principal informado no cadastro. |
| `created_at` | VARCHAR | Sim | Data e hora de criação conforme recebidas da origem. |
| `updated_at` | VARCHAR | Sim | Data e hora de atualização conforme recebidas da origem. |
| `Op` | VARCHAR | Sim | Operação CDC, quando disponível. |
| `_timestamp` | VARCHAR | Sim | Timestamp do evento CDC, quando disponível. |
| `row_hash` | VARCHAR | Sim | Hash do conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data e hora de ingestão na camada. |

### 3.7 `silver_qualidade_dados`

Tabela resumida de verificações de qualidade. O schema contém contagens por tabela e verificação, mas não registra, por si só, a regra completa, o limite esperado ou o detalhe de cada linha inválida.

| Coluna | Tipo | Aceita nulo | Descrição |
|---|---|---|---|
| `tabela` | VARCHAR | Sim | Nome da tabela avaliada. |
| `verificacao` | VARCHAR | Sim | Nome ou descrição da verificação executada. |
| `quantidade` | BIGINT | Sim | Quantidade associada à verificação. A interpretação exata depende da regra registrada em `verificacao`. |