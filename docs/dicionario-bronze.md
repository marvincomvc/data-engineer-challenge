# Dicionário de Dados — Camada Bronze

## 1. Objetivo

A camada Bronze armazena os dados provenientes das fontes de origem, mantendo os campos recebidos e os metadados técnicos utilizados no processo de ingestão.

Nesta etapa, os dados são organizados em tabelas para preservar sua rastreabilidade e permitir as transformações posteriores na camada Silver.

## 2. Tabelas da camada Bronze

A camada Bronze contém as seguintes tabelas:

| Tabela | Descrição |
|---|---|
| `bronze_enterprise_company` | Dados cadastrais das empresas e merchants. |
| `bronze_linhas_nao_inseridas` | Registros que não foram inseridos no fluxo principal. |
| `bronze_paysettler_arquivo` | Dados extraídos do arquivo de liquidação do PaySettler. |
| `bronze_reconciliation_results` | Resultados de conciliação provenientes da origem. |
| `bronze_reconciliation_runs` | Metadados das execuções de conciliação. |
| `bronze_settlement_paysettler` | Dados de liquidação do PaySettler em formato tabular. |
| `bronze_transactions_batch_1` | Transações internas provenientes do primeiro lote. |
| `bronze_transactions_batch_2` | Transações internas provenientes do segundo lote. |

## 3. Dicionário de colunas

### 3.1 `bronze_enterprise_company`

**Descrição:** armazena os dados cadastrais das empresas e merchants, incluindo metadados técnicos de ingestão.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador numérico do registro na origem. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `legal_name` | VARCHAR | Sim | Razão social da empresa. |
| `trade_name` | VARCHAR | Sim | Nome fantasia da empresa. |
| `document` | VARCHAR | Sim | Documento de identificação da empresa. |
| `primary_cnae` | VARCHAR | Sim | CNAE principal da empresa. |
| `created_at` | VARCHAR | Sim | Data/hora de criação na origem, armazenada como texto. |
| `updated_at` | VARCHAR | Sim | Data/hora de atualização na origem, armazenada como texto. |
| `Op` | VARCHAR | Sim | Metadado de operação associado à origem/CDC. |
| `_timestamp` | VARCHAR | Sim | Metadado temporal associado à origem/CDC. |
| `row_hash` | VARCHAR | Sim | Hash utilizado para identificar o conteúdo do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão do registro. |

### 3.2 `bronze_linhas_nao_inseridas`

**Descrição:** registra linhas que não foram inseridas no fluxo principal, permitindo consultar o conteúdo original e o motivo da ocorrência.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `tabela_origem` | VARCHAR | Não | Nome da tabela de origem. |
| `arquivo_origem` | VARCHAR | Não | Arquivo de origem do registro. |
| `row_hash` | VARCHAR | Não | Hash do registro original. |
| `dados_origem` | JSON | Não | Conteúdo original do registro em formato JSON. |
| `motivo` | VARCHAR | Não | Motivo registrado para a não inserção. |
| `data_ingestao` | TIMESTAMP WITH TIME ZONE | Não | Data/hora em que a ocorrência foi registrada. |
| `audit_hash` | VARCHAR | Não | Hash adicional associado à rastreabilidade da ocorrência. |

### 3.3 `bronze_paysettler_arquivo`

**Descrição:** armazena os dados lidos do arquivo de liquidação do PaySettler, mantendo os campos do arquivo e informações para rastreabilidade da ingestão.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `transaction_id` | VARCHAR | Sim | Identificador da transação informado pelo processador. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `amount` | VARCHAR | Sim | Valor informado pelo processador, armazenado como texto. |
| `currency` | VARCHAR | Sim | Código da moeda informado no arquivo. |
| `settled_at` | VARCHAR | Sim | Data/hora da liquidação, armazenada como texto. |
| `processor_reference` | VARCHAR | Sim | Referência da transação no processador. |
| `status` | VARCHAR | Sim | Status informado pelo processador. |
| `row_hash` | VARCHAR | Não | Hash associado ao registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Não | Data/hora da ingestão do registro. |
| `arquivo_origem` | VARCHAR | Não | Identificação do arquivo de origem. |
| `hash_arquivo` | VARCHAR | Não | Hash associado ao arquivo de origem. |

### 3.4 `bronze_reconciliation_results`

**Descrição:** armazena os resultados de conciliação recebidos da origem, incluindo os valores comparados e a categoria atribuída ao resultado.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador do resultado de conciliação. |
| `run_id` | BIGINT | Sim | Identificador da execução de conciliação relacionada. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação conciliada. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `category` | VARCHAR | Sim | Categoria atribuída ao resultado da conciliação. |
| `internal_amount` | DOUBLE | Sim | Valor encontrado no sistema interno, quando aplicável. |
| `processor_amount` | DOUBLE | Sim | Valor encontrado no processador, quando aplicável. |
| `difference` | DOUBLE | Sim | Diferença registrada entre os valores. |
| `created_at` | VARCHAR | Sim | Data/hora de criação do resultado na origem. |
| `Op` | VARCHAR | Sim | Metadado de operação associado à origem/CDC. |
| `_timestamp` | VARCHAR | Sim | Metadado temporal associado à origem/CDC. |
| `row_hash` | VARCHAR | Sim | Hash do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão do registro. |

### 3.5 `bronze_reconciliation_runs`

**Descrição:** armazena os metadados das execuções de conciliação, incluindo arquivo processado, data de referência, status e horários de execução.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador da execução de conciliação. |
| `reference_date` | VARCHAR | Sim | Data de referência da conciliação. |
| `file_name` | VARCHAR | Sim | Nome do arquivo processado. |
| `status` | VARCHAR | Sim | Status da execução, conforme os valores definidos pela origem. |
| `total_transactions` | BIGINT | Sim | Quantidade de transações informada para a execução. |
| `started_at` | VARCHAR | Sim | Data/hora de início da execução. |
| `completed_at` | VARCHAR | Sim | Data/hora de conclusão da execução, quando disponível. |
| `created_at` | VARCHAR | Sim | Data/hora de criação do registro na origem. |
| `Op` | VARCHAR | Sim | Metadado de operação associado à origem/CDC. |
| `_timestamp` | VARCHAR | Sim | Metadado temporal associado à origem/CDC. |
| `row_hash` | VARCHAR | Sim | Hash do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão do registro. |

### 3.6 `bronze_settlement_paysettler`

**Descrição:** armazena os dados de liquidação do PaySettler em estrutura tabular, com o campo `settled_at` tipado como timestamp com fuso horário.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `transaction_id` | VARCHAR | Sim | Identificador da transação. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `amount` | VARCHAR | Sim | Valor da liquidação armazenado como texto. |
| `currency` | VARCHAR | Sim | Código da moeda. |
| `settled_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora da liquidação com fuso horário. |
| `processor_reference` | VARCHAR | Sim | Referência da transação no processador. |
| `status` | VARCHAR | Sim | Status informado pelo processador. |
| `row_hash` | VARCHAR | Sim | Hash associado ao registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão. |

### 3.7 `bronze_transactions_batch_1`

**Descrição:** armazena as transações internas provenientes do primeiro lote de dados.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador numérico do registro. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `amount` | DOUBLE | Sim | Valor da transação. |
| `currency` | VARCHAR | Sim | Código da moeda. |
| `status` | VARCHAR | Sim | Status da transação na origem. |
| `description` | VARCHAR | Sim | Descrição da transação. |
| `created_at` | VARCHAR | Sim | Data/hora de criação na origem, armazenada como texto. |
| `updated_at` | VARCHAR | Sim | Data/hora de atualização na origem, armazenada como texto. |
| `Op` | VARCHAR | Sim | Metadado de operação associado à origem/CDC. |
| `_timestamp` | VARCHAR | Sim | Metadado temporal associado à origem/CDC. |
| `row_hash` | VARCHAR | Sim | Hash do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão. |

### 3.8 `bronze_transactions_batch_2`

**Descrição:** armazena as transações internas provenientes do segundo lote. Em relação ao primeiro lote, o schema também contém o campo `payment_method`.

| Coluna | Tipo de dado | Aceita nulo | Descrição |
|---|---|---|---|
| `id` | BIGINT | Sim | Identificador numérico do registro. |
| `transaction_id` | VARCHAR | Sim | Identificador da transação. |
| `merchant_id` | VARCHAR | Sim | Identificador do merchant. |
| `amount` | DOUBLE | Sim | Valor da transação. |
| `currency` | VARCHAR | Sim | Código da moeda. |
| `status` | VARCHAR | Sim | Status da transação na origem. |
| `description` | VARCHAR | Sim | Descrição da transação. |
| `created_at` | VARCHAR | Sim | Data/hora de criação na origem, armazenada como texto. |
| `updated_at` | VARCHAR | Sim | Data/hora de atualização na origem, armazenada como texto. |
| `payment_method` | VARCHAR | Sim | Meio de pagamento, disponível neste lote. |
| `Op` | VARCHAR | Sim | Metadado de operação associado à origem/CDC. |
| `_timestamp` | VARCHAR | Sim | Metadado temporal associado à origem/CDC. |
| `row_hash` | VARCHAR | Sim | Hash do registro. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | Sim | Data/hora de ingestão. |