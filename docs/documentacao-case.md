# Data Engineer Challenge

Esta documentação foi preparada a partir dos arquivos presentes no projeto: README, glossário, scripts Python, SQL analítico, configuração Docker, dependências e testes.

---

# 1. Resumo executivo do projeto

O desafio é transformar dados operacionais de conciliação de liquidações em produtos analíticos para três consumidores:

- **Operações:** acompanhar a saúde da conciliação e as divergências.
- **CFO:** acompanhar volumes financeiros e diferenças.
- **Compliance:** investigar resultados e reconstruir a origem dos dados usados.

A solução usa Python 3.12+, DuckDB, Parquet/CSV e Docker Compose. A organização segue três camadas:

1. **Bronze:** ingestão e preservação dos dados de origem com metadados de rastreabilidade.
2. **Silver:** estado corrente das entidades com CDC, normalização do CSV do processador, integração dos resultados com execuções e verificações de qualidade.
3. **Gold:** tabelas de consumo analítico para Operações, CFO e auditoria.

Vale ressaltar que eu "subi" uma copia do Banco de Dados no caminho: \artifacts\analytics.duckdb

# 2. Arquitetura implementada

```text
Arquivos de origem
  ├── transactions_batch_1.parquet
  ├── transactions_batch_2.parquet
  ├── reconciliation_runs.parquet
  ├── reconciliation_results.parquet
  ├── enterprise_company.parquet
  └── CSV do PaySettler
           |
           v
        Bronze
  ├── tabelas de origem
  ├── row_hash
  ├── ingested_at
  └── bronze_linhas_nao_inseridas
           |
           v
         Silver
  ├── estado atual de entidades CDC
  ├── normalização e deduplicação PaySettler
  ├── silver_transacoes_conciliacao
  └── silver_qualidade_dados
           |
           v
          Gold
  ├── gold_operacoes_conciliacao
  ├── gold_cfo_volume_financeiro
  └── gold_compliance_auditoria
           |
           v
     Consultas analíticas
```

# 3. Bronze: ingestão, hash e idempotência

## Carga inicial — `scripts/bronze/carga_inicial.py`

O script cria/substitui as tabelas Bronze a partir das fontes configuradas. A operação é envolvida em transação e, em caso de erro, executa rollback. Ao final, apresenta contagens e hashes distintos. É o script que executaremos uma unica vez no inicio do projeto, na implementacão em produção as duas cargas podem existir no mesmo código e ser parametrizavel para uma carga FULL ou INCREMENTAL.

## Carga incremental — `scripts/bronze/carga_incremental.py`

O fluxo implementado é:

1. Confere se o banco e os arquivos de entrada existem.
2. Inspeciona o schema dos arquivos e compara com o schema das tabelas Bronze existentes.
3. Cria tabelas temporárias de staging.
4. Calcula `row_hash` com SHA-256 sobre o conteúdo das colunas da origem, excluindo `Op` e `_timestamp`.
5. Numera ocorrências com o mesmo hash dentro do arquivo.
6. Insere apenas a primeira ocorrência do hash e apenas quando o hash ainda não existe na tabela Bronze.
7. Registra ocorrências duplicadas no arquivo ou já existentes na Bronze em `bronze_linhas_nao_inseridas`.
8. Executa tudo em uma transação e faz rollback em caso de erro.

### O que o `row_hash` significa

O hash identifica o conteúdo das colunas consideradas no cálculo, não uma entidade de negócio por si só. Como os campos CDC são excluídos, eventos com o mesmo conteúdo de negócio e metadados CDC diferentes podem resultar no mesmo hash.

### Idempotência: o que é verdade no código

O mecanismo evita inserir novamente uma linha com hash já presente na Bronze. A auditoria usa `audit_hash` como chave de conflito para não repetir a mesma ocorrência de auditoria em reprocessamentos idênticos.

## `ingerir_paysettler.py`

Esse script:
- calcula SHA-256 do arquivo inteiro;
- calcula hash de registro a partir dos campos do CSV;
- verifica cabeçalho e colunas obrigatórias;
- normaliza espaços dos valores textuais;
- insere registros cujo `row_hash` ainda não existe;
- grava `arquivo_origem` e `hash_arquivo`;
- usa transação por arquivo.

---

# 4. Silver: CDC, normalização e qualidade

## Entidades CDC

O script `scripts/silver/ingerir_silver.py` reconstrói o estado atual de:
- `silver_transactions`, unindo os dois batches por nome de coluna;
- `silver_reconciliation_runs`;
- `silver_reconciliation_results`;
- `silver_enterprise_company`.

O uso de `UNION ALL BY NAME` permite lidar com o schema drift conhecido entre batches, como `payment_method` estar presente no segundo batch e ausente no primeiro.

### Como o estado atual é escolhido

Para cada chave de entidade, o script ordena pelos campos temporais disponíveis entre `created_at`, `updated_at` e `_timestamp`, usando conversão para timestamp com fuso. Em caso de empate, usa `row_hash` e depois a operação CDC como desempate. Mantém a linha mais recente e exclui o registro quando o evento vencedor tem operação `D`.

Também:
- exclui eventos sem chave da visão corrente e imprime aviso;
- interrompe a publicação se encontrar valores temporais não nulos que não possam ser convertidos;
- verifica se a chave fica única no resultado.

## Normalização do PaySettler

A função `ingerir_paysettler`:
- remove espaços desnecessários;
- converte `merchant_id`, `currency` e `status` para maiúsculas;
- transforma string vazia em nulo em campos selecionados;
- aceita vírgula como separador decimal em `amount`, substituindo-a por ponto;
- converte `amount` para `DECIMAL(18,2)`;
- converte `settled_at` para `TIMESTAMPTZ`;
- deduplica por `row_hash`, mantendo uma ocorrência.

A conversão com `TRY_CAST` evita que a conversão inválida interrompa automaticamente a consulta, mas pode produzir nulo. Por isso, a qualidade de `amount` e `settled_at` precisa ser verificada depois.

## Integração histórica

`silver_transacoes_conciliacao` é construída por `INNER JOIN` entre resultados e execuções, usando `run_id = id`. A tabela é uma integração para análise; não recalcula a categoria nem mistura diretamente os dados do CSV para refazer a conciliação.

## Qualidade na Silver

A tabela `silver_qualidade_dados` agrega contagens de:
- valores internos nulos;
- valores externos nulos ou inválidos;
- datas de liquidação nulas ou inválidas;
- status do PaySettler desconhecido;
- execuções não concluídas.

O script também valida unicidade de chaves e avisa sobre resultados sem execução.

---

# 5. Gold: produtos analíticos

## `gold_operacoes_conciliacao`

Agrupa por data de referência, execução, arquivo, status, categoria e moedas. Produz:
- quantidade de resultados;
- resultados sem diferença;
- diferenças acima de zero até 0,01;
- diferenças acima de 0,01;
- totais internos e externos;
- soma das diferenças absolutas;
- início e fim da execução.

As faixas implementadas são:
- sem diferença: `ABS(diferenca) = 0`;
- diferença pequena: `ABS(diferenca) > 0 AND ABS(diferenca) <= 0.01`;
- diferença acima da tolerância: `ABS(diferenca) > 0.01`.

O SQL operacional usa essa tabela para calcular taxa de `MISMATCHED`, distribuição de categorias e dias com maior diferença absoluta total.

## `gold_cfo_volume_financeiro`

Agrupa por data de referência, moedas e categoria e produz quantidade de resultados, transações distintas, volume interno, volume do processador e diferença absoluta total.

**Ponto técnico importante:** o código atual calcula uma moeda por `transaction_id` usando `MAX(currency)` nas tabelas internas e do processador. Isso não identifica explicitamente transações com moedas múltiplas e o relacionamento com o processador não filtra `settled_at` pela data de referência nesta tabela. Em comparação, a lógica de moedas da tabela operacional tem tratamento para mais de uma moeda e considera a data de referência do processador. Essa diferença merece ser revisada ou apresentada como limitação conhecida.

## `gold_compliance_auditoria`

Faz `LEFT JOIN` dos resultados com a execução e expõe identificadores, categoria, valores, datas, status da execução, campos CDC, hashes e horários de ingestão.

O `LEFT JOIN` preserva o resultado mesmo quando a execução correspondente não existe, permitindo auditoria de órfãos. Isso é diferente da tabela de integração Silver, que usa `INNER JOIN`.

## Estratégia de materialização

As tabelas Gold são recriadas com `CREATE OR REPLACE TABLE`. A função principal executa as três criações dentro de uma transação, com `COMMIT` ao final e `ROLLBACK` se ocorrer exceção.

---

# 6. Regras de negócio

## Categorias

- `MATCHED`: existe nos dois lados e os valores estão de acordo dentro da tolerância.
- `MISMATCHED`: existe nos dois lados, mas a diferença ultrapassa a tolerância.
- `UNRECONCILED_PROCESSOR`: aparece no processador e não nos registros internos.
- `UNRECONCILED_INTERNAL`: aparece internamente, mas não no processador dentro do escopo temporal.

## Tolerância financeira

Diferenças de até R$ 0,01, inclusive, são aceitáveis de acordo com o glossário. Na Gold operacional, diferenças positivas até 0,01 ficam na faixa pequena; acima de 0,01 são separadas como divergência acima da tolerância.

## Janela temporal

O glossário define a janela interna como `[reference_date - 7 dias, reference_date]`, inclusiva, aplicada a `created_at` das transações internas. A data de referência é interpretada em UTC.

## Chave de matching

O glossário define `transaction_id` como chave de correspondência entre o sistema interno e o processador.

---

# 7. Escalabilidade: de arquivos locais para bilhões de resultados

O enunciado projeta até cerca de 5 milhões de transações por dia em 18 meses. O desenho local serve para demonstrar o raciocínio, mas não deve ser apresentado como pronto para 1,8 bilhão de resultados por ano sem mudanças.

## Evolução plausível para produção

- Armazenamento de objetos para dados brutos e curados, com particionamento por data de referência.
- Orquestrador para agendar, monitorar, repetir e recuperar etapas.
- Processamento incremental por janela/data, com checkpoints e controle explícito de estado.
- Catálogo e contratos de dados com schema versionado.
- Camada de serving/warehouse adequada aos consumidores e à concorrência de consultas.
- Observabilidade com métricas, logs estruturados, alertas e SLAs.
- Testes unitários, integração e reconciliação de contagens/valores após cada etapa.
---
