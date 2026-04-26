# CLAUDE.md — Data Engineer Challenge

## O que é este projeto

Desafio técnico para vaga de Data Engineer Sênior/Especialista em uma fintech de pagamentos.

O sistema existente (`Settlement Reconciliation Service`) processa diariamente um CSV do processador externo **PaySettler** e reconcilia com registros internos, persistindo resultados em PostgreSQL.

**Missão:** construir uma camada analítica sobre esses dados usando DuckDB + Python + Docker.

---

## Stack obrigatória (não mudar)

- Python 3.12+
- DuckDB >= 1.2.0
- Docker + Docker Compose
- Armazenamento: filesystem local (sem banco externo)
- Rodar com: `docker compose up`

Dependências já definidas em `pyproject.toml`. Dev deps: pytest, ruff.

---

## Estrutura de arquivos existente

```
data-engineer-challenge-main/
├── CLAUDE.md                  ← este arquivo
├── README.md                  ← enunciado completo do desafio
├── Makefile                   ← atalhos: make up / make test / make shell / make generate
├── Dockerfile                 ← python:3.12-slim, instala projeto com pip install -e ".[dev]"
├── docker-compose.yml         ← serviço "pipeline", monta sample-data como :ro
├── pyproject.toml             ← build config (hatchling)
├── .env.example
├── docs/
│   ├── domain-glossary.md     ← LEIA: regras de negócio e categorias de reconciliação
│   └── sample-data/           ← dados de exemplo para desenvolvimento
│       ├── transactions_batch_1.parquet
│       ├── transactions_batch_2.parquet
│       ├── reconciliation_runs.parquet
│       ├── reconciliation_results.parquet
│       ├── settlement_paysettler.csv
│       └── enterprise_company.parquet
├── scripts/
│   └── generate_sample_data.py  ← gerador sintético parametrizável
└── tests/
    └── test_generate_sample_data.py
```

---

## Domínio: regras críticas de negócio

### Categorias de reconciliação (campo `category`)

| Categoria | Significa |
|-----------|-----------|
| `MATCHED` | Transação existe nos dois lados, valores dentro da tolerância |
| `MISMATCHED` | Existe nos dois lados, mas diferença > R$ 0,01 |
| `UNRECONCILED_PROCESSOR` | Existe no PaySettler mas NÃO no sistema interno |
| `UNRECONCILED_INTERNAL` | Existe internamente mas NÃO apareceu no PaySettler |

### Tolerância: R$ 0,01 (um centavo) — diferenças menores são MATCHED
### Matching: campo `transaction_id` (UUID) — mesmo identificador nos dois sistemas
### Janela temporal: `[reference_date - 7 dias, reference_date]` sobre `created_at` das transações

### Tabelas fonte (PostgreSQL → já disponíveis como parquet/csv em sample-data/)

- `transactions` — transações internas (status: COMPLETED/PENDING/FAILED)
- `reconciliation_runs` — cada execução de reconciliação (status: IN_PROGRESS/COMPLETED/FAILED)
- `reconciliation_results` — resultado por transação com categoria e valores
- `enterprise_company` — cadastro de merchants (razão social, CNPJ, CNAE)
- CSV PaySettler — arquivo externo diário (status: SETTLED/REVERSED)

---

## Três partes a entregar

### Parte 1 — Modelagem analítica

Criar tabelas analíticas (fatos + dimensões) para três consumidores:

- **Operações:** saúde diária das reconciliações (taxa de discrepância, volume de mismatches)
- **CFO:** volume financeiro consolidado por dia/merchant
- **Compliance:** rastreabilidade e histórico para auditoria

Para cada consumidor documentar: quais perguntas o modelo responde, por que essas métricas, e exemplos de queries rodando.

### Parte 2 — Pipeline de dados

Pipeline que processa as fontes e alimenta as tabelas da Parte 1.

Critérios obrigatórios:
- **Idempotência:** rodar duas vezes para a mesma `reference_date` não duplica dados
- **Observabilidade:** logs e sinais para diagnóstico em produção
- **Qualidade de dados:** checks que protegem downstream (o que para vs só alerta)
- **Testabilidade:** testes automatizados validando comportamento

Projetar para ~5M transações/mês em produção.

### Parte 3 — Arquitetura (documentação)

- 3.1: Desenho de arquitetura end-to-end (PostgreSQL → consumo pelo negócio)
- 3.2: Troubleshooting — dashboards sem dados desde sexta, segunda de manhã, como investigar
- 3.3: Escalabilidade — chegar em ~5M transações/dia (1,8B/ano em reconciliation_results)

---

## Entrega

PR do fork para o repositório original. README do PR deve incluir:

- Como rodar
- Premissas e decisões
- Visão geral da arquitetura (diagrama bem-vindo)
- Extensibilidade (segunda fonte de liquidação: quantos arquivos/linhas mudam?)
- Limites do desenho (2-3 exemplos concretos do que não suporta)
- O que faria diferente em produção
- Ferramentas de IA utilizadas (encorajado — documentar como usou)

---

## Comandos disponíveis

```bash
make up            # build + start container
make down          # stop
make shell         # bash dentro do container
make test          # pytest tests/
make logs          # logs do container
make generate      # gera 10k linhas em /tmp/generated
make generate-large # gera 1M linhas em /tmp/generated-large
```

Gerador de dados sintéticos (dentro do container):
```bash
python scripts/generate_sample_data.py --rows 1000000 --days 90 --merchants 500 --seed 42 --out /tmp/medium
```

---

## Convenções

- Ruff para linting (line-length: 100, target: py312)
- pytest para testes, testpaths = ["tests/"]
- Type hints obrigatórios em funções públicas
- Commits descritivos em inglês (feat/fix/docs/chore)

---

## O que NÃO mudar

- `pyproject.toml` — dependências core definidas (duckdb, pyarrow)
- `Dockerfile` — base image e estrutura de build
- `docker-compose.yml` — serviço "pipeline" e volumes
- `scripts/generate_sample_data.py` — gerador fornecido pelo desafio
- `tests/test_generate_sample_data.py` — teste existente do gerador
- `docs/sample-data/` — dados de exemplo (montados como read-only no container)

---

## Contexto adicional

- O desafio **encoraja explicitamente** manter CLAUDE.md e configs de IA no repositório
- Prazo: 3 a 4 dias
- Após entrega haverá conversa técnica de ~45 min sobre as decisões tomadas
- Qualidade e completude valem mais que quantidade de features
