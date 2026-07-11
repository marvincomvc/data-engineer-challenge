FROM python:3.12-slim

WORKDIR /app

# System deps: git is required by dbt for package resolution.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install deps + package first (needs src/ so hatchling can build) for layer
# caching: this layer only rebuilds when pyproject.toml or src/ change.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev]"

# The rest of the project (dbt/, tests/, docs/, etc.); editable install already
# points at /app/src so code changes are picked up without reinstalling.
COPY . .

# Warehouse + dbt target live on a writable volume at runtime.
ENV DUCKDB_PATH=/app/warehouse/settlement.duckdb \
    DATA_PATH=/app/docs/sample-data \
    PYTHONUNBUFFERED=1
RUN mkdir -p /app/warehouse

CMD ["tail", "-f", "/dev/null"]
