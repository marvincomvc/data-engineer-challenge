"""
Interface de linha de comando (CLI) do pipeline de reconciliação.

Uso:
    python -m src.cli --reference-date 2025-03-15

Argumentos:
    --reference-date  Data de referência (formato YYYY-MM-DD). Obrigatório.
    --data-path       Pasta com os arquivos de entrada (default: docs/sample-data).
    --output-path     Caminho do DuckDB de saída (default: data/analytics.db).
    --bronze-path     Pasta bronze/ para registros de qualidade (default: bronze).
    --log-level       Nível de logging: DEBUG, INFO, WARNING, ERROR (default: INFO).
"""

import argparse
import logging
import sys
from pathlib import Path

from src.config import (
    DEFAULT_BRONZE_PATH,
    DEFAULT_DATA_PATH,
    DEFAULT_LOG_LEVEL,
    DEFAULT_OUTPUT_PATH,
)
from src.pipeline import run_pipeline


def configurar_logging(nivel: str) -> None:
    """
    Configura o logging com formato estruturado.

    Formato: 2025-03-15 14:30:00,123 - INFO - mensagem

    Args:
        nivel: Nível de logging (DEBUG, INFO, WARNING, ERROR).
    """
    formato = "%(asctime)s - %(levelname)s - %(message)s"
    nivel_log = getattr(logging, nivel.upper(), logging.INFO)

    logging.basicConfig(
        level=nivel_log,
        format=formato,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """
    Configura e faz o parse dos argumentos da CLI.

    Returns:
        Namespace com os argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="Pipeline de Reconciliação de Liquidações",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python -m src.cli --reference-date 2025-03-15
  python -m src.cli --reference-date 2025-03-15 --data-path docs/sample-data
  python -m src.cli --reference-date 2025-03-15 --output-path data/analytics.db --bronze-path bronze --log-level DEBUG
        """,
    )

    parser.add_argument(
        "--reference-date",
        type=str,
        required=True,
        help="Data de referência no formato YYYY-MM-DD (ex: 2025-03-15)",
    )

    parser.add_argument(
        "--data-path",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help=f"Pasta com os arquivos de entrada (default: {DEFAULT_DATA_PATH})",
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Caminho do DuckDB de saída (default: {DEFAULT_OUTPUT_PATH})",
    )

    parser.add_argument(
        "--bronze-path",
        type=Path,
        default=DEFAULT_BRONZE_PATH,
        help=f"Pasta bronze para registros de qualidade (default: {DEFAULT_BRONZE_PATH})",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=f"Nível de logging (default: {DEFAULT_LOG_LEVEL})",
    )

    return parser.parse_args()


def main() -> None:
    """Função principal da CLI."""
    args = parse_args()

    # Configura logging
    configurar_logging(args.log_level)

    logger = logging.getLogger(__name__)

    # Valida se a data de referência está no formato correto
    import re

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", args.reference_date):
        logger.error(
            "Data de referência inválida: '%s'. Use o formato YYYY-MM-DD.",
            args.reference_date,
        )
        sys.exit(1)

    # Valida se a pasta de dados existe
    if not args.data_path.exists():
        logger.error(
            "Pasta de dados não encontrada: '%s'.", args.data_path
        )
        sys.exit(1)

    logger.info("Iniciando pipeline via CLI...")
    logger.info("  reference_date: %s", args.reference_date)
    logger.info("  data_path:      %s", args.data_path)
    logger.info("  output_path:    %s", args.output_path)
    logger.info("  bronze_path:    %s", args.bronze_path)
    logger.info("  log_level:      %s", args.log_level)

    try:
        sumario = run_pipeline(
            reference_date=args.reference_date,
            data_path=args.data_path,
            output_path=args.output_path,
            bronze_path=args.bronze_path,
        )

        logger.info("Pipeline executado com sucesso.")
        logger.info(
            "Registros carregados na fato: %s",
            sumario["registros_carregados_fato"],
        )
        logger.info(
            "Tempo total: %.2f segundos",
            sumario["tempo_total_segundos"],
        )

    except SystemExit as e:
        logger.error("Pipeline encerrado com falha crítica: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Erro inesperado durante a execução do pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    main()