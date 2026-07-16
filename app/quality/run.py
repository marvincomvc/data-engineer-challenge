from app.quality.checks import run_all_checks
from app.quality.models import save_quality_results

from app.config import get_logger

logger = get_logger("quality.run")

def run_quality():

    logger.info("=" * 80)
    logger.info("QUALITY CHECKS")
    logger.info("=" * 80)

    results = run_all_checks()

    for result in results:
        logger.info(f"[{result['status']}] {result['check_name']} -> {result['message']}")

    save_quality_results(results)

    passed = sum(r["status"] == "PASS" for r in results)
    warnings = sum(r["status"] == "WARN" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)

    logger.info("")
    logger.info("=" * 80)
    logger.info(f"PASS={passed}  WARN={warnings}  FAIL={failed}")
    logger.info("=" * 80)

    if failed:
        raise RuntimeError("Quality validation failed.")
