from app.quality.checks import run_all_checks
from app.quality.models import save_quality_results


def run_quality():

    print("=" * 80)
    print("QUALITY CHECKS")
    print("=" * 80)

    results = run_all_checks()

    for result in results:
        print(f"[{result['status']}] {result['check_name']} -> {result['message']}")

    save_quality_results(results)

    passed = sum(r["status"] == "PASS" for r in results)
    warnings = sum(r["status"] == "WARN" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)

    print()
    print("=" * 80)
    print(f"PASS={passed}  WARN={warnings}  FAIL={failed}")
    print("=" * 80)

    if failed:
        raise RuntimeError("Quality validation failed.")
