"""CLI de orquestacion: python -m saas_pipeline.cli --env dev --tenant all \
--start-date ... --end-date ...

Ejecuta, por cada tenant solicitado: Bronze -> Silver (fact + dim) -> Quality -> Gold.
Respeta execution.fail_fast (5.8) y quality.fail_on_critical (5.8, 6.5).
"""

from __future__ import annotations

import argparse
import sys

from saas_pipeline import bronze, get_spark, gold, quality, silver
from saas_pipeline.config import list_available_tenants, load_config


def run_for_tenant(spark, cfg, tenant: str) -> tuple[bool, str]:
    print(f"\n=== Procesando tenant '{tenant}' ===")
    try:
        b_deliveries = bronze.ingest_deliveries(
            spark, cfg, tenant, cfg.execution.start_date, cfg.execution.end_date
        )
        bronze.ingest_materials(spark, cfg, tenant)
        print(f"  [bronze] deliveries -> {b_deliveries}")

        dim_stats = silver.build_dim_materials(spark, cfg, tenant)
        print(f"  [silver] dim_materials: {dim_stats}")

        fact_stats = silver.build_fact_deliveries(spark, cfg, tenant)
        print(f"  [silver] fact_deliveries: {fact_stats}")

        qr = quality.run_silver_checks(spark, cfg, tenant)
        quality.persist_quality_logs(spark, cfg, tenant, "silver", "fact_deliveries", qr)
        for r in qr.results:
            status = "OK" if r.passed else "FAIL"
            print(f"  [quality:{r.check_severity}] {r.check_name}: {status} "
                  f"({r.records_failed}/{r.records_checked} fallidos)")

        if cfg.quality.fail_on_critical and qr.any_critical_failed:
            msg = f"Validacion critica fallida para tenant '{tenant}'. Abortando antes de Gold."
            print(f"  [ABORT] {msg}")
            return False, msg

        gold_stats = gold.build_daily_metrics_by_delivery_type(spark, cfg, tenant)
        print(f"  [gold] daily_metrics_by_delivery_type: {gold_stats}")
        return True, "ok"
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] tenant '{tenant}' fallo: {exc}")
        return False, str(exc)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline SAAS multi-tenant")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "main"])
    parser.add_argument("--tenant", default="all", help="codigo de tenant o 'all'")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--fail-fast", action="store_true", default=None)
    parser.add_argument("--fail-on-critical", action="store_true", default=None)
    args = parser.parse_args(argv)

    tenants = list_available_tenants() if args.tenant == "all" else [args.tenant]
    spark = get_spark()

    failures = []
    for tenant in tenants:
        cfg = load_config(
            env=args.env,
            tenant=tenant,
            start_date=args.start_date,
            end_date=args.end_date,
            fail_fast=args.fail_fast,
            fail_on_critical=args.fail_on_critical,
        )
        ok, msg = run_for_tenant(spark, cfg, tenant)
        if not ok:
            failures.append((tenant, msg))
            if cfg.execution.fail_fast:
                print(f"\nfail_fast=true: abortando corrida completa por fallo en '{tenant}'.")
                break

    print("\n=== Resumen ===")
    if failures:
        for tenant, msg in failures:
            print(f"  FALLO {tenant}: {msg}")
        return 1
    print(f"  OK: {len(tenants)} tenant(s) procesados sin errores criticos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
