"""Validaciones de calidad de datos sobre Silver + persistencia en quality_logs (5.9, 6.5)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from saas_pipeline.config import paths_for

QUALITY_LOG_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)


@dataclass
class CheckResult:
    check_name: str
    check_severity: str  # critical | warning | info
    records_checked: int
    records_failed: int

    @property
    def passed(self) -> bool:
        return self.records_failed == 0


@dataclass
class QualityRunResult:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def any_critical_failed(self) -> bool:
        return any(r.check_severity == "critical" and not r.passed for r in self.results)


def run_silver_checks(spark: SparkSession, cfg, tenant: str) -> QualityRunResult:
    """Ejecuta >=3 validaciones no triviales sobre fact_deliveries/dim_materials."""
    lp = paths_for(cfg, tenant, "fact_deliveries")
    dim_lp = paths_for(cfg, tenant, "dim_materials")

    fact = spark.read.format("delta").load(lp.silver_table())
    dim = spark.read.format("delta").load(dim_lp.silver_table())

    results: list[CheckResult] = []

    # 1) Unicidad de la clave de negocio en fact_deliveries (critical: si falla, el MERGE
    #    esta produciendo duplicados y las metricas de Gold quedan infladas).
    key_cols = ["tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    n_total = fact.count()
    n_distinct_keys = fact.select(*key_cols).distinct().count()
    results.append(
        CheckResult(
            "fact_deliveries_business_key_uniqueness",
            "critical",
            n_total,
            max(n_total - n_distinct_keys, 0),
        )
    )

    # 2) cantidad_st siempre positiva (warning: valores negativos/cero ya deberian
    #    haber sido puestos en cuarentena; si aparecen aqui, hay una fuga en la logica).
    n_invalid_qty = fact.filter(F.col("cantidad_st") <= 0).count()
    results.append(
        CheckResult("fact_deliveries_cantidad_st_positive", "warning", n_total, n_invalid_qty)
    )

    # 3) Cobertura de enriquecimiento: todo registro de fact debe tener match con
    #    dim_materials (si no, el join temporal fallo -> revenue/categoria quedan nulos).
    n_unmatched = fact.filter(F.col("material_descripcion").isNull()).count()
    results.append(
        CheckResult(
            "fact_deliveries_material_enrichment_coverage", "critical", n_total, n_unmatched
        )
    )

    # 4) SCD2: exactamente una fila is_current=true por SKU en dim_materials (info:
    #    ayuda a detectar cargas de catalogo corruptas, no bloquea el pipeline).
    n_materials = dim.select("material").distinct().count()
    n_current_flags = dim.filter(F.col("is_current")).groupBy("material").count()
    n_bad_current = n_current_flags.filter(F.col("count") != 1).count()
    n_materials_without_current = n_materials - n_current_flags.count()
    results.append(
        CheckResult(
            "dim_materials_single_current_version",
            "info",
            n_materials,
            n_bad_current + n_materials_without_current,
        )
    )

    return QualityRunResult(results)


def persist_quality_logs(
    spark: SparkSession, cfg, tenant: str, layer: str, table_name: str, result: QualityRunResult
) -> None:
    run_id = str(uuid.uuid4())
    batch_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    rows = [
        Row(
            _run_id=run_id,
            _batch_id=batch_id,
            tenant_id=tenant.lower(),
            layer=layer,
            table_name=table_name,
            check_name=r.check_name,
            check_severity=r.check_severity,
            records_checked=r.records_checked,
            records_failed=r.records_failed,
            check_passed=r.passed,
            executed_at=now,
        )
        for r in result.results
    ]
    df = spark.createDataFrame(rows, schema=QUALITY_LOG_SCHEMA)

    from delta.tables import DeltaTable

    path = cfg.paths.quality_logs
    if DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("append").save(path)
    else:
        df.write.format("delta").mode("overwrite").save(path)
