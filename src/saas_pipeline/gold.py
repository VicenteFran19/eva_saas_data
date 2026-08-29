"""Capa Gold (seccion 5.5, 6.4). Recomputo completo por particion de fecha (no autoritativa)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saas_pipeline.config import paths_for


def build_daily_metrics_by_delivery_type(spark: SparkSession, cfg, tenant: str) -> dict:
    lp = paths_for(cfg, tenant, "fact_deliveries")
    gold_lp = paths_for(cfg, tenant, "daily_metrics_by_delivery_type")

    fact = spark.read.format("delta").load(lp.silver_table())

    metrics = (
        fact.withColumn("_tenant_id", F.col("tenant_id"))
        .groupBy("tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_st").alias("total_units"),
            F.sum(F.col("cantidad_st") * F.col("precio")).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
    )

    # Gold es derivada y no autoritativa: recompute completo por particion de fecha.
    from delta.tables import DeltaTable

    out_path = gold_lp.gold_table()
    partitions = [r["fecha_proceso"] for r in metrics.select("fecha_proceso").distinct().collect()]
    writer = metrics.write.format("delta").mode("overwrite").partitionBy("fecha_proceso")
    if partitions and DeltaTable.isDeltaTable(spark, out_path):
        pred = " OR ".join(f"fecha_proceso = '{p}'" for p in partitions)
        writer = writer.option("replaceWhere", pred)
    writer.save(out_path)

    return {"rows": metrics.count(), "path": out_path}
