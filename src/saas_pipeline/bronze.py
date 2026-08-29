"""Capa Bronze (seccion 5, 6.2).

Ingesta el CSV crudo a Delta preservando el esquema original + columnas tecnicas.
Particionado por fecha_proceso y tenant_id. Idempotente via overwrite por particion
(replaceWhere). No aplica reglas de negocio ni descarta filas: esa logica vive en Silver.

Ambiguedad resuelta aqui (ver docs/observations.md #2): fecha_proceso viene como
string crudo y puede ser nula o invalida. Como Bronze debe particionar por esta
columna (5.4) pero "solo lectura / sin transformacion" (5.1), no podemos enviar esas
filas a cuarentena en esta capa (la cuarentena es una decision de negocio, propia de
Silver). Se preserva el valor crudo tal cual llega en una columna nueva
`fecha_proceso_raw`, y se deriva `fecha_proceso` como partición: si el valor no matchea
YYYYMMDD se bucketiza en la particion especial "invalid", sin perder el dato.
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.config import paths_for

RAW_DELIVERIES_PATH = "data/raw/global_mobility_data_entrega_productos.csv"
RAW_MATERIALS_PATH = "data/raw/materials_catalog.csv"

VALID_DATE_RE = r"^\d{8}$"


def _is_calendar_valid_date_col(col_name: str):
    """True si el string YYYYMMDD es una fecha de calendario real (no solo 8 digitos).

    Detecta casos como '20250230' (30 de febrero no existe), '20251332' (mes 13) o
    '00000000', que pasan una validacion de solo-regex pero no son fechas validas.
    """
    return F.col(col_name).rlike(VALID_DATE_RE) & F.to_date(
        F.col(col_name), "yyyyMMdd"
    ).isNotNull()


def _with_technical_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_deliveries(
    spark: SparkSession, cfg, tenant: str, start_date: str, end_date: str
) -> str:
    """Ingesta deliveries crudos para un tenant. Retorna el path Delta escrito."""
    batch_id = str(uuid.uuid4())
    raw = spark.read.option("header", True).csv(RAW_DELIVERIES_PATH)

    tenant_upper = tenant.upper()
    df = raw.filter(F.col("pais") == tenant_upper)
    df = df.withColumn("_tenant_id", F.lower(F.col("pais")))

    # Particion derivada: valida YYYYMMDD -> se usa tal cual; invalida/nula -> "invalid".
    df = df.withColumn(
        "fecha_proceso_raw", F.col("fecha_proceso")
    ).withColumn(
        "fecha_proceso",
        F.when(
            _is_calendar_valid_date_col("fecha_proceso"), F.col("fecha_proceso")
        ).otherwise(F.lit("invalid")),
    )

    start_str = start_date.replace("-", "")
    end_str = end_date.replace("-", "")
    df = df.filter(
        (F.col("fecha_proceso") == "invalid")
        | ((F.col("fecha_proceso") >= start_str) & (F.col("fecha_proceso") <= end_str))
    )

    df = _with_technical_columns(df, RAW_DELIVERIES_PATH, batch_id)

    lp = paths_for(cfg, tenant, "deliveries")
    out_path = lp.bronze_table()

    partitions_touched = [
        r["fecha_proceso"] for r in df.select("fecha_proceso").distinct().collect()
    ]

    from delta.tables import DeltaTable

    writer = df.write.format("delta").partitionBy("fecha_proceso").mode("overwrite")
    if partitions_touched and DeltaTable.isDeltaTable(spark, out_path):
        # Overwrite solo de las particiones tocadas por esta corrida (idempotencia, 5.5).
        pred = " OR ".join(f"fecha_proceso = '{p}'" for p in partitions_touched)
        writer = writer.option("replaceWhere", pred)
    writer.save(out_path)

    return out_path


def ingest_materials(spark: SparkSession, cfg, tenant: str) -> str:
    """Ingesta el catalogo de materiales crudo para un tenant.

    Nota (ver docs/observations.md #1): el catalogo de materiales es data de
    referencia global, no por-pais. La arquitectura provista, sin embargo, define
    aislamiento estricto por schema/tenant (5.2) sin distinguir entre tablas
    transaccionales y catalogos compartidos. Se replica el catalogo dentro del
    schema de cada tenant para respetar la convencion tal como esta escrita,
    documentando la alternativa (catalogo unico compartido) como observacion.
    """
    batch_id = str(uuid.uuid4())
    raw = spark.read.option("header", True).csv(RAW_MATERIALS_PATH)
    df = raw.withColumn("_tenant_id", F.lit(tenant.lower()))
    df = _with_technical_columns(df, RAW_MATERIALS_PATH, batch_id)

    lp = paths_for(cfg, tenant, "materials_catalog")
    out_path = lp.bronze_table()
    df.write.format("delta").mode("overwrite").save(out_path)
    return out_path
