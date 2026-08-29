"""Capa Silver (seccion 5.5, 5.6, 5.7, 6.3).

fact_deliveries: normalizada, enriquecida, con MERGE INTO idempotente por clave de
negocio compuesta. dim_materials: SCD Type 2 con MERGE INTO por (material, valid_from).
Anomalias manejadas segun la matriz de 5.6.

Diseño: las funciones `transform_*` son puras (DataFrame -> DataFrame), sin I/O ni
dependencia de Delta, para poder probarlas de forma aislada (ver tests/). Las
funciones `build_*` orquestan I/O (lectura de Bronze, MERGE INTO a Silver) y son las
que se llaman desde el CLI.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

from saas_pipeline.config import paths_for

BUSINESS_KEY_FACT = ["tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
VALID_TIPO_ENTREGA = ["ZPRE", "ZVE1", "Z04", "Z05"]
ROUTINE_TYPES = ["ZPRE", "ZVE1"]
BONUS_TYPES = ["Z04", "Z05"]
CS_TO_ST_FACTOR = 20

RAW_DEDUP_COLUMNS = [
    "pais", "fecha_proceso_raw", "transporte", "ruta", "tipo_entrega",
    "material", "precio", "cantidad", "unidad",
]


def cast_deliveries(df: DataFrame) -> DataFrame:
    """Castea columnas numericas leidas como string desde el CSV."""
    return (
        df.withColumn("cantidad", F.col("cantidad").cast(DoubleType()))
        .withColumn("precio", F.col("precio").cast(DoubleType()))
        .withColumn("transporte", F.col("transporte").cast(LongType()))
        .withColumn("ruta", F.col("ruta").cast(LongType()))
    )


def filter_valid_tipo_entrega(df: DataFrame) -> tuple[DataFrame, int]:
    """Descarta (sin persistir) filas con tipo_entrega fuera de las 4 categorias validas.

    Retorna (df_en_alcance, cantidad_descartada).
    """
    n_before = df.count()
    in_scope = df.filter(F.col("tipo_entrega").isin(VALID_TIPO_ENTREGA))
    return in_scope, n_before - in_scope.count()


def deduplicate_exact(df: DataFrame) -> tuple[DataFrame, int]:
    """Elimina duplicados exactos (misma fila de negocio repetida). Retorna (df, n_removidos)."""
    n_before = df.count()
    cols = [c for c in RAW_DEDUP_COLUMNS if c in df.columns]
    deduped = df.dropDuplicates(cols)
    return deduped, n_before - deduped.count()


def split_quarantine(
    df: DataFrame, materials_catalog: DataFrame, tenant: str
) -> tuple[DataFrame, DataFrame]:
    """Separa filas validas de filas en cuarentena segun la matriz de anomalias (5.6).

    `materials_catalog` debe tener una columna `material` (SKUs vigentes en el catalogo).
    Retorna (clean_df, quarantine_df). quarantine_df incluye `_quarantine_reason`.
    """
    cat = materials_catalog.select(F.col("material").alias("cat_material")).distinct()

    # Fecha valida = formato YYYYMMDD Y fecha de calendario real (descarta casos como
    # '20250230', '20251332' o '00000000', que pasan una regex pero no son fechas
    # reales; ver docs/observations.md #2).
    is_valid_date = F.col("fecha_proceso").rlike(r"^\d{8}$") & F.to_date(
        F.col("fecha_proceso"), "yyyyMMdd"
    ).isNotNull()
    is_valid_cantidad = F.col("cantidad").isNotNull() & (F.col("cantidad") > 0)
    is_valid_precio = F.col("precio").isNotNull()

    checked = df.join(cat, df.material == cat.cat_material, "left").withColumn(
        "_material_in_catalog", F.col("cat_material").isNotNull()
    )

    # Nota: array_remove(arr, NULL) siempre retorna NULL en Spark (no filtra nulls),
    # por eso se usa la funcion de orden superior `filter` para descartar los
    # elementos NULL del array de razones de cuarentena.
    raw_reasons = F.array(
        F.when(~is_valid_date, F.lit("invalid_or_null_fecha_proceso")),
        F.when(~is_valid_cantidad, F.lit("invalid_cantidad")),
        F.when(~F.col("_material_in_catalog"), F.lit("material_not_in_catalog")),
        F.when(~is_valid_precio, F.lit("null_precio")),
    )
    reasons = F.filter(raw_reasons, lambda x: x.isNotNull())
    checked = checked.withColumn("_quarantine_reasons", reasons)

    quarantine = (
        checked.filter(F.size("_quarantine_reasons") > 0)
        .withColumn("_quarantine_reason", F.array_join("_quarantine_reasons", "|"))
        .withColumn("_tenant_id", F.lit(tenant.lower()))
        .drop("cat_material", "_material_in_catalog", "_quarantine_reasons")
    )
    clean = checked.filter(F.size("_quarantine_reasons") == 0).drop(
        "cat_material", "_material_in_catalog", "_quarantine_reasons"
    )
    return clean, quarantine


def normalize_and_flag(df: DataFrame, tenant: str) -> DataFrame:
    """Normaliza CS->ST y agrega flags de negocio + tenant_id + fecha tipada."""
    df = df.withColumn(
        "cantidad_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * CS_TO_ST_FACTOR).otherwise(
            F.col("cantidad")
        ),
    )
    df = df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(ROUTINE_TYPES))
    df = df.withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(BONUS_TYPES))
    df = df.withColumn("fecha_proceso_date", F.to_date(F.col("fecha_proceso"), "yyyyMMdd"))
    df = df.withColumn("_tenant_id", F.lit(tenant.lower()))
    return df


def enrich_with_materials_temporal(fact: DataFrame, dim: DataFrame) -> DataFrame:
    """Join temporal fact.fecha_proceso_date BETWEEN dim.valid_from AND dim.valid_to (5.7).

    No usa is_current: la version aplicable es la vigente A LA FECHA de la transaccion.
    """
    enriched = (
        fact.alias("f")
        .join(
            dim.alias("d"),
            (F.col("f.material") == F.col("d.material"))
            & (F.col("f.fecha_proceso_date") >= F.col("d.valid_from"))
            & (F.col("f.fecha_proceso_date") <= F.col("d.valid_to")),
            "left",
        )
        .select(
            F.col("f._tenant_id").alias("tenant_id"),
            "f.fecha_proceso", "f.fecha_proceso_date", "f.transporte",
            "f.ruta", "f.tipo_entrega", "f.material", "f.precio", "f.cantidad",
            "f.unidad", "f.cantidad_st", "f.is_routine_delivery", "f.is_bonus_delivery",
            F.col("d.descripcion").alias("material_descripcion"),
            F.col("d.categoria").alias("material_categoria"),
            F.col("d.precio_base").alias("material_precio_base"),
            "f._ingestion_timestamp", "f._source_file", "f._batch_id",
        )
    )
    return enriched


def transform_deliveries(bronze_df: DataFrame, materials_catalog: DataFrame, tenant: str) -> dict:
    """Pipeline completo de transformacion Silver (puro, sin I/O). Retorna dict con
    'clean', 'quarantine' (DataFrames) y 'stats' (conteos)."""
    casted = cast_deliveries(bronze_df)
    in_scope, n_discarded = filter_valid_tipo_entrega(casted)
    deduped, n_dup = deduplicate_exact(in_scope)
    clean, quarantine = split_quarantine(deduped, materials_catalog, tenant)
    clean = normalize_and_flag(clean, tenant)

    stats = {
        "total_bronze": bronze_df.count(),
        "discarded_tipo_entrega": n_discarded,
        "duplicates_removed": n_dup,
        "quarantined": quarantine.count(),
        "clean_rows": clean.count(),
    }
    return {"clean": clean, "quarantine": quarantine, "stats": stats}


def transform_dim_materials_scd2(materials_bronze: DataFrame) -> DataFrame:
    """Tipa y de-duplica versiones del catalogo; recalcula is_current desde valid_to."""
    return (
        materials_bronze.withColumn("precio_base", F.col("precio_base").cast(DoubleType()))
        .withColumn("valid_from", F.to_date("valid_from"))
        .withColumn("valid_to", F.to_date("valid_to"))
        .withColumn("is_current", F.col("valid_to") == F.lit("9999-12-31").cast("date"))
        .select(
            "material", "descripcion", "categoria", "precio_base",
            "valid_from", "valid_to", "is_current",
        )
        .dropDuplicates(["material", "valid_from"])
    )


# ---------------------------------------------------------------------------
# Orquestacion con I/O (Delta). Se importa Delta dentro de las funciones para
# que el modulo siga siendo importable (y sus funciones `transform_*` testeables)
# en entornos donde el jar de Delta no este disponible (ej. CI sin acceso a Maven).
# ---------------------------------------------------------------------------

def _delta_table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def build_dim_materials(spark: SparkSession, cfg, tenant: str) -> dict:
    from delta.tables import DeltaTable

    lp = paths_for(cfg, tenant, "dim_materials")
    bronze_lp = paths_for(cfg, tenant, "materials_catalog")

    src = transform_dim_materials_scd2(spark.read.format("delta").load(bronze_lp.bronze_table()))
    target_path = lp.silver_table()
    rows_in = src.count()

    if _delta_table_exists(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(src.alias("s"), "t.material = s.material AND t.valid_from = s.valid_from")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        src.write.format("delta").save(target_path)

    return {"rows_processed": rows_in, "path": target_path}


def build_fact_deliveries(spark: SparkSession, cfg, tenant: str) -> dict:
    from delta.tables import DeltaTable

    # bronze_lp: lee de bronze/<tenant>/deliveries (nombre de ingesta cruda, sin prefijo).
    # fact_lp: escribe/lee de silver/<tenant>/fact_deliveries (prefijo fact_, seccion 5.3).
    # Son nombres de tabla distintos a proposito -- no reusar el mismo LayerPaths para ambos.
    bronze_lp = paths_for(cfg, tenant, "deliveries")
    fact_lp = paths_for(cfg, tenant, "fact_deliveries")
    dim_lp = paths_for(cfg, tenant, "dim_materials")

    bronze_df = spark.read.format("delta").load(bronze_lp.bronze_table())
    dim = spark.read.format("delta").load(dim_lp.silver_table())

    result = transform_deliveries(bronze_df, dim, tenant)
    clean, quarantine, stats = result["clean"], result["quarantine"], result["stats"]

    enriched = enrich_with_materials_temporal(clean, dim)

    target_path = fact_lp.silver_table()
    if _delta_table_exists(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in BUSINESS_KEY_FACT)
        (
            target.alias("t")
            .merge(enriched.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched.write.format("delta").partitionBy("fecha_proceso").save(target_path)

    quarantine_path = fact_lp.silver_quarantine()
    partitions = [
        r["fecha_proceso"] for r in quarantine.select("fecha_proceso").distinct().collect()
    ]
    writer = quarantine.write.format("delta").mode("overwrite").partitionBy("fecha_proceso")
    if partitions and _delta_table_exists(spark, quarantine_path):
        pred = " OR ".join(f"fecha_proceso = '{p}'" for p in partitions)
        writer = writer.option("replaceWhere", pred)
    writer.save(quarantine_path)

    stats["path"] = target_path
    return stats