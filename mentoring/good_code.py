"""Refactor de bad_code.py: procesamiento de deliveries con Spark nativo.

Corrige los 5 puntos de code_review.md:
  1. Spark nativo en vez de pandas + iterrows().
  2. Constantes de negocio con nombre, no numeros magicos inline.
  3. Validaciones explicitas (cantidad, material, fecha) antes de calcular metricas.
  4. Escritura idempotente con Delta (particion + replaceWhere), no overwrite total.
  5. Naming consistente en ingles, type hints, tenant parametrizable via CLI.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_ROUTINE_TIPOS = ["ZPRE", "ZVE1"]
CS_TO_ST_FACTOR = 20


def get_spark(app_name: str = "deliveries-processing") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_deliveries(spark: SparkSession, file_path: str, tenant: str) -> DataFrame:
    """Lee el CSV directamente con Spark (sin pandas) y filtra por tenant."""
    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
    return df.filter(F.col("pais") == tenant)


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa filas validas de invalidas. No descarta silenciosamente: retorna ambas."""
    is_valid = (
        F.col("cantidad").isNotNull()
        & (F.col("cantidad") > 0)
        & F.col("material").isNotNull()
        & F.col("fecha_proceso").isNotNull()
    )
    valid = df.filter(is_valid)
    invalid = df.filter(~is_valid)
    return valid, invalid


def compute_routine_deliveries(df: DataFrame) -> DataFrame:
    """Filtra entregas de rutina y normaliza unidades a ST, vectorizado (sin iterrows)."""
    routine = df.filter(F.col("tipo_entrega").isin(VALID_ROUTINE_TIPOS))
    return routine.withColumn(
        "cantidad_st",
        F.when(F.col("unidad") == "CS", F.col("cantidad") * CS_TO_ST_FACTOR).otherwise(
            F.col("cantidad")
        ),
    ).withColumn("total", F.col("cantidad_st") * F.col("precio")).select(
        "pais", F.col("fecha_proceso").alias("fecha"), "material", "cantidad_st", "total"
    )


def write_idempotent(df: DataFrame, output_path: str, tenant: str) -> None:
    """Escritura particionada + replaceWhere: reprocesar un tenant no borra otros,
    y reprocesar el mismo tenant no deja archivos huerfanos de corridas previas."""
    from delta.tables import DeltaTable

    writer = df.write.format("delta").partitionBy("pais").mode("overwrite")
    if DeltaTable.isDeltaTable(df.sparkSession, output_path):
        writer = writer.option("replaceWhere", f"pais = '{tenant}'")
    writer.save(output_path)


def process(file_path: str, tenant: str, output_path: str = "/tmp/output") -> DataFrame:
    spark = get_spark()
    raw = load_deliveries(spark, file_path, tenant)
    valid, invalid = validate(raw)

    n_invalid = invalid.count()
    if n_invalid:
        print(f"[warn] {n_invalid} filas invalidas descartadas de la validacion basica "
              f"(cantidad/material/fecha nulos o cantidad <= 0) para tenant={tenant}")

    result = compute_routine_deliveries(valid)
    write_idempotent(result, output_path, tenant)
    print(f"done: {result.count()} filas escritas para tenant={tenant}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Procesa deliveries de rutina por tenant")
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--tenant", required=True, help="Codigo de pais/tenant, ej. GT")
    parser.add_argument("--output-path", default="/tmp/output")
    args = parser.parse_args()
    process(args.file_path, args.tenant, args.output_path)


if __name__ == "__main__":
    main()
