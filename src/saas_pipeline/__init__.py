"""Pipeline SAAS: ingesta multi-tenant Bronze/Silver/Gold sobre Spark + Delta Lake."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "saas-pipeline") -> SparkSession:
    """Crea una SparkSession con soporte Delta Lake.

    Funciona tanto en ejecucion local (Spark standalone) como en Databricks:
    en Databricks el runtime ya trae Delta configurado, por lo que estas
    configuraciones son inocuas (no se pisan builders existentes en notebooks).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass  # En Databricks Runtime, Delta ya viene instalado y configurado.

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
