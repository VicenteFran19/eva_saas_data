"""Fixtures compartidos.

Los tests corren sobre PySpark local puro (sin Delta): las funciones `transform_*`
de silver.py son puras (DataFrame -> DataFrame) y no requieren el jar de Delta, lo
que permite correrlas en cualquier entorno de CI sin acceso a Maven Central.
Las operaciones de I/O con Delta (MERGE INTO, etc.) se ejercitan en la sustentación
contra un entorno con el jar disponible (local con `--packages` o Databricks).
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("saas-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
