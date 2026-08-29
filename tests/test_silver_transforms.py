"""Pruebas sobre las transformaciones puras de silver.py (7.1: conversion de unidades,
filtrado, manejo de anomalias, logica SCD)."""

from datetime import datetime

from pyspark.sql import Row
from saas_pipeline import silver

_FIXED_TS = datetime(2025, 1, 1, 0, 0, 0)


def _delivery_row(**overrides):
    base = dict(
        pais="SV",
        fecha_proceso="20250314",
        fecha_proceso_raw="20250314",
        transporte=111,
        ruta=222,
        tipo_entrega="ZPRE",
        material="AA004003",
        precio=10.0,
        cantidad=5.0,
        unidad="ST",
        _ingestion_timestamp=_FIXED_TS,
        _source_file="x.csv",
        _batch_id="b1",
    )
    base.update(overrides)
    return Row(**base)


def _materials_row(**overrides):
    base = dict(
        material="AA004003",
        descripcion="Cola Regular 600ml",
        categoria="BEBIDAS_GASEOSAS",
        precio_base=31.95,
        valid_from="2024-01-01",
        valid_to="9999-12-31",
        is_current=True,
    )
    base.update(overrides)
    return Row(**base)


class TestUnitConversion:
    def test_cs_converts_to_st_using_factor_20(self, spark):
        df = spark.createDataFrame([_delivery_row(unidad="CS", cantidad=3.0)])
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        row = result["clean"].collect()[0]
        assert row["cantidad_st"] == 60.0  # 3 CS * 20

    def test_st_unit_stays_unchanged(self, spark):
        df = spark.createDataFrame([_delivery_row(unidad="ST", cantidad=7.0)])
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        row = result["clean"].collect()[0]
        assert row["cantidad_st"] == 7.0


class TestFiltradoTipoEntrega:
    def test_valid_types_pass_and_flags_are_correct(self, spark):
        rows = [
            _delivery_row(tipo_entrega="ZPRE"),
            _delivery_row(tipo_entrega="ZVE1"),
            _delivery_row(tipo_entrega="Z04"),
            _delivery_row(tipo_entrega="Z05"),
        ]
        df = spark.createDataFrame(rows)
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        clean = {r["tipo_entrega"]: r for r in result["clean"].collect()}
        assert clean["ZPRE"]["is_routine_delivery"] is True
        assert clean["ZPRE"]["is_bonus_delivery"] is False
        assert clean["Z04"]["is_bonus_delivery"] is True
        assert clean["Z04"]["is_routine_delivery"] is False
        assert result["stats"]["discarded_tipo_entrega"] == 0

    def test_invalid_types_are_discarded_and_not_persisted(self, spark):
        rows = [
            _delivery_row(tipo_entrega="ZPRE"),
            _delivery_row(tipo_entrega="COBR"),
            _delivery_row(tipo_entrega="Z99"),
        ]
        df = spark.createDataFrame(rows)
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        assert result["stats"]["discarded_tipo_entrega"] == 2
        all_types = {r["tipo_entrega"] for r in result["clean"].collect()} | {
            r["tipo_entrega"] for r in result["quarantine"].collect()
        }
        assert "COBR" not in all_types
        assert "Z99" not in all_types


class TestManejoAnomalias:
    def test_negative_or_zero_cantidad_goes_to_quarantine(self, spark):
        rows = [
            _delivery_row(cantidad=-5.0),
            _delivery_row(cantidad=0.0),
            _delivery_row(cantidad=10.0),
        ]
        df = spark.createDataFrame(rows)
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        assert result["stats"]["quarantined"] == 2
        assert result["stats"]["clean_rows"] == 1
        reasons = [r["_quarantine_reason"] for r in result["quarantine"].collect()]
        assert all("invalid_cantidad" in r for r in reasons)

    def test_material_not_in_catalog_goes_to_quarantine_not_lost(self, spark):
        df = spark.createDataFrame([_delivery_row(material="XX999999")])
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        assert result["stats"]["quarantined"] == 1
        assert result["stats"]["clean_rows"] == 0
        assert result["quarantine"].collect()[0]["_quarantine_reason"] == "material_not_in_catalog"

    def test_invalid_calendar_date_is_quarantined_even_if_format_matches(self, spark):
        # '20250230' tiene formato YYYYMMDD valido pero 30 de febrero no existe.
        df = spark.createDataFrame(
            [_delivery_row(fecha_proceso="20250230", fecha_proceso_raw="20250230")]
        )
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        assert result["stats"]["quarantined"] == 1
        assert result["quarantine"].collect()[0]["_quarantine_reason"] == (
            "invalid_or_null_fecha_proceso"
        )

    def test_exact_duplicates_are_deduplicated_not_quarantined(self, spark):
        row = _delivery_row()
        df = spark.createDataFrame([row, row, row])
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        assert result["stats"]["duplicates_removed"] == 2
        assert result["stats"]["clean_rows"] == 1
        assert result["stats"]["quarantined"] == 0

    def test_no_rows_are_silently_lost(self, spark):
        rows = [
            _delivery_row(),
            _delivery_row(cantidad=-1.0),
            _delivery_row(tipo_entrega="COBR"),
            _delivery_row(material="XX999999"),
        ]
        row_dup = _delivery_row(transporte=333)
        rows += [row_dup, row_dup]
        df = spark.createDataFrame(rows)
        dim = spark.createDataFrame([_materials_row()])
        result = silver.transform_deliveries(df, dim, "sv")
        stats = result["stats"]
        accounted = (
            stats["clean_rows"]
            + stats["quarantined"]
            + stats["discarded_tipo_entrega"]
            + stats["duplicates_removed"]
        )
        assert accounted == stats["total_bronze"]


class TestSCD2:
    def test_is_current_recomputed_from_valid_to_sentinel(self, spark):
        rows = [
            _materials_row(valid_to="2025-03-31", is_current=False),
            _materials_row(material="AA004003", valid_from="2025-04-01", valid_to="9999-12-31"),
        ]
        df = spark.createDataFrame(rows)
        dim = silver.transform_dim_materials_scd2(df)
        collected = {str(r["valid_from"]): r["is_current"] for r in dim.collect()}
        assert collected["2024-01-01"] is False
        assert collected["2025-04-01"] is True

    def test_temporal_join_uses_version_valid_at_transaction_date_not_current(self, spark):
        # El material tuvo un precio_base distinto antes y despues del 2025-04-01.
        dim_rows = [
            _materials_row(
                valid_from="2024-01-01", valid_to="2025-03-31",
                precio_base=31.95, is_current=False,
            ),
            _materials_row(
                valid_from="2025-04-01", valid_to="9999-12-31",
                precio_base=33.80, is_current=True,
            ),
        ]
        dim = silver.transform_dim_materials_scd2(spark.createDataFrame(dim_rows))

        # Transaccion ocurre en enero 2025: debe enriquecerse con la version VIEJA
        # (31.95), no con la version actual (33.80), aunque is_current apunte a la nueva.
        txn = _delivery_row(fecha_proceso="20250115", fecha_proceso_raw="20250115")
        result = silver.transform_deliveries(spark.createDataFrame([txn]), dim, "sv")
        enriched = silver.enrich_with_materials_temporal(result["clean"], dim)
        row = enriched.collect()[0]
        assert row["material_precio_base"] == 31.95

    def test_only_one_current_version_per_sku_after_scd_transform(self, spark):
        rows = [
            _materials_row(valid_from="2024-01-01", valid_to="2025-03-31", is_current=False),
            _materials_row(valid_from="2025-04-01", valid_to="9999-12-31", is_current=True),
            _materials_row(
                material="AA004007", valid_from="2024-01-01",
                valid_to="9999-12-31", is_current=True,
            ),
        ]
        dim = silver.transform_dim_materials_scd2(spark.createDataFrame(rows))
        current_counts = (
            dim.filter(dim.is_current)
            .groupBy("material")
            .count()
            .collect()
        )
        for r in current_counts:
            assert r["count"] == 1
