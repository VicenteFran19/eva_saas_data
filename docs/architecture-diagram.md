# Diagrama de arquitectura

Diagrama de referencia (sección 5.10 del enunciado), reproducido aquí para que
viva versionado dentro del repositorio junto con el resto de la documentación.

```
RAW (CSV)
    |
    | overwrite por particion
    v
BRONZE: <tenant>.deliveries          (Delta, particionado fecha + tenant)
    |
    | MERGE INTO (clave de negocio)
    v
SILVER: <tenant>.fact_deliveries          SHARED: quality_logs
        <tenant>.dim_materials (SCD2)     SHARED: quarantine_<table>
    |
    | recompute por particion
    v
GOLD: <tenant>.daily_metrics_by_delivery_type
```

## Aislamiento multi-tenant (paths locales)

```
data/
  bronze/<tenant>/<table>/fecha_proceso=YYYYMMDD/
  bronze_quarantine/<tenant>/<table>/
  silver/<tenant>/<table>/fecha_proceso=YYYYMMDD/
  silver_quarantine/<tenant>/<table>/
  gold/<tenant>/<table>/
  shared/quality_logs/
```

Cada tenant (sv, hn, ec, jm, pe, gt) tiene su propio subárbol aislado en cada
capa, reflejando en local la separación por schema que tendría en Unity Catalog.

## Flujo de calidad de datos

```
Bronze (crudo)
  -> filter_valid_tipo_entrega    (descartados: no persistidos, solo contados)
  -> deduplicate_exact            (duplicados: no persistidos)
  -> split_quarantine             (a silver_quarantine, con _quarantine_reason)
  -> normalize_and_flag + enrich_with_materials_temporal (SCD2, join temporal)
  -> MERGE INTO silver.fact_deliveries
  -> run_silver_checks            (a shared/quality_logs)
  -> [si fail_on_critical=true y alguna critical fallo: abortar aqui]
  -> Gold: daily_metrics_by_delivery_type
```
