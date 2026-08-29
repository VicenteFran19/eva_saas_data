# Diagrama de arquitectura

Diagrama de referencia (sección 5.10 del enunciado), reproducido aquí para que
viva versionado dentro del repositorio junto con el resto de la documentación.

\`\`\`
┌──────────────┐
│  RAW (CSV)   │
└──────┬───────┘
       │  overwrite por partición
       ▼
┌─────────────────────────────────┐
│ BRONZE: <tenant>.deliveries     │  ← Delta, particionado fecha+tenant
└──────┬──────────────────────────┘
       │  MERGE INTO (clave de negocio)
       ▼
┌─────────────────────────────────┐  ┌────────────────────────────┐
│ SILVER:                         │  │ SHARED:                    │
│   <tenant>.fact_deliveries      │──┤   quality_logs             │
│   <tenant>.dim_materials (SCD2) │  │   quarantine_<table>       │
└──────┬──────────────────────────┘  └────────────────────────────┘
       │  recompute por partición
       ▼
┌─────────────────────────────────┐
│ GOLD:                           │
│   <tenant>.daily_metrics_*      │
└─────────────────────────────────┘
\`\`\`

## Flujo de aislamiento multi-tenant

\`\`\`
data/
├── bronze/<tenant>/<table>/fecha_proceso=YYYYMMDD/
├── bronze_quarantine/<tenant>/<table>/
├── silver/<tenant>/<table>/fecha_proceso=YYYYMMDD/
├── silver_quarantine/<tenant>/<table>/
├── gold/<tenant>/<table>/
└── shared/quality_logs/
\`\`\`

Cada tenant (sv, hn, ec, jm, pe, gt) tiene su propio subárbol aislado en cada
capa, reflejando en local la separación por schema que tendría en Unity Catalog.

## Flujo de calidad de datos

\`\`\`
Bronze (crudo)
    │
    ▼
filter_valid_tipo_entrega ──► descartados (no persistidos, solo contados)
    │
    ▼
deduplicate_exact ──► duplicados removidos (no persistidos)
    │
    ▼
split_quarantine ──► silver_quarantine/<tenant>/fact_deliveries (_quarantine_reason)
    │
    ▼
normalize_and_flag + enrich_with_materials_temporal (SCD2, join temporal)
    │
    ▼
silver/<tenant>/fact_deliveries (MERGE INTO)
    │
    ▼
run_silver_checks ──► shared/quality_logs (4 validaciones, severidad, timestamp)
    │
    ▼ (si fail_on_critical=true y alguna critical falló → abortar aquí)
Gold: daily_metrics_by_delivery_type
\`\`\`