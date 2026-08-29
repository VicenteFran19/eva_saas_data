# Onboarding de un tenant nuevo

Pasos para dar de alta un tenant nuevo (ej. `co`) en el pipeline, sin tocar código:

1. **Infraestructura** (una vez, vía Terraform): ver `docs/infra.md`. Crea los
   schemas de Unity Catalog, los paths en ADLS y los grants.

2. **Config del pipeline**: crear `config/tenants/co.yaml`:
   ```yaml
   tenant:
     code: "co"
   ```
   Si el tenant necesita overrides específicos (ej. un `cs_to_st_factor` distinto,
   poco probable pero soportado por la estructura jerárquica), se agregan aquí.

3. **Validar que el país/tenant existe en la fuente**: el CLI filtra
   `pais == tenant.upper()` sobre el CSV/fuente de deliveries. Si el tenant nuevo
   usa un código de país distinto al de 2 letras usado hoy (`SV`, `HN`, `EC`, `JM`,
   `PE`, `GT`), habría que confirmarlo con el equipo de origen de datos.

4. **Primera carga**:
   ```bash
   python -m saas_pipeline.cli --env dev --tenant co \
     --start-date 2025-01-01 --end-date 2025-06-30
   ```
   Esto corre Bronze → Silver (fact + dim) → Quality → Gold solo para `co`, sin
   afectar a los demás tenants (aislamiento por path/schema, sección 5.2).

5. **Incluir el tenant en corridas `--tenant all`**: no requiere ningún cambio
   adicional — `list_available_tenants()` en `config.py` detecta automáticamente
   cualquier archivo nuevo en `config/tenants/`.

6. **Monitoreo**: revisar `data/shared/quality_logs/` filtrando por
   `tenant_id = 'co'` para confirmar que las validaciones de calidad corrieron
   correctamente en la primera carga.
