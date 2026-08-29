# Infraestructura como código (Terraform) — onboarding de un tenant nuevo

## Qué provisionaría Terraform

Para dar de alta un tenant nuevo (ej. `co` — Colombia) en la plataforma SAAS sobre
Databricks + Unity Catalog + ADLS Gen2, el módulo de Terraform debería crear:

1. **Schemas en Unity Catalog** (uno por capa, dentro del catálogo del ambiente):
   `saas_<env>.bronze_co`, `saas_<env>.silver_co`, `saas_<env>.gold_co`.
2. **External locations / paths en ADLS Gen2** para cada capa, apuntando al
   contenedor del tenant (`abfss://<env>@saasdatalake.dfs.core.windows.net/bronze/co/`,
   análogo para silver/gold), con el `storage credential` correspondiente.
3. **Grants de Unity Catalog** por schema: el grupo `data-engineers-co` con
   `USE CATALOG` + `USE SCHEMA` + `SELECT`/`MODIFY` sobre su propio schema
   únicamente (aislamiento lógico, sección 5.2), y un grupo `analytics-readonly`
   con `SELECT` solo sobre `gold_co`.
4. **Secretos** (Databricks secret scope o Azure Key Vault-backed scope) para
   credenciales de la fuente operacional del tenant, si aplica (ej. conexión a
   MongoDB/Couchbase mencionada en el stack objetivo).
5. **Archivo de configuración del tenant** (`config/tenants/co.yaml`): esto no lo
   provisiona Terraform, pero el pipeline de CI del onboarding debería generarlo
   y abrir el PR automáticamente junto con el `terraform apply`.

## Snippet ilustrativo (no probado contra una cuenta real)

```hcl
# modules/tenant_onboarding/main.tf
# Snippet ilustrativo del modulo principal de onboarding de un tenant.
# No requerido que `terraform plan` funcione contra un proveedor real (seccion 7.2).

variable "tenant_code" {
  description = "Codigo de tenant en minuscula, ej. 'co'"
  type        = string
}

variable "environment" {
  description = "dev | qa | main"
  type        = string
}

variable "storage_account_name" {
  type = string
}

locals {
  layers = ["bronze", "silver", "gold"]
  catalog_name = "saas_${var.environment}"
}

resource "databricks_schema" "tenant_schema" {
  for_each     = toset(local.layers)
  catalog_name = local.catalog_name
  name         = "${each.value}_${var.tenant_code}"
  comment      = "Schema ${each.value} para tenant ${var.tenant_code}"
}

resource "azurerm_storage_container" "tenant_layer_container" {
  for_each              = toset(local.layers)
  name                  = "${each.value}-${var.tenant_code}"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

resource "databricks_external_location" "tenant_layer_location" {
  for_each        = toset(local.layers)
  name            = "${each.value}-${var.tenant_code}-loc"
  url             = "abfss://${azurerm_storage_container.tenant_layer_container[each.value].name}@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = "saas-storage-credential"
}

resource "databricks_grants" "tenant_schema_grants" {
  for_each = databricks_schema.tenant_schema
  schema   = each.value.id

  grant {
    principal  = "data-engineers-${var.tenant_code}"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
  }
}

resource "databricks_secret_scope" "tenant_scope" {
  name = "tenant-${var.tenant_code}-secrets"
}
```

## Uso

```bash
terraform apply \
  -var="tenant_code=co" \
  -var="environment=dev" \
  -var="storage_account_name=saasdatalakedev"
```

En paralelo, el equipo de datos agrega `config/tenants/co.yaml` al repo del pipeline
(ver `docs/onboarding-tenant.md`) y corre la primera carga con
`--tenant co --start-date ... --end-date ...`.
