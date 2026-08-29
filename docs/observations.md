# Observaciones a la arquitectura provista

Documento obligatorio (sección 9.2). No se modificó la arquitectura unilateralmente;
cada punto de desacuerdo u observación se registra aquí para discutirse en la
sustentación.

## 1. Catálogo de materiales replicado por tenant, en vez de compartido

**Decisión de la arquitectura provista:** aislamiento estricto por schema/tenant
(sección 5.2) aplicado uniformemente a toda tabla, incluyendo `dim_materials`.

**Con lo que no estoy de acuerdo:** `materials_catalog` es, por su naturaleza, data
de **referencia global** (un mismo SKU con el mismo precio_base y las mismas fechas
de vigencia aplica a las 6 unidades de negocio). Replicarlo dentro del schema de
cada tenant (`saas_dev.silver_sv.dim_materials`, `saas_dev.silver_hn.dim_materials`,
etc.) significa:

- 6 copias idénticas de la misma dimensión, que hay que mantener sincronizadas.
- Si un tenant recibe una actualización de catálogo y otro no (por una corrida
  parcial, un `--tenant sv` en vez de `--tenant all`), los tenants quedan
  **desincronizados silenciosamente** — el mismo material podría enriquecerse con
  precio_base distinto según el tenant, sin que nada lo detecte.

**Propuesta alternativa:** un catálogo compartido en `saas_<env>.shared.dim_materials`
(mismo patrón que ya se usa para `quality_logs`, sección 5.2/5.9), con el join
temporal de la sección 5.7 aplicado igual, pero desde una única fuente de verdad.

**Trade-off:** rompe la simetría "todo vive dentro del schema del tenant", que sí
tiene valor para el aislamiento de grants en Unity Catalog. Si el governance exige
que cada tenant solo pueda leer su propio schema (incluida su copia de la
dimensión), la replicación actual es defendible como precio de ese aislamiento.
Lo dejo así en la implementación (siguiendo la arquitectura tal como está escrita)
mientras se decide en la sustentación.

## 2. Ambigüedad: fechas con formato válido pero calendario inválido

**Ambigüedad en la arquitectura provista:** la sección 5.6 define la política para
"fecha_proceso nula o inválida" → cuarentena, pero no especifica qué significa
"inválida": ¿solo formato (`no matchea YYYYMMDD`), o también validez de calendario?

**Cómo la resolví:** al inspeccionar el dataset encontré casos que pasan una
validación de solo-formato pero no son fechas reales: `20250230` (30 de febrero no
existe), `20251332` (mes 13) y `00000000`. Si solo se valida el formato con una
regex `^\d{8}$`, estas 3 filas se cuelan como "válidas" y en Bronze terminan
escritas en particiones sin sentido (`fecha_proceso=00000000`), y en Silver el join
temporal contra `dim_materials` falla silenciosamente (no hay ninguna versión del
catálogo vigente en el año cero), produciendo filas con `material_descripcion` nula
que **no estaban siendo detectadas** por la regla de "material no en catálogo".

Implementé la validación como **formato + parseable como fecha de calendario real**
(`to_date(fecha_proceso, 'yyyyMMdd') IS NOT NULL`), lo cual sí manda estas 3 filas a
cuarentena con el motivo `invalid_or_null_fecha_proceso`. Sin este fix, 3 filas por
tenant (aprox.) quedaban en Silver con datos de enriquecimiento incompletos sin
ninguna señal de alerta — el tipo de bug silencioso que la prueba explícitamente
penaliza (sección 11: "materiales... perdidos silenciosamente").

**Ambigüedad relacionada, también resuelta aquí:** en Bronze, que debe particionar
por `fecha_proceso` (5.4) pero es "solo lectura, sin transformación" (5.1), una fecha
inválida no puede mandarse a cuarentena todavía (esa es una decisión de negocio de
Silver). Se resolvió preservando el valor crudo en `fecha_proceso_raw` y usando una
partición centinela `fecha_proceso="invalid"` en Bronze, para no perder el dato ni
adelantar lógica de negocio a una capa que no debería tenerla.

## 3. Mejora tecnológica propuesta: `quality_logs` sin retención ni compactación

**Situación actual:** `quality_logs` se escribe en modo `append` puro (sección 5.9),
sin política de retención ni compactación. En producción, con corridas diarias por
6 tenants y ~4 checks por corrida, esto genera decenas de miles de archivos pequeños
Delta al cabo de un año, degradando performance de lectura (small-file problem).

**Propuesta para Horizonte 2-3:**
1. Job de mantenimiento semanal con `OPTIMIZE` + `VACUUM` sobre `quality_logs` y
   sobre las tablas Silver/Gold con alta frecuencia de `MERGE`.
2. Particionar `quality_logs` por `layer` + mes de `executed_at` (hoy no está
   particionada en absoluto), ya que las consultas típicas de un dashboard de
   observabilidad de calidad filtran por rango de fechas reciente.
3. Agregar una vista `quality_logs_latest` (última corrida por `check_name` +
   `tenant_id` + `table_name`) para que un dashboard no tenga que escanear el
   histórico completo en cada refresh.

## 4. Observación adicional: `is_current` de origen no es confiable por sí solo

El CSV de `materials_catalog` ya trae una columna `is_current`, pero nada garantiza
que el sistema origen la mantenga consistente (por ejemplo, ante una corrección de
histórico que actualice `valid_to` sin recalcular `is_current` en versiones previas).
En la implementación, `is_current` se **recalcula** en Silver a partir de
`valid_to = '9999-12-31'` en vez de propagar el flag de origen tal cual, y se agregó
una validación de calidad (`dim_materials_single_current_version`, severidad `info`)
que detecta si algún SKU queda con 0 o más de 1 versión vigente. Esto es más robusto
que confiar en el flag de origen, que es precisamente el anti-patrón que la sección
11 advierte evitar ("SCD Type 2 implementado usando solo `is_current`").
