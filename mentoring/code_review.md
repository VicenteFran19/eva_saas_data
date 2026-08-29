# Code Review — `bad_code.py`

## 1. Pandas + `iterrows()` en vez de Spark nativo

**Qué está mal:** el código importa `SparkSession` pero usa `pandas` para leer el
CSV y `iterrows()` para procesar fila por fila, y solo al final convierte a Spark
para escribir. Esto anula por completo el propósito de usar Spark.

**Por qué importa:** `iterrows()` es la operación más lenta posible sobre un
DataFrame — procesa una fila de Python a la vez, sin vectorización. Con 3,100
filas no se nota, pero con los volúmenes reales del proyecto SAAS (múltiples
tenants, múltiples años) esto no escala: pandas carga todo en memoria en el driver,
y el "procesamiento distribuido" solo ocurre en la escritura final, cuando ya es
tarde.

**Cómo se corrige:** leer directamente con `spark.read.csv(...)` y expresar toda
la lógica (filtro, cálculo de `cantidad_st`, `total`) con las funciones nativas de
`pyspark.sql.functions` (`when`, `col`, operaciones vectorizadas), como se hace en
`silver.py` de este repo (`cast_deliveries`, `normalize_and_flag`).

## 2. Lógica de negocio (tipos válidos, factor de conversión) hardcodeada e inline

**Qué está mal:** `"ZPRE"`, `"ZVE1"` y el factor `20` están escritos directamente
dentro del `if`, repetidos y sin nombre.

**Por qué importa:** si el factor de conversión cambia (o si mañana se agrega un
tercer tipo de entrega válido), hay que ir a buscar el número mágico dentro de la
lógica de control de flujo, en vez de cambiarlo en un solo lugar. Además, no hay
forma de testear "¿cuáles son los tipos de entrega válidos?" de forma aislada.

**Cómo se corrige:** extraer constantes con nombre (`VALID_TIPO_ENTREGA`,
`CS_TO_ST_FACTOR`) al nivel de módulo o de configuración (ver `config/base.yaml`
en este repo, sección `business_rules`), y referenciarlas — nunca literales sueltos
dentro de la lógica de negocio.

## 3. Ausencia total de validaciones y manejo de errores

**Qué está mal:** no hay ningún control sobre `cantidad` nula/negativa, `material`
inexistente en un catálogo, `fecha_proceso` inválida, ni sobre qué pasa si el CSV
no existe o tiene columnas distintas a las esperadas.

**Por qué importa:** el pipeline de la prueba encontró ~4% de anomalías reales en
el dataset (fechas inválidas, cantidades negativas, materiales fuera de catálogo).
Sin ninguna validación, esas filas se cuelan silenciosamente en el resultado final
y contaminan métricas de negocio (revenue, unidades) sin que nadie lo note —
exactamente el escenario que la sección 11 de la prueba marca como penalizado
("materiales... perdidos silenciosamente").

**Cómo se corrige:** aplicar una capa explícita de validación/cuarentena antes de
calcular métricas (ver `silver.split_quarantine` en este repo), con razones de
cuarentena persistidas y auditables, en vez de dejar que las filas malas fluyan
sin marca alguna.

## 4. Escritura no idempotente (`overwrite` de todo el path por país)

**Qué está mal:** `sdf.write.mode("overwrite").parquet("/tmp/output/" + country)`
sobreescribe **todo** el directorio del país en cada corrida, sin ningún concepto
de partición por fecha ni de rango de reproceso.

**Por qué importa:** si se necesita reprocesar solo un día específico (por
ejemplo, porque llegó una corrección de datos de una sola fecha), esta escritura
obliga a reprocesar y sobreescribir el histórico completo del país, con riesgo de
perder datos si el job falla a mitad de camino (no hay atomicidad — Parquet plano
no tiene transacciones).

**Cómo se corrige:** usar Delta Lake con particionado por fecha y `overwrite` con
`replaceWhere` acotado a la partición reprocesada (Bronze) o `MERGE INTO` con clave
de negocio (Silver), como se implementa en `bronze.ingest_deliveries` y
`silver.build_fact_deliveries` de este repo. Esto da atomicidad (ACID) y permite
reprocesar rangos acotados sin tocar el resto del histórico.

## 5. Naming inconsistente y falta de tipado (observación adicional)

**Qué está mal:** mezcla español/inglés (`process`, `file_path`, pero `pais`,
`cantidad_st`, `total` en vez de `revenue`), sin type hints en la firma de
`process(file_path, country)`, y sin soporte multi-tenant real (el país se pasa
como parámetro pero está hardcodeado en el `process("data.csv", "GT")` final, no
es parametrizable desde afuera).

**Por qué importa:** dificulta la mantenibilidad y la onboarding de otros
desarrolladores al código; la ausencia de tipado impide detectar errores de tipo
en tiempo de desarrollo (ej. pasar un `int` donde se espera un `str`).

**Cómo se corrige:** convención de naming consistente en inglés para código
(`process_deliveries(file_path: str, tenant: str) -> DataFrame`), y CLI
parametrizable como en `saas_pipeline.cli` de este repo (`--tenant`, en vez de un
valor hardcodeado al final del script).
