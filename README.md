# saas-data-platform

Pipeline Medallion (Bronze/Silver/Gold) multi-tenant sobre PySpark + Delta Lake,
implementado para la Prueba Técnica de Senior Data Engineer (proyecto SAAS, Apex Digital / M5).

## 1. Estructura del repositorio

```
saas-data-platform/
├── README.md
├── Makefile
├── pyproject.toml
├── .github/workflows/ci.yml
├── docs/
│   ├── infra.md
│   ├── observations.md
│   └── onboarding-tenant.md
├── config/
│   ├── base.yaml
│   ├── env/{dev,qa,main}.yaml
│   └── tenants/{sv,hn,ec,jm,pe,gt}.yaml
├── data/raw/            # CSVs de entrada (provistos)
├── src/saas_pipeline/
│   ├── __init__.py      # SparkSession + Delta
│   ├── config.py        # carga jerarquica OmegaConf
│   ├── bronze.py        # ingesta cruda
│   ├── silver.py        # transformaciones (puras) + orquestacion MERGE INTO
│   ├── gold.py          # agregaciones de negocio
│   ├── quality.py       # validaciones + quality_logs
│   └── cli.py           # orquestador end-to-end
├── tests/
│   ├── conftest.py
│   ├── test_silver_transforms.py
│   └── test_quality.py
└── mentoring/
    ├── bad_code.py
    ├── good_code.py
    └── code_review.md
```

## 2. Versiones utilizadas

- Python 3.11
- PySpark 3.5.1
- delta-spark 3.1.0 (Delta Lake 3.x)
- OmegaConf 2.3.0

Compatibles con Databricks Runtime 15.x LTS.

## 3. Cómo levantar el entorno

```bash
make install     # crea venv, instala el paquete en modo editable + deps de dev
```

### Notas para Windows (probado en un entorno real durante el desarrollo)

En Windows no hay `make` por defecto, así que los comandos del Makefile se corren
directos. Además, PySpark + Delta Lake tienen 3 fricciones específicas de Windows
que vale la pena resolver antes de instalar:

1. **Java bloqueado por Smart App Control.** El `openjdk` que instala conda-forge
   no viene firmado digitalmente y Windows lo bloquea ("una directiva de Control de
   aplicaciones bloqueó este archivo"). Solución: instalar un JDK firmado en su lugar:
```powershell
   winget install EclipseAdoptium.Temurin.17.JDK
   conda remove openjdk -y                          # si ya lo habías instalado por conda
   conda env config vars set JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot" -n <tu_env>
```

2. **`Python was not found` al correr Spark (alias de Microsoft Store).** Windows
   intercepta el comando `python` con un alias que redirige a la Store. Hay que
   decirle a Spark explícitamente qué `python.exe` usar:
```powershell
   conda env config vars set PYSPARK_PYTHON="<ruta a tu python.exe del env>" -n <tu_env>
   conda env config vars set PYSPARK_DRIVER_PYTHON="<ruta a tu python.exe del env>" -n <tu_env>
```

3. **`HADOOP_HOME and hadoop.home.dir are unset` (falta winutils.exe).** Hadoop
   (dependencia de Spark) necesita `winutils.exe` en Windows aunque no se use HDFS:
```powershell
   mkdir C:\hadoop\bin
   Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
   Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"
   # copiar hadoop.dll a C:\Windows\System32 (requiere terminal de administrador)
   conda env config vars set HADOOP_HOME="C:\hadoop" -n <tu_env>
```

Después de fijar estas 3 variables (`JAVA_HOME`, `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`,
`HADOOP_HOME`) con `conda env config vars set`, hay que reactivar el entorno
(`conda deactivate` + `conda activate <tu_env>`) para que tomen efecto.

## 4. Cómo correr el pipeline

```bash
# Todos los tenants, todo el rango de fechas del dataset
make run-all

# Un tenant especifico
make run-tenant TENANT=sv START=2025-03-01 END=2025-03-31

# Equivalente directo:
python -m saas_pipeline.cli --env dev --tenant sv --start-date 2025-03-01 --end-date 2025-03-31
```

El CLI ejecuta, por tenant: **Bronze → Silver (fact + dim) → Quality checks → Gold**,
y respeta `execution.fail_fast` y `quality.fail_on_critical` (ver `config/base.yaml`).

## 5. Cómo correr tests y linter localmente

```bash
make test    # pytest tests/ -v  (17 pruebas, no requieren el jar de Delta)
make lint    # ruff check src/ tests/
```

Nota de diseño: las funciones de transformación en `silver.py` (`transform_deliveries`,
`transform_dim_materials_scd2`, etc.) son **puras** — reciben y devuelven DataFrames sin
tocar disco ni depender de Delta. Esto permite testear toda la lógica de negocio
(conversión de unidades, filtrado, cuarentena, SCD2) con PySpark local puro, sin
necesitar el jar de `delta-spark` disponible en el classpath. Las funciones `build_*`
envuelven esa lógica con I/O real (lectura Bronze, `MERGE INTO` a Silver) y sí requieren
Delta — se ejercitan al correr `make run-all` o `make run-tenant`.

## 6. Cómo se onboarding-aría un tenant nuevo

1. Crear `config/tenants/<nuevo_tenant>.yaml` (ver `docs/onboarding-tenant.md` para el detalle).
2. No se requiere tocar código: `bronze.py`, `silver.py` y `gold.py` son genéricos por tenant.
3. Correr `python -m saas_pipeline.cli --tenant <nuevo_tenant> --start-date ... --end-date ...`.
4. En Databricks productivo: aprovisionar el schema `saas_<env>.bronze_<tenant>` /
   `silver_<tenant>` / `gold_<tenant>` en Unity Catalog vía Terraform (ver `docs/infra.md`).

## 7. Qué dejé fuera y por qué

Dado el marco de 3 días / 12-15 horas, prioricé el MVP funcional sobre alcance amplio:

- **No implementé Auto Loader / streaming** (sección 10, bonus): el dataset es un batch
  estático de 3,116 filas; Auto Loader no aporta valor demostrable aquí y hubiera
  consumido tiempo que preferí invertir en que el `MERGE INTO` con SCD2 y el manejo de
  anomalías estuvieran genuinamente bien probados.
- **No implementé una segunda tabla Gold** (bonus): con una tabla Gold bien modelada y
  probada alcanza para demostrar el patrón; agregar una segunda sin necesidad real
  hubiera sido alcance por alcance, no valor.
- **No configuré pre-commit hooks** (bonus): el mismo lint corre en CI; localmente no es
  bloqueante para la evaluación.
- **Terraform es un snippet ilustrativo**, no un módulo funcional contra una cuenta real
  (explícitamente no requerido, sección 7.2).
- **No traté el "20% de cardinalidad baja" de Gold particionado por fecha**: con 6
  tenants y ~180 días la tabla Gold es pequeña; particionar por fecha aquí añadiría
  archivos pequeños sin beneficio real (lo documento como observación, no como bug).
- **La tabla `quality_logs` se escribe en modo append sin deduplicación por `_run_id`**:
  aceptable para el alcance de la prueba; en productivo agregaría una política de
  retención/compactación (ver `docs/observations.md`, mejora #3).

## 8. Sustentación

Ver `docs/observations.md` para las 3+ observaciones sustantivas a la arquitectura
provista, y `mentoring/` para el ejercicio de code review.
