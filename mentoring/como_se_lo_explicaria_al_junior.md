# Cómo se lo explicaría al junior

No empezaría por la lista de errores. Empezaría preguntándole qué pasaría si el
archivo tuviera 50 millones de filas en vez de 3,000 — casi siempre la propia
persona llega sola a "ah, no debería usar `iterrows()`" cuando lo piensa en esos
términos, y eso vale más que si se lo digo yo directamente. El feedback funciona
mejor cuando la persona descubre el problema razonando sobre un caso concreto, no
cuando recibe una corrección abstracta.

A partir de ahí, iría de mayor a menor impacto, no en el orden en que aparecen en
el código: primero pandas-vs-Spark y la escritura no idempotente (los dos que
realmente rompen en producción), después las validaciones ausentes (el que más le
cuesta a la empresa si nadie lo nota), y al final naming/tipado, que son mejoras
reales pero no bloqueantes. Separar "esto no puede ir a producción así" de "esto
está bien pero se puede pulir" evita que la persona sienta que todo el código está
mal cuando en realidad la estructura general (leer, filtrar, calcular, escribir) es
correcta.

Cada observación la acompañaría con el "por qué" antes que el "qué", porque un
junior que entiende por qué `iterrows()` no escala va a evitarlo en el próximo
proyecto sin que nadie se lo tenga que repetir; uno que solo memoriza "usar
`iterrows()` está mal" lo va a volver a hacer en un contexto ligeramente distinto.

**Temas que le pediría investigar por su cuenta antes de la próxima revisión:**
- Diferencia entre transformaciones *lazy* y *eager* en Spark, y por qué eso hace
  que mezclar pandas y Spark en el mismo script sea un antipatrón (no solo un
  problema de estilo).
- Qué significa que una escritura sea "idempotente" y por qué en un pipeline de
  datos eso no es opcional — le pediría que investigue `MERGE INTO` en Delta Lake
  y que me traiga un ejemplo propio, no que se lo explique yo primero.
- Cómo diseñar una función para que sea testeable sin necesitar levantar Spark
  completo (separar lógica pura de I/O), que es exactamente el patrón que usé en
  `silver.py` de este mismo repo.
