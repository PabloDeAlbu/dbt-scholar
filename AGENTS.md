# AGENTS.md

## dbt / Data Vault guardrails

- Nunca proponer ni ejecutar `dbt run --full-refresh` sobre modelos de `models/02_dv/**`.
- En este proyecto, `ldg` representa solo el ultimo batch disponible por entidad; no es historico completo.
- Asumir que un full refresh en DV puede truncar historia valida y romper consistencia aguas abajo.
- Ante cambios de modelado en DV, preferir migraciones compatibles hacia adelante, ajustes en `dm`, o instrucciones especificas del usuario.
- Si una solucion pareciera requerir full refresh en DV, detenerse y pedir confirmacion explicita con explicacion del impacto.

## Responsabilidades por capa

- `models/01_ldg/**` adapta las relaciones recibidas en landing con normalizacion minima y conserva el contexto tecnico de extraccion y carga.
- Cada modelo de `models/02_dv/**/01_stg` debe preparar un modelo de landing para Data Vault. No crear staging artificiales solo para unir claves de varias entidades.
- Los hubs conservan claves de negocio de una entidad. Una misma clave puede provenir de varios staging; en ese caso, usar la capacidad multifuente de `automate_dv.hub` mediante una lista en `source_model`.
- Los links conservan relaciones entre claves de negocio.
- Los satelites conservan atributos y observaciones historicas con un grano compatible con su clave. Verificar que exista como maximo una observacion por clave y momento de carga antes de usar un satelite estandar.
- En `models/02_dv/**`, preferir las macros de AutomateDV disponibles antes de escribir implementaciones SQL equivalentes.
- `models/03_dm/**` presenta hechos, dimensiones y bridges reutilizables por fuente, sin reglas propias de una organizacion o caso analitico.
- `models/04_org/**` define subconjuntos y semantica propios de una organizacion.
- En `models/04_org/<organizacion>/sources/<fuente>`, conservar representaciones
  institucionales construidas desde una fuente explicita.
- En `models/04_org/<organizacion>/integration`, ubicar correspondencias entre
  fuentes, conservando entidades de origen, regla, evidencia y ambiguedad.
- Reservar la raiz de `models/04_org/<organizacion>` para hechos, dimensiones y
  bridges institucionales reutilizables que articulan una o mas fuentes.
- `models/05_analytics/**` responde casos analiticos concretos y organiza sus bases, dashboards, metricas y reportes sin reimplementar la ingesta ni la historizacion.

## Pendientes y cambios locales

- No agregar comentarios `TODO` o `FIXME` al codigo versionado para recordar tareas operativas personales.
- Registrar esos pendientes en `TODO.local.md`, excluido localmente mediante `.git/info/exclude`.
- Los commits `dev(...)` pueden utilizarse como checkpoints locales, pero deben reescribirse con un tipo semantico antes de publicar la rama.

## Descripciones de modelos

- Escribir descripciones breves, con una oracion por linea.
- Cada oracion debe aportar una sola idea.
- Indicar primero que representa el modelo.
- Mencionar el modelo de origen sin volver a describir su logica.
- Documentar reglas de inclusion o exclusion cuando definan el significado del resultado.
- No repetir transformaciones, pruebas ni detalles que puedan leerse directamente en el SQL.
- Describir las columnas por su significado, no por la expresion SQL que las produce.

## Metadatos de modelos

- Usar `config.meta` para documentar propiedades estructuradas del modelo.
- Incluir solamente propiedades conocidas y utiles; no inferir valores para completar el bloque.
- Usar `organization` para la organizacion responsable del modelo.
- Usar `source_system` para una fuente y `source_systems` para varias fuentes.
- Usar `model_role` para la funcion del modelo dentro del DAG.
- Usar `grain` para expresar que representa cada fila.
- Usar `temporal_scope` para distinguir estado actual, ultima observacion o historia.
- Mantener el grano tambien en la descripcion cuando sea importante para interpretar el modelo.
- No incorporar estos metadatos retrospectivamente de forma masiva sin auditar cada modelo.

## Contratos en schema.yml

- Ordenar las propiedades de cada modelo como `description`, `config`, `columns` y `data_tests`.
- Ubicar las pruebas generales del modelo después de la definición de sus columnas.
- Mantener las pruebas propias de cada columna dentro de su definición.
- Aplicar esta convención al crear o modificar un contrato sin reorganizar otros archivos de forma masiva.

## Estructura de modelos SQL

- Anteponer a cada CTE una pregunta breve que exprese su responsabilidad.
- Evitar calificativos subjetivos cuando puedan reemplazarse por la regla aplicada.
- En modelos organizados mediante CTE, llamar `final` a la CTE que define el contrato de salida.
- Terminar esos modelos con `SELECT * FROM final`.
