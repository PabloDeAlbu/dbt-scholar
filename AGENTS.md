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
- `models/05_analytics/**` responde casos analiticos concretos y organiza sus bases, dashboards, metricas y reportes sin reimplementar la ingesta ni la historizacion.

## Pendientes y cambios locales

- No agregar comentarios `TODO` o `FIXME` al codigo versionado para recordar tareas operativas personales.
- Registrar esos pendientes en `TODO.local.md`, excluido localmente mediante `.git/info/exclude`.
- Los commits `dev(...)` pueden utilizarse como checkpoints locales, pero deben reescribirse con un tipo semantico antes de publicar la rama.
