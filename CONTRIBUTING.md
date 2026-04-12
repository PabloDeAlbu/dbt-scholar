# Contribuir

Este repositorio organiza el modelado en capas de `dbt` sobre varias fuentes (`dspacedb`, `dspacedb5`, `oai`, `openaire`, `openalex`, `coar`) y además tiene modelos específicos por institución o caso de uso.

La idea de esta guía es responder tres preguntas:

1. dónde va cada cambio
2. cómo probarlo localmente
3. cómo nombrar commits y modelos de forma consistente

## Flujo de trabajo recomendado

Para desarrollo diario:

1. correr y probar por defecto contra la base local configurada en `profiles.yml`
2. trabajar con selectores acotados (`-s`) y, cuando aplique, con tags
3. validar al final contra el target remoto si el cambio lo requiere

Ejemplos:

```bash
dbt debug
dbt run -s dim_dspacedb5_community
dbt test -s dim_dspacedb5_community

dbt debug --target dw
dbt run --target dw -s dim_dspacedb5_community
```

Si necesitás refrescar datos locales de DSpace 5 desde Docker:

```bash
make dump-dspace-public
make restore-dspace-public-to-dw
```

Los dumps locales se guardan en `var/dumps/` y están ignorados por git.

## Estructura del proyecto

### `models/01_ldg`

Capa landing. Acá viven:

- `sources`
- normalización inicial
- inyección de contexto técnico de extracción

Si el cambio depende directamente de tablas crudas o de cómo entra una fuente al proyecto, normalmente empieza acá.

### `models/02_dv`

Data Vault. La estructura se organiza por dominio y por tipo de modelo:

- `01_stg`
- `02_hub`
- `03_lnk` o `03_link`
- `04_sat`

Si el cambio afecta claves de negocio, relaciones o satélites, va en esta capa.

### `models/03_dm/core`

Modelo dimensional reutilizable. Acá viven dimensiones, hechos y bridges compartidos por dominio, pensados como capa semántica base para consumo posterior.

Regla práctica:

- si el modelo expresa una entidad de análisis, probablemente sea una `dim_`
- si modela eventos, relaciones de actividad o métricas a un grano explícito, probablemente sea una `fct_`
- si resuelve relaciones muchos-a-muchos o asociaciones navegables entre entidades, probablemente sea una `brg_`
- si puede reutilizarse fuera de un caso institucional puntual, va en `core`

### `models/03_dm/custom`

Modelos específicos por institución o implementación:

- `cic`
- `conicet`
- `smn`
- `unlp`

Si un modelo depende de reglas, filtros o semántica particular de una institución, no debería ir en `core`.

#### Patrón sugerido para `custom/<org>/<fuente>`

En general, los modelos institucionales de `custom` deberían seguir una estructura parecida:

- partir de un hecho `core` de la fuente
- definir un subconjunto institucional explícito
- agregar sólo el contexto o enriquecimiento necesario para consumo institucional
- exponer una interfaz estable para capas posteriores sin reabrir lógica cruda en `viz`

Regla práctica por fuente:

- `custom/<org>/openalex`: partir de `fct_openalex_work_publication` y definir el subconjunto institucional con `fct_openalex_work_extraction` cuando exista extracción institucional explícita por `institutions.ror`
- `custom/<org>/dspacedb` o `custom/<org>/dspacedb5`: partir de `fct_dspacedb_item_publication` o `fct_dspacedb5_item_publication`, filtrar por `institution_ror` y agregar metadatos institucionales desde los facts core de metadata
- `custom/<org>/oai`: partir de `fct_oai_record_publication`, filtrar por `repository_identifier` e `institution_ror`, y agregar bridges institucionales sólo cuando aporten semántica local
- `custom/<org>/openaire`: partir de `fct_openaire_researchproduct_publication` o de la extracción core correspondiente, filtrando por el criterio institucional efectivamente usado en la carga

Conviene que estos modelos tengan CTEs reconocibles:

- `base`: subconjunto institucional derivado de `core`
- `context` o `extract`: primera y última observación temporal del subconjunto
- `enrichment`: metadatos o bridges necesarios para el caso institucional
- `final`: interfaz estable para consumo aguas abajo

#### Convención operativa de ejecución

Además del patrón de modelado, conviene documentar un selector operativo estable por fuente institucional. La idea es que, ante una nueva carga upstream, el equipo pueda refrescar la cadena relevante sin tener que reconstruir mentalmente todo el DAG.

Regla práctica:

- si cambió sólo la lógica de un modelo institucional, correr desde el subárbol de `custom/<org>/<fuente>`
- si entró nueva data de una fuente y ese selector ya recompone la cadena `core` necesaria, usar igualmente el selector de `custom/<org>/<fuente>` como comando operativo recomendado
- si una carga nueva no se propaga correctamente desde ese selector, subir el punto de entrada al primer modelo `core` que consolida esa fuente

Ejemplo operativo vigente:

- para refrescar OpenAlex de UNLP, usar `dbt run --target dw_sedici -s +models/03_dm/custom/unlp/openalex/`

La convención buscada es distinguir dos planos:

- el patrón semántico del modelo, que define de qué hechos y dimensiones parte
- el selector operativo recomendado, que define desde dónde conviene ejecutar una recarga real

No conviene cambiar un stream de ejecución que ya funciona bien sólo para hacerlo más "puro" desde el punto de vista conceptual.

Qué debería quedar en `core`:

- hechos base por fuente
- dimensiones y bridges reutilizables
- parsing o normalización genérica de fechas, PIDs y tipos
- contexto técnico de extracción reutilizable entre instituciones

Qué debería quedar en `custom/<org>`:

- filtros por `institution_ror`, `repository_identifier` o `_filter_value`
- naming institucional
- selección de atributos relevantes para una institución
- bridges o aplanados que sólo tienen sentido en ese subconjunto institucional

Qué evitar en `viz`:

- redefinir el subconjunto institucional
- volver a cruzar contra metadata cruda si eso ya fue resuelto en `dm`
- duplicar lógica de parseo o reconciliación que debería vivir en `core` o `custom`

### `models/04_viz`

Salidas orientadas a consumo analítico o visualización. En general esta capa debería depender de `dm` y evitar reimplementar lógica de negocio pesada.

## Convenciones de nombres

### Prefijos

Usar prefijos consistentes según el tipo de modelo:

- `ldg_` para landing
- `stg_` para staging de vault
- `hub_` para hubs
- `link_` o `lnk_` según la convención ya usada en esa carpeta
- `sat_` para satellites
- `latest_sat_` para vistas o tablas del último estado
- `brg_` para bridges
- `dim_` para dimensiones
- `fct_` para hechos

### Dominio y fuente

El nombre debería dejar claro el dominio o fuente principal. Ejemplos:

- `ldg_dspacedb5_item`
- `hub_dspacedb5_item`
- `dim_dspacedb5_community`
- `fct_unlp_dspacedb5_item_publication`

### Archivos `schema.yml`

Cada conjunto coherente de modelos debería tener su documentación y tests cerca. No hace falta un `schema.yml` por archivo, pero sí por unidad lógica mantenible.

## Notas de modelado

- Mantener las hash keys nativas (`*_hk`) en su tipo técnico original, salvo que exista una razón clara para exponer otra representación.
- En hechos (`fct_`), dejar explícito el grano del modelo y evitar mezclar en una misma tabla más de un evento o nivel de detalle.
- En dimensiones (`dim_`), priorizar atributos descriptivos estables y evitar incorporar métricas derivadas que cambien el grano semántico.
- En bridges (`brg_`), modelar asociaciones entre entidades sin duplicar lógica de agregación propia de hechos.
- En modelos transversales de `dm`, distinguir relación bibliográfica de contexto de extracción.
- En OpenAlex y OpenAIRE, `institution_ror` puede representar el ROR usado para recuperar la entidad y no una afiliación intrínseca de la publicación.
- En `ldg`, conservar explícitamente el contexto técnico cuando aplique, por ejemplo `_source_label`, `_institution_ror`, `_extract_datetime`, `_load_datetime`.
- Si un subconjunto institucional depende de cómo fue hecha la extracción upstream, esa definición debe respetarse en `custom` y no inferirse de otra tabla sólo porque parece equivalente.

## Convención de commits

El historial del repositorio usa mensajes cortos y formato:

`tipo(scope) mensaje`

Ejemplos reales del repo:

- `feat(dm dspacedb5) agrega modelado de comunidades y colecciones`
- `refactor(ldg dspacedb5) valida contexto y parametriza fechas de carga`
- `feat(dm smn) agrega fact item publication`

Tipos recomendados:

- `feat`: nueva funcionalidad o nuevo modelo
- `fix`: corrección de bug o ajuste semántico
- `refactor`: cambio estructural sin cambio funcional intencional
- `docs`: documentación
- `test`: tests o validaciones
- `chore`: mantenimiento

Scopes sugeridos:

- capa y dominio juntos: `ldg dspacedb5`, `dv dspacedb`, `dm openalex`
- variante más específica cuando aporta claridad: `dm custom unlp`
- `dev` para tooling local o helpers de desarrollo

Ejemplos:

- `feat(dm dspacedb5) agrega fact de comunidades por item`
- `fix(ldg dspacedb5) corrige tipado de metadatavalue`
- `refactor(dm custom unlp) reutiliza logica de item publication`
- `docs(repo) aclara flujo de desarrollo local`
- `feat(dev) agrega helpers para dump y restore local de dspace`

## Qué revisar antes de abrir un cambio

- que el modelo esté en la capa correcta
- que el nombre siga la convención existente
- que el `schema.yml` cercano refleje la intención del cambio
- que el selector mínimo relevante compile o ejecute
- que el mensaje de commit describa el cambio con el scope correcto
