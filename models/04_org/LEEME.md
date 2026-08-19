# Capa organizacional

Esta capa representa entidades y procesos desde la perspectiva de una
institución. Una organización puede tener varias representaciones simultáneas,
una por cada fuente disponible.

## Estructura

```text
models/04_org/<organización>/<fuente>/
```

La organización delimita el universo institucional. La fuente indica desde qué
sistema y recorrido de datos se construye ese universo.

Por ejemplo, CIC puede representar publicaciones desde un dump directo de CIC
Digital o desde los modelos compartidos de `dspacedb`. Ninguna representación
reemplaza silenciosamente a la otra.

## Responsabilidades

- `03_dm` representa sistemas fuente de manera reutilizable entre instituciones.
- `04_org` aplica el alcance y las reglas de una institución sobre una fuente explícita.
- `05_analytics` elige qué representaciones utilizar para una pregunta o entrega concreta.

Un modelo de `04_org` puede depender de un modelo reutilizable de `03_dm` o,
cuando se trabaja con una fuente institucional directa, de su esquema `ldg`.
Esa decisión debe quedar visible en la carpeta, el nombre del modelo y su
`schema.yml`.

## Contratos y procedencia

Los modelos equivalentes construidos desde fuentes distintas deberían ofrecer
un contrato comparable sin borrar su procedencia. Cuando corresponda, deben
conservar:

- organización e identificador institucional;
- sistema fuente y repositorio;
- identificador del registro en la fuente;
- fecha de extracción o fecha del snapshot;
- identificadores públicos del recurso.

Los modelos analytics deben referenciar explícitamente la variante que usan.
No se debe cambiar de fuente mediante una selección implícita dependiente del
target.

## Niveles de documentación

El `schema.yml` funciona como contrato descriptivo y verificable: declara el
propósito, el grano, la procedencia, las columnas y las pruebas del modelo. Sus
descripciones deben ser breves para que también puedan ser utilizadas por dbt,
catálogos y herramientas de asistencia.

Las decisiones de arquitectura, los alcances y los ejemplos que requieren más
contexto pertenecen a un `LEEME.md` cercano a los modelos. Un `README.md` puede
ofrecer posteriormente una entrada equivalente en inglés, sin duplicar las
definiciones estructurales del `schema.yml`.

Los campos `meta` utilizados como vocabulario común son:

- `organization`: institución cuyo universo representa el modelo;
- `source_system`: sistema del que provienen los datos;
- `input_layer`: capa desde la que el modelo recibe su entrada principal;
- `model_role`: responsabilidad del modelo;
- `business_process`: proceso institucional representado, cuando corresponde;
- `grain`: clave o unidad que representa cada fila;
- `temporal_scope`: alcance temporal de la representación.

Por ahora este contrato no habilita `contract.enforced`. La correspondencia
entre columnas documentadas y producidas se controla mediante pruebas y
revisión. Declarar tipos físicos obligatorios se evaluará sólo si aporta más
seguridad que costo de mantenimiento.

## Criterio de simplicidad

Cada modelo debe declarar un grano y cumplir una responsabilidad principal.
Las normalizaciones de un metadatafield pertenecen a modelos `int_`; las
relaciones multivaluadas pertenecen a `brg_`; las entidades descriptivas a
`dim_`; y los procesos relevantes para la institución a `fct_`.

Se prefieren transformaciones cortas y trazables. Las agregaciones, rankings y
expresiones regulares se incorporan sólo cuando resuelven una regla explícita y
quedan documentadas y probadas en el modelo que las necesita.
