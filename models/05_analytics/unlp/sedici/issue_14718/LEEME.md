# Issue 14718: deduplicación CIC LIFIA → SEDICI

El objetivo es identificar cuáles publicaciones de dos colecciones LIFIA de
CIC Digital ya se encuentran en SEDICI antes de preparar una incorporación.

## Escenario directo

El primer escenario compara snapshots directos de ambos repositorios:

```text
ldg_cicdigital
    -> fct_cic_cicdigital_item_publication
    -> fct_cic_cicdigital_dedup_publication
    -> input 1

ldg_sedicidb
    -> fct_unlp_sedicidb_item_publication
    -> fct_unlp_sedicidb_dedup_publication
    -> input 2
```

El input 1 se limita a estas colecciones:

- `3da12564-7da1-4d08-9037-f706cc294a09`
- `ff4e8e87-449a-4df0-b424-07ef1b533e1e`

El input 2 utiliza el universo general elegible de SEDICI. No se restringe por
colección, entidad de origen ni pertenencia institucional de los autores porque
esas señales no cubren de manera completa las publicaciones de LIFIA.

En el dump de CIC Digital del 10 de agosto de 2026 se observaron 382 ítems en
las dos colecciones. Dos están retirados y fuera del archivo; los 380 restantes
cumplen el contrato mínimo del deduplicador.

## Escenarios posteriores

El mismo issue puede incorporar escenarios construidos desde `dspacedb` u
otras fuentes. Cada escenario debe usar nombres de modelo distintos y declarar
en su `schema.yml` las fuentes de ambos inputs. Los resultados no deben cambiar
de procedencia implícitamente según el target.

## Validaciones realizadas

Se verificaron los metadatafields utilizados para título, autor, fecha, tipo e
identificadores; la cobertura dentro de las dos colecciones; el mapeo de tipos;
y los motivos de exclusión del input 1.
