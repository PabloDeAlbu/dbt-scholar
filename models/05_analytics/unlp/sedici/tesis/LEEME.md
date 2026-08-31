# Tesis UNLP en SEDICI

Este directorio presenta las tesis publicadas y visibles en SEDICI para su
consulta, exportación y análisis descriptivo.

## Universo

El universo se obtiene de `fct_unlp_sedicidb_item_publication` y requiere:

- `dc_type = 'Tesis'`;
- ítem incorporado al repositorio;
- ítem no retirado y visible mediante búsqueda;
- handle público disponible.
- ubicación bajo la comunidad base `Unidades académicas`.

`sedici.subtype` conserva la clasificación específica: tesis de grado,
maestría, doctorado, trabajo de especialización o trabajo final de grado.
`unidad_academica` se obtiene de la comunidad ubicada inmediatamente debajo de
`Unidades académicas` en la ruta de cada ítem.

## Alcance

`unlp_sedici_tesis_reporte` tiene una fila por tesis y utiliza nombres de
columnas orientados a las personas que reciben el reporte. Los metadatos que
pueden repetirse se presentan separados por punto y coma.

Las tesis permanecen en el universo aunque algún metadato descriptivo no esté
informado. En particular, existe al menos un registro visible sin una autoría
utilizable; el modelo lo conserva con ese campo vacío para hacer explícita la
ausencia en la fuente.

El modelo describe lo registrado en el dump vigente de SEDICI. No constituye
por sí solo el registro administrativo completo de las tesis producidas por la
UNLP ni permite inferir la unidad académica cuando esa relación no está
declarada de forma explícita.
