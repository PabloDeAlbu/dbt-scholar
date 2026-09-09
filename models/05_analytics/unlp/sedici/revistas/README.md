# Revistas en SEDICI — análisis para la UNLP

Modelos analíticos sobre las revistas y publicaciones organizadas bajo la
comunidad raíz `Revistas` de SEDICI.

Esta versión describe las publicaciones bajo la comunidad raíz `Revistas` de SEDICI. No integra automáticamente los contenidos que se
administran exclusivamente desde otros portales internos de la UNLP.

`Revistas` se interpreta como un agrupamiento institucional materializado en
la jerarquía del repositorio, no como una tipología documental. Esta decisión
permite aplicar posteriormente el mismo enfoque a otras ramas organizativas,
como la comunidad raíz `Eventos`.

Los modelos se materializan en el esquema `analytics_unlp_sedici_revistas`.

## Fuentes

- Publicaciones y jerarquía: `fct_unlp_sedicidb_item_publication` y
  `dim_unlp_sedicidb_community`.
- Autorías e identidades: `brg_unlp_sedicidb_item_author` y
  `dim_unlp_sedicidb_author`.
- El enriquecimiento con VOC SEDICI se mantiene en una capa separada y utiliza
  `dim_unlp_author` y `fct_unlp_author_affiliation`.

## Modelos

### Fuente principal

| Modelo | Grano | Uso |
| :--- | :--- | :--- |
| `02_dashboard/unlp_sedici_revistas_dashboard` | Publicación y autor | Fuente principal de visualización. Permite que los filtros de revista, período y estado alcancen publicaciones y autores desde un único conjunto de datos. Conserva una fila con autor nulo cuando la publicación no tiene una identidad reconocida. |

### Base SEDICIDB

| Modelo | Grano | Contenido |
| :--- | :--- | :--- |
| `00_base/dim_unlp_sedici_revista` | Revista | Comunidades hijas directas de la comunidad raíz `Revistas`. |
| `00_base/fct_unlp_sedici_revista_publicacion` | Publicación | Ítems publicados y visibles bajo una revista. Conserva el subtipo informado sin reclasificarlo. |
| `00_base/brg_unlp_sedici_revista_publicacion_autor` | Publicación y autor | Relaciones con autores personales registradas en los metadatos de SEDICI. |

### Enriquecimiento

| Modelo | Fuente adicional | Grano | Contenido |
| :--- | :--- | :--- | :--- |
| `01_enrichment/vocsedici/fct_unlp_sedici_revista_autoria_afiliacion_vocsedici` | VOC SEDICI | Publicación y autor | Correspondencia con autoridades y afiliaciones aplicables a la fecha de publicación. |

### Métricas y salidas complementarias

Las definiciones, fórmulas y estado de los modelos agregados se documentan en
[`03_metrics/README.md`](03_metrics/README.md).

El reporte fechado es el entregable principal del caso analítico. Los modelos
de `03_metrics/` producen las tablas que lo acompañan; el dashboard queda como
herramienta complementaria de exploración.

Las facts y dimensiones construidas solo con SEDICIDB se ubican en `00_base/`.
Los aportes de otras fuentes se ubican en `01_enrichment/<fuente>/`. La fuente
de visualización está en `02_dashboard/`, los agregados en `03_metrics/` y los
entregables fechados en `04_report/`.

## Granos y conteos

| Concepto | Identificador | Forma de conteo |
| :--- | :--- | :--- |
| Revista | `handle_revista` | `COUNT(DISTINCT handle_revista)` |
| Publicación | `handle_publicacion` | `COUNT(DISTINCT handle_publicacion)` |
| Autoría | `id_autoria` | `COUNT(DISTINCT id_autoria)` |
| Autor identificado | `id_autor` | `COUNT(DISTINCT id_autor)` |

Una publicación puede tener varios autores. Por este motivo, la cantidad de
autores puede superar la cantidad de publicaciones. `Record Count` no debe
utilizarse para medir publicaciones ni autores en la fuente principal.

## Ejecución

```bash
dbt build \
  --select path:models/05_analytics/unlp/sedici/revistas \
  --target dw_sedici
```
