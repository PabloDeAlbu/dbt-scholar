# Revistas en SEDICI — análisis para la UNLP

Modelos analíticos sobre las revistas y publicaciones organizadas bajo la
comunidad raíz `Revistas` de SEDICI. El
[Portal de Revistas de la UNLP](https://portalderevistas.unlp.edu.ar/) es un
destinatario posible del análisis, pero no define el universo de datos.

Esta primera versión describe solamente el universo alojado bajo la comunidad
raíz `Revistas` de SEDICI. No integra automáticamente los contenidos que se
administran exclusivamente desde otros portales internos de la UNLP.

`Revistas` se interpreta como un agrupamiento institucional materializado en
la jerarquía del repositorio, no como una tipología documental. Esta decisión
permite aplicar posteriormente el mismo enfoque a otras ramas organizativas,
como la comunidad raíz `Eventos`.

Los modelos se materializan en el esquema `analytics_unlp_sedici_revistas`.

## Fuentes

- Publicaciones y jerarquía: `fct_unlp_sedicidb_item_publication` y
  `dim_unlp_sedicidb_community`.
- Autorías: `brg_unlp_sedicidb_item_author` y
  `dim_unlp_sedicidb_author`.
- Para los análisis posteriores de afiliación: `dim_vocsedici_persona`,
  `fct_vocsedici_persona_afiliacion` y `dim_vocsedici_institucion`.

## Modelos

- `unlp_sedici_revistas_dashboard`: fuente principal del dashboard a grano ítem-autor. Permite
  compartir filtros de revista, período y estado de cierre entre publicaciones
  y autores, y conserva los ítems sin autor mediante una fila con autor nulo.
  Expone conteos comparables de publicaciones del autor en todo SEDICI, bajo la
  comunidad `Revistas` y dentro del subconjunto clasificado como artículo.
- `base/dim_unlp_sedici_revista`: una fila por comunidad que representa una
  revista, definida como hija directa de la comunidad raíz `Revistas`.
- `base/fct_unlp_sedici_revista_publicacion`: una fila por ítem publicado y visible de
  una revista. Conserva tipo y subtipo originales y marca como artículo a los
  subtipos `Articulo`, `Comunicacion`, `Contribucion a revista` y `Revision`.
- `base/brg_unlp_sedici_revista_publicacion_autor`: una fila por ítem e identidad de
  autor personal, sin clasificar afiliaciones.
- `base/fct_unlp_sedici_revista_articulo_autor`: una fila por artículo y autor, con
  la afiliación vigente a la fecha de publicación y la evidencia utilizada
  para clasificarla.
- `metrics/unlp_sedici_revistas_panorama`: una fila por revista con el
  recorrido desde ítems publicados hasta artículos con autor identificado.
- `metrics/unlp_sedici_revistas_por_anio`: evolución anual por revista
  de ítems, artículos y cobertura de autores, sin clasificar afiliaciones.
- `metrics/unlp_sedici_revistas_universo_autores`: una fila por identidad de
  autor utilizada en el análisis. Explicita si la identidad se construyó
  mediante una authority o mediante el nombre normalizado, su correspondencia
  con VOC y los nombres compartidos por más de una identidad.
- `metrics/unlp_sedici_revistas_cobertura_general`: vista de cobertura general
  por estado y evidencia de afiliación.
- `metrics/unlp_sedici_revistas_ultimos_5_anios`: resumen de autorías
  y afiliaciones por revista para los últimos cinco años, incluido el corriente.
- `metrics/unlp_sedici_revistas_afiliacion_autores_por_anio`: resumen anual por
  revista de autorías, autores y su clasificación de afiliación.

Las facts y dimensiones que sirven como base analítica se ubican en `base/`.
La única vista SQL ubicada en la raíz es la fuente principal del dashboard.
Los perfiles y agregados analíticos complementarios se ubican en `metrics/`.

## Ejecución

```bash
dbt build \
  --select path:models/05_analytics/unlp/sedici/revistas \
  --target dw_sedici
```
