# Portal de Revistas de la UNLP — análisis basado en SEDICI

Modelos analíticos desarrollados para el
[Portal de Revistas de la UNLP](https://portalderevistas.unlp.edu.ar/) a partir
de las revistas y publicaciones disponibles en SEDICI.

Esta primera versión describe solamente el universo alojado bajo la comunidad
raíz `Revistas` de SEDICI. No integra automáticamente los contenidos que se
administran exclusivamente desde otros portales internos de la UNLP.

Los modelos se materializan en el esquema `analytics_unlp_portalderevistas`.

## Fuentes

- Publicaciones y jerarquía: `fct_unlp_sedicidb_item_publication` y
  `dim_unlp_sedicidb_community`.
- Autorías: `brg_unlp_sedicidb_item_author` y
  `dim_unlp_sedicidb_author`.
- Para los análisis posteriores de afiliación: `dim_vocsedici_persona`,
  `fct_vocsedici_persona_afiliacion` y `dim_vocsedici_institucion`.

## Modelos

- `unlp_portalderevistas_00_base`: fuente única del dashboard a grano ítem-autor. Permite
  compartir filtros de revista, período y estado de cierre entre publicaciones
  y autores, y conserva los ítems sin autor mediante una fila con autor nulo.
  Expone conteos comparables de publicaciones del autor en todo SEDICI, bajo la
  comunidad `Revistas` y dentro del subconjunto clasificado como artículo.
- `unlp_portalderevistas_00_panorama_revistas`: una fila por revista con el recorrido
  desde ítems publicados hasta artículos con autor identificado.
- `unlp_portalderevistas_00_revistas_por_anio`: evolución anual por revista de ítems,
  artículos y cobertura de autores, sin clasificar afiliaciones.
- `unlp_portalderevistas_00_universo_autores`: una fila por identidad de autor utilizada
  en el análisis. Explicita si la identidad se construyó mediante una authority
  o mediante el nombre normalizado, su correspondencia con VOC y los nombres
  compartidos por más de una identidad.
- `base/dim_unlp_portalderevistas_journal`: una fila por comunidad que representa una
  revista, definida como hija directa de la comunidad raíz `Revistas`.
- `base/fct_unlp_portalderevistas_journal_item`: una fila por ítem publicado y visible de
  una revista. Conserva tipo y subtipo originales y marca como artículo a los
  subtipos `Articulo`, `Comunicacion`, `Contribucion a revista` y `Revision`.
- `base/brg_unlp_portalderevistas_journal_item_author`: una fila por ítem e identidad de
  autor personal, sin clasificar afiliaciones.
- `base/fct_unlp_portalderevistas_journal_article_author`: una fila por artículo y autor, con
  la afiliación vigente a la fecha de publicación y la evidencia utilizada
  para clasificarla.
- `unlp_portalderevistas_01_cobertura_general`: vista de cobertura general
  por estado y evidencia de afiliación.
- `unlp_portalderevistas_02_revistas_ultimos_5_anios`: resumen por revista para los
  últimos cinco años, incluido el corriente; se materializa como vista.
- `unlp_portalderevistas_03_revistas_por_anio`: resumen por revista y
  año; se materializa como vista.

Las facts y dimensiones que sirven como base analítica se ubican en `base/`.
Las vistas de consulta ubicadas en la raíz siguen el patrón
`<organización>_<proyecto>_<secuencia>_<análisis>` y quedan ordenadas según el
recorrido sugerido para explorar los resultados.

## Ejecución

```bash
dbt build \
  --select path:models/05_analytics/unlp/portalderevistas \
  --target dw_sedici
```
