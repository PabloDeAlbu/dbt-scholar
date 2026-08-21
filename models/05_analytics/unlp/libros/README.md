# Afiliación de autores en revistas UNLP

Modelos analíticos para clasificar la afiliación de autores de artículos de
revistas publicadas en SEDICI y resumir los resultados por revista y año.

Los modelos se materializan en el esquema `analytics_unlp_libros`.

## Fuentes

- Publicaciones y jerarquía: `fct_unlp_sedicidb_item_publication` y
  `dim_unlp_sedicidb_community`.
- Autorías: `brg_unlp_sedicidb_item_author` y
  `dim_unlp_sedicidb_author`.
- Personas y afiliaciones: `dim_vocsedici_persona`,
  `fct_vocsedici_persona_afiliacion` y `dim_vocsedici_institucion`.
- Instituciones de origen: `int_unlp_sedicidb_item_mods_origin_info_place`.

## Modelos

- `unlp_libros_00_items_dashboard`: fuente principal del dashboard, a grano
  ítem, con las dimensiones de revista y los indicadores de cobertura de
  autores necesarios para compartir filtros. Incluye la unidad principal UNLP
  atribuida a cada revista y el porcentaje que respalda esa atribución.
- `unlp_libros_00_revistas_unidad`: una fila por revista con la unidad principal
  UNLP más frecuente entre sus ítems. Conserva la cobertura y marca los casos
  ambiguos o sin unidad identificada.
- `unlp_libros_00_panorama_revistas`: una fila por revista con el recorrido
  desde ítems publicados hasta artículos con autor identificado.
- `unlp_libros_00_revistas_por_anio`: evolución anual por revista de ítems,
  artículos y cobertura de autores, sin clasificar afiliaciones.
- `unlp_libros_00_universo_autores`: una fila por identidad de autor utilizada
  en el análisis. Explicita si la identidad se construyó mediante una authority
  o mediante el nombre normalizado, su correspondencia con VOC y los nombres
  compartidos por más de una identidad.
- `base/dim_libros_unlp_journal`: una fila por comunidad que representa una
  revista, definida como hija directa de la comunidad raíz `Revistas`.
- `base/fct_libros_unlp_journal_item`: una fila por ítem publicado y visible de
  una revista. Conserva tipo y subtipo originales y marca como artículo a los
  subtipos `Articulo`, `Comunicacion`, `Contribucion a revista` y `Revision`.
- `base/brg_libros_unlp_journal_item_author`: una fila por ítem e identidad de
  autor personal, sin clasificar afiliaciones.
- `base/fct_libros_unlp_journal_article_author`: una fila por artículo y autor, con
  la afiliación vigente a la fecha de publicación y la evidencia utilizada
  para clasificarla.
- `unlp_libros_01_cobertura_general`: vista de cobertura general
  por estado y evidencia de afiliación.
- `unlp_libros_02_revistas_ultimos_5_anios`: resumen por revista para los
  últimos cinco años, incluido el corriente; se materializa como vista.
- `unlp_libros_03_revistas_por_anio`: resumen por revista y
  año; se materializa como vista.

Las facts y dimensiones que sirven como base analítica se ubican en `base/`.
Las vistas de consulta ubicadas en la raíz siguen el patrón
`<organización>_<proyecto>_<secuencia>_<análisis>` y quedan ordenadas según el
recorrido sugerido para explorar los resultados.

## Ejecución

```bash
dbt build \
  --select path:models/05_analytics/unlp/libros \
  --target dw_sedici
```
