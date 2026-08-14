# Afiliación de autores en revistas UNLP

Modelos analíticos para clasificar la afiliación de autores de artículos de
revistas publicadas en SEDICI y resumir los resultados por revista y año.

Los modelos se materializan en el esquema `analytics_unlp_libros`.

## Fuentes

- Publicaciones: `fct_unlp_sedicidb_item_publication` y
  `fct_unlp_sedicidb_dedup_publication`.
- Autorías: `brg_unlp_sedicidb_item_author` y
  `dim_unlp_sedicidb_author`.
- Personas y afiliaciones: `dim_vocsedici_persona`,
  `fct_vocsedici_persona_afiliacion` y `dim_vocsedici_institucion`.

## Modelos

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
