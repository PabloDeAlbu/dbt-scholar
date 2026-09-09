# Métricas de revistas en SEDICI

Este directorio reúne definiciones analíticas derivadas de las publicaciones y
autorías registradas bajo la comunidad raíz `Revistas` de SEDICI.

Las métricas se documentan independientemente de la herramienta de
visualización. Las mismas fórmulas pueden utilizarse en Superset, PostgreSQL u
otro software que consuma `unlp_sedici_revistas_dashboard`.

## Métricas del dashboard

| Nombre visible | Fórmula | Qué representa |
| :--- | :--- | :--- |
| Revistas | `COUNT(DISTINCT handle_revista)` | Revistas con registros alcanzados por los filtros activos. |
| Publicaciones | `COUNT(DISTINCT handle_publicacion)` | Publicaciones distintas alcanzadas por los filtros activos. |
| Autorías identificadas | `COUNT(DISTINCT id_autoria)` | Relaciones distintas entre publicaciones e identidades de autor. |
| Autores identificados | `COUNT(DISTINCT id_autor)` | Identidades de autor distintas. No equivale necesariamente a personas deduplicadas con certeza. |
| Autores con correspondencia en VOC SEDICI | `COUNT(DISTINCT CASE WHEN tiene_correspondencia_voc IS TRUE THEN id_autor END)` | Identidades de autor vinculadas con una persona registrada en VOC SEDICI. |
| Autores sin correspondencia en VOC SEDICI | `COUNT(DISTINCT CASE WHEN tiene_correspondencia_voc IS FALSE THEN id_autor END)` | Identidades reconocidas que no pudieron vincularse con una persona de VOC SEDICI. |
| Autores con afiliación UNLP registrada | `COUNT(DISTINCT CASE WHEN tiene_afiliacion_unlp IS TRUE THEN id_autor END)` | Identidades con una afiliación al árbol institucional UNLP registrada en VOC SEDICI y aplicable a la fecha de publicación. |
| Publicaciones con autor en VOC SEDICI | `COUNT(DISTINCT CASE WHEN correspondencia_voc_publicacion = 'Con autor en VOC SEDICI' THEN handle_publicacion END)` | Publicaciones con al menos un autor vinculado con VOC SEDICI. |
| Publicaciones sin autor en VOC SEDICI | `COUNT(DISTINCT CASE WHEN correspondencia_voc_publicacion = 'Sin autor en VOC SEDICI' THEN handle_publicacion END)` | Publicaciones sin autores vinculados con VOC SEDICI. |

La correspondencia con VOC SEDICI no demuestra por sí sola una afiliación con
la UNLP. La afiliación es una clasificación posterior basada en las relaciones
institucionales registradas para la persona.

## Período de referencia

Cuando el requerimiento sea analizar los últimos cinco años calendario, el
período comprende el año corriente y los cuatro anteriores. En 2026, el rango
es 2022–2026 y los datos del último año son parciales.

El período debe aplicarse como filtro sobre `fecha_publicacion` o
`anio_publicacion`. No debe incorporarse a las fórmulas de conteo, para que las
métricas continúen siendo reutilizables.

## Modelos agregados existentes

| Modelo | Grano | Estado | Observación |
| :--- | :--- | :--- | :--- |
| `unlp_sedici_revistas_resumen_historico` | Revista | Vigente | Listado histórico completo, preparado para exportar a una planilla. |
| `unlp_sedici_revistas_resumen_ultimos_5_anios` | Revista | Vigente | Listado correspondiente al período utilizado por el reporte. |
| `unlp_sedici_revistas_resumen_por_anio` | Revista y año | Vigente | Permite analizar la evolución y seleccionar otros períodos. |

Los diagnósticos técnicos de cobertura y los agregados experimentales se
retiraron de esta capa. Si vuelven a ser necesarios, deben incorporarse con un
caso de uso y un grano explícitos.

## Reglas de agregación

| Campo | Agregación segura | Motivo |
| :--- | :--- | :--- |
| `handle_publicacion` | `COUNT(DISTINCT ...)` | La publicación se repite por cada autor. |
| `id_autor` | `COUNT(DISTINCT ...)` | Un autor puede aparecer en varias publicaciones. |
| `id_autoria` | `COUNT(DISTINCT ...)` | Identifica una relación publicación–autor. |

Las cantidades de autores no son aditivas entre revistas o años: una misma
identidad puede aparecer en varios grupos. Para obtener un total combinado se
debe volver a contar `DISTINCT id_autor` sobre las filas del período completo.
