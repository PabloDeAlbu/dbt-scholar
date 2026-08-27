# Revistas en SEDICI — Guía conceptual para el dashboard

## Propósito

Construir para el Portal de Revistas de la UNLP un panorama comprensible y
verificable de las revistas alojadas en SEDICI, sus publicaciones y los autores
registrados en sus metadatos.

El universo se delimita mediante la organización interna de SEDICI. La rama
`Revistas` representa un agrupamiento institucional y no una clasificación
tipológica exhaustiva de las publicaciones.

El dashboard debe permitir avanzar desde una visión general hasta el detalle
de una revista o un autor. La selección de una revista o de un período debe
filtrar tanto las publicaciones como los autores observados.

## Pregunta principal

> ¿Cómo está compuesto el universo de revistas alojadas en SEDICI, qué se
> publica en ellas y qué autores aparecen en sus metadatos?

## Texto de apertura sugerido

> Este tablero presenta las revistas alojadas bajo la comunidad `Revistas` de
> SEDICI. Permite explorar sus publicaciones, su evolución temporal y los
> autores registrados en los metadatos del repositorio.

> Los resultados describen el contenido disponible en SEDICI. No constituyen
> por sí solos un padrón editorial oficial de revistas de la UNLP ni una
> evaluación de su calidad o impacto.

> Esta primera versión no integra automáticamente las publicaciones que se
> administran exclusivamente desde otros portales internos de la UNLP.

## Fuente y alcance

Fuente de datos para todos los gráficos:

`analytics_unlp_sedici_revistas.unlp_sedici_revistas_dashboard`

La fuente tiene grano ítem–autor:

- una publicación aparece una vez por cada identidad de autor reconocida;
- una publicación sin autor reconocido conserva una fila con `id_autor` nulo;
- los filtros de revista, fecha, subtipo y cierre afectan publicaciones y
  autores desde la misma fuente.

Texto breve sugerido para el pie del dashboard:

> Fuente: metadatos de SEDICI. Universo: publicaciones visibles alojadas bajo
> la comunidad `Revistas`. Fecha de actualización: [completar].

## Conceptos

### Revista alojada en SEDICI

En este análisis, una revista es una comunidad hija directa de la comunidad
raíz `Revistas` de SEDICI. Las comunidades descendientes se interpretan como
secciones de esa revista.

Esta es una definición basada en la estructura del repositorio. Una comunidad
incluida en este universo no necesariamente representa una revista actualmente
activa, editada por una unidad académica de la UNLP o incluida en un padrón
institucional externo.

El título general sugerido es `Portal de Revistas de la UNLP — Panorama basado
en datos de SEDICI`. En los gráficos conviene hablar de `Revistas en SEDICI` o
`Revistas alojadas en SEDICI`.

### Revista cerrada

Una revista se considera cerrada cuando el título de su comunidad contiene el
marcador `[Publicación cerrada]`. El campo `es_cerrada` reproduce esa marca de
SEDICI; no infiere el cierre a partir de la falta de publicaciones recientes.

### Publicación

Ítem que cumple simultáneamente estas condiciones:

- está incorporado al repositorio;
- no está retirado;
- es visible mediante búsqueda;
- tiene handle;
- está alojado bajo una revista del universo.

Las publicaciones deben contarse mediante `COUNT_DISTINCT(handle_publicacion)`. No debe
usarse `Record Count`, porque una publicación puede ocupar varias filas al
tener varios autores.

### Artículo

Publicación cuyo `sedici.subtype` tiene alguno de estos valores:

- `Articulo`;
- `Comunicacion`;
- `Contribucion a revista`;
- `Revision`.

El campo `es_articulo` materializa esta definición. El análisis conserva
también las publicaciones de otros subtipos.

### Autoría

Relación entre una publicación y una identidad de autor personal registrada
mediante `sedici.creator.person`.

Las autorías deben contarse mediante `COUNT_DISTINCT(id_autoria)`. Una misma
identidad puede aportar varias autorías si aparece en varias publicaciones.

### Identidad de autor

Agrupamiento técnico de observaciones de autor:

- cuando existe una authority, la identidad se construye mediante su URI;
- cuando no existe, se utiliza el nombre normalizado.

Una identidad de autor no equivale necesariamente a una persona identificada
con certeza. Dos personas pueden compartir un nombre y una misma persona puede
aparecer bajo distintas variantes.

Los autores deben contarse mediante `COUNT_DISTINCT(id_autor)`.

### Correspondencia con VOC

`tiene_correspondencia_voc` indica que la authority pudo enlazarse con una persona de VOC
SEDICI. No significa por sí solo que la persona pertenezca a la UNLP ni que su
afiliación esté vigente para una publicación determinada.

### Fecha y período

`fecha_publicacion` y `anio_publicacion` derivan de la fecha de publicación
informada en los metadatos. No representan la fecha de depósito en SEDICI.

### Conteos globales del autor

La base expone tres atributos por identidad:

- `publicaciones_autor_en_sedici`: publicaciones visibles de todo SEDICI donde
  figura como autor personal;
- `publicaciones_autor_en_revistas`: publicaciones visibles bajo la comunidad
  `Revistas`;
- `articulos_autor_en_revistas`: subconjunto anterior clasificado como artículo.

Estos valores describen el total global del autor y deben agregarse con `MAX`.
No cambian cuando se selecciona una revista o un período. Para conocer la
cantidad correspondiente a la selección actual debe utilizarse
`COUNT_DISTINCT(handle_publicacion)`.

## Recorrido narrativo

El relato recomendado avanza de lo general a lo particular:

1. Definir qué entendemos por revista en SEDICI.
2. Dimensionar el universo de revistas y publicaciones.
3. Mostrar cómo se distribuyen y evolucionan las publicaciones.
4. Presentar las autorías y las identidades de autor.
5. Comparar la producción global de cada autor con la selección actual.
6. Explicitar la cobertura y las limitaciones de los metadatos.

## Página 1 — Panorama de revistas y publicaciones

### ¿Cuál es el universo disponible?

Visualización: tarjetas.

Indicadores:

- Revistas: `COUNT_DISTINCT(handle_revista)`.
- Publicaciones: `COUNT_DISTINCT(handle_publicacion)`.
- Autorías identificadas: `COUNT_DISTINCT(id_autoria)`.
- Autores identificados: `COUNT_DISTINCT(id_autor)`.

Texto sugerido:

> Los indicadores responden a los filtros activos. Al seleccionar una revista,
> el universo se restringe a sus publicaciones y autores registrados.

### ¿Qué revistas concentran más publicaciones?

Visualización: tabla o barras horizontales con interacción de filtro.

Dimensión:

- `revista`.

Métricas:

- Publicaciones: `COUNT_DISTINCT(handle_publicacion)`.
- Autorías: `COUNT_DISTINCT(id_autoria)`.
- Autores: `COUNT_DISTINCT(id_autor)`.

Campos opcionales:

- `es_cerrada`, presentado como `Cerrada`;
- cantidad de artículos mediante un filtro `es_articulo = TRUE`.

Pregunta auxiliar:

> ¿La concentración de publicaciones también implica una mayor diversidad de
> autores o principalmente una mayor cantidad de autorías?

### ¿Cuántas revistas están abiertas o cerradas?

Visualización: tarjetas o barra apilada.

Dimensión:

- `es_cerrada`, presentado como `Abierta` y `Cerrada`.

Métrica:

- `COUNT_DISTINCT(handle_revista)`.

Texto sugerido:

> El estado de cierre reproduce una marca explícita en el título de la
> comunidad de SEDICI.

### ¿Qué tipos de contenido se publican bajo las revistas?

Visualización: barras ordenadas.

Dimensión:

- `tipo_publicacion`.

Métrica:

- `COUNT_DISTINCT(handle_publicacion)`.

Preguntas auxiliares:

- ¿Qué proporción corresponde a artículos?
- ¿Qué otros subtipos tienen una presencia relevante?
- ¿La composición cambia entre revistas o períodos?

## Página 2 — Evolución y cobertura

### ¿Cómo evolucionaron las publicaciones?

Visualización: serie temporal.

Dimensión:

- `anio_publicacion`.

Métricas:

- Publicaciones: `COUNT_DISTINCT(handle_publicacion)`.
- Artículos: `COUNT_DISTINCT(handle_publicacion)` con `es_articulo = TRUE`.

Texto sugerido:

> La serie utiliza el año de publicación informado en los metadatos, no el año
> de incorporación del ítem al repositorio.

### ¿Qué revistas tuvieron actividad en el período seleccionado?

Visualización: tabla, barras o mapa de calor revista–año.

Dimensiones:

- `revista`;
- `anio_publicacion`.

Métrica:

- `COUNT_DISTINCT(handle_publicacion)`.

### ¿Qué cobertura de autorías tienen las publicaciones?

Visualización: tarjeta, barra porcentual o ranking por revista.

Campo calculado sugerido:

```text
COUNT_DISTINCT(
  CASE WHEN tiene_autor_reconocido THEN handle_publicacion ELSE NULL END
)
/
COUNT_DISTINCT(handle_publicacion)
```

Texto sugerido:

> Una publicación sin autor reconocido puede tener autores en su contenido. El
> indicador mide la cobertura del metadato `sedici.creator.person`.

## Página 3 — Autores

### ¿Qué autores aparecen en la selección actual?

Visualización: tabla ordenada por publicaciones en la selección.

Dimensión:

- `nombre_autor`.

Filtro del gráfico:

- `id_autor` no es nulo;
- opcionalmente `es_articulo = TRUE` si la página se limita a artículos.

Métricas:

- Todo SEDICI: `MAX(publicaciones_autor_en_sedici)`.
- Comunidad Revistas: `MAX(publicaciones_autor_en_revistas)`.
- Artículos de revistas: `MAX(articulos_autor_en_revistas)`.
- Selección actual: `COUNT_DISTINCT(handle_publicacion)`.

Texto sugerido:

> Los tres primeros valores representan totales globales de la identidad. La
> última columna responde a la revista y al período seleccionados.

### ¿Qué autores concentran más publicaciones en la selección?

Visualización: barras horizontales.

Dimensión:

- `nombre_autor`.

Métrica:

- `COUNT_DISTINCT(handle_publicacion)`.

Pregunta auxiliar:

> ¿Los autores con mayor presencia en una revista también tienen una producción
> amplia en el resto de SEDICI?

### ¿Cómo se construyeron las identidades?

Visualización: barras o tarjetas.

Dimensión:

- `id_autorentity_basis`.

Valores:

- `authority`: identidad basada en una URI de autoridad;
- `normalized_name`: identidad nominal basada en texto normalizado.

Métrica:

- `COUNT_DISTINCT(id_autor)`.

### ¿Qué proporción tiene correspondencia con VOC?

Visualización: barra porcentual o tarjetas.

Dimensión:

- `tiene_correspondencia_voc`.

Métrica:

- `COUNT_DISTINCT(id_autor)`.

Texto sugerido:

> La correspondencia con VOC informa cobertura de control de autoridades. No
> debe interpretarse automáticamente como afiliación a la UNLP.

## Controles recomendados

- Período, basado en `fecha_publicacion`.
- Revista, mediante `revista`.
- Estado de cierre, mediante `es_cerrada`.
- Subtipo, mediante `tipo_publicacion`.
- Es artículo, mediante `es_articulo`.
- Base de identidad, mediante `id_autorentity_basis`.
- Correspondencia VOC, mediante `tiene_correspondencia_voc`.

Todos los gráficos principales deben utilizar `unlp_sedici_revistas_dashboard` para que
los filtros y la interacción por clic se propaguen de forma consistente.

## Reglas para no distorsionar las métricas

- No utilizar `Record Count` como cantidad de publicaciones.
- Contar publicaciones con `COUNT_DISTINCT(handle_publicacion)`.
- Contar autorías con `COUNT_DISTINCT(id_autoria)`.
- Contar autores con `COUNT_DISTINCT(id_autor)`.
- Agregar los conteos globales del autor mediante `MAX`, nunca mediante `SUM`.
- Mostrar cantidades junto con porcentajes de cobertura.
- Aplicar explícitamente `es_articulo = TRUE` cuando se hable de artículos.
- Diferenciar los totales globales del autor de la cantidad bajo los filtros
  actuales.

## Preguntas que quedan fuera de esta versión

Este dashboard no debería intentar responder todavía:

- ¿A qué unidad académica pertenece cada revista?
- ¿Qué autores pertenecen laboralmente a la UNLP?
- ¿Cuál era la afiliación de un autor al momento de publicar?
- ¿Qué impacto, calidad o alcance tiene una revista?
- ¿Cuántas citas recibió una publicación?
- ¿Una identidad nominal corresponde inequívocamente a una persona?

Estas preguntas requieren otras fuentes, reglas de atribución o análisis de
calidad específicos.

## Lista de control antes de publicar

- La definición de revista está visible.
- La fuente y la fecha de actualización están informadas.
- Todas las cantidades de publicaciones usan `COUNT_DISTINCT(handle_publicacion)`.
- La tabla de autores excluye filas con `id_autor` nulo.
- Los conteos globales del autor usan `MAX`.
- Los títulos distinguen publicaciones, artículos, autorías y autores.
- El filtro de revista afecta la tabla de autores.
- El estado cerrado se presenta como una marca de SEDICI.
- Las limitaciones sobre identidad y afiliación están explicitadas.
