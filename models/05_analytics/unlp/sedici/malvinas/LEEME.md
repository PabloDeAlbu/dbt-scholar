# Producciones candidatas sobre Malvinas en SEDICI

Esta carpeta reúne la base analítica del proyecto “Repositorio Institucional
SEDICI: análisis de las producciones académicas sobre Malvinas en la UNLP”.

## Recuperación inicial

El universo inicial se recuperó el 26 de agosto de 2026 mediante esta
consulta al Solr de SEDICI:

```text
http://solr.sedici.unlp.edu.ar:8080/solr_sedici/search/select
  ?q=malvinas
  &fq=search.resourcetype:2
  &rows=8000
  &fl=handle,search.resourceid
  &wt=csv
```

Los parámetros representan:

- `q=malvinas`: término utilizado en la búsqueda general;
- `fq=search.resourcetype:2`: restricción a ítems de DSpace;
- `rows=8000`: máximo solicitado a Solr;
- `fl`: identificadores incluidos en la respuesta;
- `wt=csv`: formato de exportación.

La extracción produjo 7.008 handles únicos.

## Alcance y limitaciones

Solr puede encontrar el término en los metadatos o en el texto completo. Por
eso, una aparición de `malvinas` no garantiza que la publicación tenga a
Malvinas como tema principal. A su vez, este criterio puede omitir documentos
relevantes que utilicen otros términos, tengan problemas de OCR, no posean
texto completo indexable o todavía no estén reflejados en el índice.

El resultado debe interpretarse como un conjunto de candidatos. El feedback
de las investigadoras permitirá identificar falsos positivos, documentos
faltantes y nuevos términos o filtros. Cada modificación sustantiva del
criterio debería registrarse aquí junto con la fecha y producir una nueva
extracción reproducible.

## Modelos

El seed `seed_unlp_sedici_malvinas_solr_item_candidate` conserva el snapshot
de identificadores devuelto por Solr. El modelo
`unlp_sedici_malvinas_publication_candidate` usa los handles para seleccionar
y enriquecer esas publicaciones desde
`fct_unlp_sedicidb_dedup_publication`.
Su salida está orientada a la revisión de las investigadoras: presenta nombres
de columnas en español, enlaces navegables y omite identificadores internos de
DSpace. Los valores multivaluados se separan con punto y coma.

`unlp_sedici_malvinas_publication_identifier` conserva una fila por
publicación y presenta por separado los metadatafields de identificadores. En
sus nombres de columna reemplaza los puntos por guiones bajos; por ejemplo,
`dc.identifier.uri` se presenta como `dc_identifier_uri`. También conserva
`doi`, `isbn` e `issn` como versiones consolidadas para facilitar el uso.

`unlp_sedici_malvinas_item_candidate_excluded` conserva los candidatos que
no ingresan en esa fact y distingue ítems retirados, ausentes del dump u otras
reglas de exclusión. Este modelo está pensado para revisión manual y permite
que ninguna fila recuperada por Solr se pierda silenciosamente.

Para revisar exclusiones, `search_resource_id` se cruza con `item_id`. Esto
permite reconocer ítems que ya existían al momento del dump pero todavía no
estaban archivados ni tenían el handle informado posteriormente por Solr.
