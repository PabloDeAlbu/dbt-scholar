# Seeds de la tarea 14365

Esta carpeta conserva los datos externos y las evidencias necesarias para
reproducir las deduplicaciones de libros y tesis de Koha FDA contra SEDICI.

## Insumos recibidos

- `libros_catedra_fda_koha_sedici_cruce_2026.csv`: catálogo de libros de
  cátedra exportado desde Koha FDA.
- `tesis_fda_koha_sedici_cruce_2026.csv`: catálogo de tesis exportado desde
  Koha FDA.

Estos archivos son las fuentes de los modelos que preparan el input 1 de cada
dedup. No deben reemplazarse sin registrar la procedencia y la fecha del nuevo
export.

## Carpeta `dedups`

Los artefactos conservados en esta carpeta usan el prefijo `seed_` para dejar
explícito su rol como recursos dbt y evitar colisiones con los modelos que los
originaron.

- `seed_issue_14365_03_dedup_thesis_koha_sedici_result.csv`: resultado completo de la
  dedup de tesis Koha FDA → SEDICI. Contiene una decisión por cada registro
  Koha evaluado.
- `seed_issue_14365_02_dedup_book_sedici_koha_result.csv`: resultado de la dedup
  exploratoria de libros SEDICI FDA → Koha.
- `seed_issue_14365_03_export_thesis_koha_url.csv`: snapshot del CSV preparado
  para importar en SEDICI. Agrega las URLs de Koha mediante
  `mods.recordInfo.recordContentSource[es]` únicamente para correspondencias
  unívocas confirmadas. La columna `id` contiene el identificador interno del
  ítem de DSpace, no el handle.
- `seed_issue_14365_03_review_thesis_koha_sedici_manual.csv`: conciliación manual de
  los casos `NEAR_DUPLICATE` y `DONT_KNOW`. Conserva la decisión final y la
  evidencia utilizada; es la fuente para generar actualizaciones
  complementarias y recuperar falsos positivos para la siguiente dedup.
- `seed_issue_14365_03_export_thesis_koha_url_reviewed.csv`: snapshot de
  la salida del modelo homónimo sin el prefijo `seed_`. Contiene el segundo
  lote de actualizaciones de SEDICI, correspondiente a duplicados confirmados
  manualmente.
- `seed_issue_14365_04_thesis_fda_digitization.csv`: listado operativo exportado de
  la hoja “Tesis fda para digitalizar”. Incluye estados de envío y devolución
  y puede contener varias piezas documentales con el mismo inventario.
- `seed_issue_14365_04_dedup_thesis_digitization_result.csv`: resultado de
  la dedup de las tesis de Koha FDA sin SEDICI contra el listado operativo de
  digitalización.
- `seed_issue_14365_04_review_thesis_digitization_manual.csv`: resolución
  manual de las relaciones múltiples de la dedup 04. Registra además que Koha
  `44692` duplica a `44693`, elegido como canónico por figurar disponible en
  sala.

Los CSV usados como inputs del deduplicador no se guardan aquí cuando pueden
regenerarse desde modelos dbt. Esto evita duplicar información, incorporar
archivos grandes al repositorio y crear colisiones entre seeds y modelos con
el mismo nombre.
