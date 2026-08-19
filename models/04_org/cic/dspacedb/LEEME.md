# CIC desde dspacedb

Esta rama delimita CIC dentro de los modelos compartidos de `dspacedb`. La
institución se identifica por el ROR `https://ror.org/02s7sax82` y la fuente
conserva contexto de repositorio, extracción y carga.

```text
modelos compartidos dspacedb
    -> int_cic_dspacedb_item
    -> int_cic_dspacedb_item_metadatavalue
    -> un int por metadato utilizado
    -> fct_cic_dspacedb_item_publication
```

`int_cic_dspacedb_item` elige una sola observación institucional por
`item_uuid`. La unión posterior de metadatos conserva `item_hk`,
`source_label` e `institution_ror` para no mezclar las distintas etiquetas con
las que el mismo repositorio fue cargado en el warehouse.

La fact conserva todos los ítems publicados del alcance institucional. La
ausencia de un metadato, incluido `dc.type`, se expone como nulo y se controla
como calidad de datos; no se utiliza para excluir silenciosamente una
publicación.

A diferencia de `cicdigital`, esta rama se apoya en el recorrido historizado
del Data Warehouse. Debe utilizarse cuando el análisis necesita comparar
extracciones, conservar procedencia temporal o trabajar con varias instancias
DSpace bajo el mismo contrato técnico.

La rama no debe depender de que exista un dump directo en `ldg_cicdigital`.

`fct_cic_dspacedb_dedup_publication` adapta esta representación al contrato de
deduplicación. En ese contrato, `source` identifica al repositorio CIC Digital
y `source_system` identifica el recorrido histórico `dspacedb`.

Para el tipo de deduplicación se prioriza `cic.parentType`. Cuando una
observación histórica no tiene ese metadato, se aplican equivalencias
documentales inequívocas de `dc.type`; los valores ambiguos permanecen como
`unknown`.
