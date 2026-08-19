# CIC Digital desde la base directa

Esta rama representa el estado vigente de CIC Digital a partir de un dump
completo de su base DSpace 7 restaurado en `ldg_cicdigital`.

```text
ldg_cicdigital
    -> int_cic_cicdigital_*
    -> dimensiones y bridges
    -> fct_cic_cicdigital_item_publication
```

No conserva historia entre dumps. La fecha del snapshot pertenece a la
procedencia de la carga y no debe inferirse desde `last_modified`.

## Modelos intermedios

`int_cic_cicdigital_item_metadatavalue` conserva una observación por
`metadata_value_id`. Los modelos `int_` específicos preparan sólo los
metadatafields requeridos por modelos posteriores.

Los metadatos escalares eligen el último valor no vacío mediante el mayor
`metadata_value_id`. Los autores e identificadores permanecen multivaluados y
se agregan únicamente en el contrato que lo requiere.

Los modelos específicos exponen:

- `value_raw`: contenido original;
- `value`: contenido normalizado;
- `value_precision`: precisión reconocida, solamente para fechas.

## Fact de publicaciones

`fct_cic_cicdigital_item_publication` contiene una fila por ítem archivado y no
retirado. Conserva flags operativos y metadatos escalares. Las autorías se
mantienen fuera de la fact en `brg_cic_cicdigital_item_author`.

Para construir y probar esta rama:

```bash
dbt build --target dev_docker --threads 1 --selector cic_cicdigital
```
